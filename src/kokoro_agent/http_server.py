from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kokoro_agent.events import SessionEvent
from kokoro_agent.run_agent import RunAgentInput, run_agent
from kokoro_agent.sse import format_sse

LOGGER = logging.getLogger(__name__)
RunAgentFn = Callable[[RunAgentInput], Iterator[SessionEvent]]


def _build_input(
    session_id: str,
    body: dict[str, object],
) -> RunAgentInput | None:
    conversation_id = body.get("conversation_id")
    user_input = body.get("input")

    if not isinstance(conversation_id, str) or not conversation_id:
        return None

    if not isinstance(user_input, str) or not user_input:
        return None

    return RunAgentInput(
        session_id=session_id,
        conversation_id=conversation_id,
        user_input=user_input,
    )


def _session_stream_path(path: str) -> str | None:
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) != 4:
        return None
    if segments[0] != "sessions" or segments[2] != "runs" or segments[3] != "stream":
        return None
    return segments[1] or None


def build_handler(run_agent_fn: RunAgentFn = run_agent) -> type[BaseHTTPRequestHandler]:
    class AgentHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/health":
                self.send_error(HTTPStatus.NOT_FOUND, "not found")
                return

            self.send_response(HTTPStatus.OK)
            self.send_header("content-type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        def do_POST(self) -> None:
            session_id = _session_stream_path(self.path)
            if session_id is None:
                self.send_error(HTTPStatus.NOT_FOUND, "not found")
                return

            try:
                body = self._read_json_body()
            except ValueError as error:
                LOGGER.warning("invalid agent request body: %s", error)
                self.send_error(HTTPStatus.BAD_REQUEST, str(error))
                return

            run_input = _build_input(session_id, body)
            if run_input is None:
                self.send_error(
                    HTTPStatus.BAD_REQUEST,
                    "conversation_id and input must be non-empty strings",
                )
                return

            self.send_response(HTTPStatus.OK)
            self.send_header("content-type", "text/event-stream; charset=utf-8")
            self.send_header("cache-control", "no-cache")
            self.send_header("connection", "keep-alive")
            self.end_headers()

            for event in run_agent_fn(run_input):
                self.wfile.write(format_sse(event))
                self.wfile.flush()

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("agent-http %s", format % args)

        def _read_json_body(self) -> dict[str, object]:
            content_length = int(self.headers.get("content-length", "0"))
            raw_body = self.rfile.read(content_length).decode("utf-8")
            if not raw_body:
                raise ValueError("request body is required")

            payload = json.loads(raw_body)
            if not isinstance(payload, dict):
                raise ValueError("request body must be a JSON object")
            return payload

    return AgentHandler


def build_server(
    host: str = "127.0.0.1",
    port: int = 8001,
    run_agent_fn: RunAgentFn = run_agent,
) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), build_handler(run_agent_fn))
