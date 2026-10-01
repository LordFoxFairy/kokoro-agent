-- kokoro-agent V1 canonical PostgreSQL schema.
-- This is the only project-owned DDL authority and is installed into a blank
-- deployment schema by kokoro-agent-db-apply-schema. Cross-resource integrity
-- is enforced by application transactions, fencing and reconciliation.

SET lock_timeout = '5s';
SET statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS kokoro_agent_run (
  run_id                  TEXT PRIMARY KEY,
  tenant_id               TEXT NOT NULL,
  request_json            TEXT,
  assembly_recipe_bytes   BYTEA,
  assembly_recipe_fingerprint TEXT,
  interaction_revision    BIGINT NOT NULL DEFAULT 0,
  interaction_phase       TEXT NOT NULL DEFAULT 'active',
  interaction_source_index BIGINT,
  pause_revision          BIGINT NOT NULL DEFAULT 0,
  pause_ref               TEXT,
  pending_groups_json     JSONB NOT NULL DEFAULT '[]'::jsonb,
  pause_snapshot_json     JSONB,
  pause_collection_digest TEXT,
  interaction_command_id  TEXT,
  owner                   TEXT,
  lease_generation        BIGINT NOT NULL DEFAULT 0,
  lease_expires_at        TIMESTAMPTZ(3),
  terminal                BOOLEAN NOT NULL DEFAULT FALSE,
  terminal_at             TIMESTAMPTZ(3),
  durable_counter         BIGINT NOT NULL DEFAULT 0,
  event_index_counter     BIGINT NOT NULL DEFAULT 0,
  terminal_fence_seq      BIGINT,
  token_total             BIGINT NOT NULL DEFAULT 0,
  usage_input_total       BIGINT NOT NULL DEFAULT 0,
  usage_output_total      BIGINT NOT NULL DEFAULT 0,
  sandbox_id              TEXT,
  sandbox_generation      BIGINT,
  sandbox_backend_kind    TEXT,
  sandbox_teardown_ref    TEXT,
  created_at              TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at              TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT ck_run_interaction_revision CHECK (pause_revision >= 0 AND interaction_revision >= pause_revision),
  CONSTRAINT ck_run_interaction_phase CHECK (interaction_phase IN ('active','waiting','resuming','terminal') AND terminal = (interaction_phase = 'terminal')),
  CONSTRAINT ck_run_interaction_collection CHECK (jsonb_typeof(pending_groups_json) = 'array' AND
    CASE WHEN interaction_phase IN ('waiting','resuming') THEN jsonb_array_length(pending_groups_json) > 0
         ELSE jsonb_array_length(pending_groups_json) = 0 END),
  CONSTRAINT ck_run_pause_identity CHECK (
    (pause_revision = 0 AND pause_ref IS NULL AND pause_snapshot_json IS NULL AND pause_collection_digest IS NULL) OR
    (pause_revision > 0 AND pause_ref IS NOT NULL AND length(pause_ref) > 0 AND pause_snapshot_json IS NOT NULL AND
     jsonb_typeof(pause_snapshot_json) = 'object' AND pause_collection_digest IS NOT NULL AND pause_collection_digest ~ '^[0-9a-f]{64}$')),
  CONSTRAINT ck_run_interaction_command CHECK ((interaction_command_id IS NULL OR length(interaction_command_id) > 0) AND
    (interaction_phase <> 'resuming' OR interaction_command_id IS NOT NULL)),
  CONSTRAINT ck_run_interaction_source CHECK ((interaction_source_index IS NULL OR interaction_source_index >= 0) AND
    (interaction_revision <> 0 OR interaction_source_index IS NULL) AND
    (interaction_phase NOT IN ('waiting','resuming') OR interaction_source_index IS NOT NULL)),
  CONSTRAINT ck_kokoro_agent_run_static_recipe CHECK (
    (assembly_recipe_bytes IS NULL AND assembly_recipe_fingerprint IS NULL)
    OR (
      assembly_recipe_bytes IS NOT NULL AND assembly_recipe_fingerprint IS NOT NULL
      AND octet_length(assembly_recipe_bytes) >= 1
      AND octet_length(assembly_recipe_bytes) <= 8388608
      AND assembly_recipe_fingerprint ~ '^[0-9a-f]{64}$'
    )
  ),
  CONSTRAINT ck_kokoro_agent_run_tenant CHECK (length(trim(tenant_id)) > 0),
  CONSTRAINT ck_kokoro_agent_run_lease_generation CHECK (lease_generation >= 0),
  CONSTRAINT ck_kokoro_agent_run_counters CHECK (
    durable_counter >= 0
    AND event_index_counter >= 0
    AND token_total >= 0
    AND usage_input_total >= 0
    AND usage_output_total >= 0
  ),
  CONSTRAINT ck_kokoro_agent_run_sandbox_binding CHECK (
    (
      sandbox_id IS NULL
      AND sandbox_generation IS NULL
      AND sandbox_backend_kind IS NULL
      AND sandbox_teardown_ref IS NULL
    )
    OR
    (
      sandbox_id IS NOT NULL
      AND sandbox_generation IS NOT NULL
      AND sandbox_backend_kind IN ('docker', 'e2b', 'custom')
      AND sandbox_teardown_ref IS NOT NULL
    )
  )
);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_run_reclaim
  ON kokoro_agent_run (terminal, lease_expires_at, run_id);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_run_retention
  ON kokoro_agent_run (terminal_at, run_id) WHERE terminal = TRUE;

CREATE TABLE IF NOT EXISTS kokoro_agent_run_dispatch (
  run_id       TEXT PRIMARY KEY,
  tenant_id    TEXT NOT NULL,
  session_id   TEXT NOT NULL,
  namespace    TEXT NOT NULL,
  request_json TEXT NOT NULL,
  fence        TEXT NOT NULL,
  status       TEXT NOT NULL,
  claimed_by   TEXT,
  created_at   TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT ck_kokoro_agent_run_dispatch_tenant CHECK (length(trim(tenant_id)) > 0),
  CONSTRAINT ck_kokoro_agent_run_dispatch_status CHECK (status IN ('pending', 'claimed'))
);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_run_dispatch_pending
  ON kokoro_agent_run_dispatch (status, created_at, run_id);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_run_dispatch_tenant
  ON kokoro_agent_run_dispatch (tenant_id, run_id);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_dlq (
  raw_hash     TEXT PRIMARY KEY,
  source       TEXT NOT NULL,
  reason       TEXT NOT NULL,
  occurred_at  TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_outbox (
  run_id        TEXT NOT NULL,
  durable_seq   BIGINT NOT NULL,
  event_id      TEXT NOT NULL,
  kind          TEXT NOT NULL,
  status        TEXT NOT NULL,
  index_value   BIGINT,
  occurred_at   TIMESTAMPTZ(3),
  payload_json  TEXT,
  published_at  TIMESTAMPTZ(3),
  created_at    TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_run_outbox PRIMARY KEY (run_id, durable_seq),
  CONSTRAINT uq_kokoro_agent_run_outbox_event UNIQUE (event_id),
  CONSTRAINT ck_kokoro_agent_run_outbox_status CHECK (
    status IN ('queued', 'published', 'superseded')
  )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_kokoro_agent_run_outbox_run_index
  ON kokoro_agent_run_outbox (run_id, index_value)
  WHERE index_value IS NOT NULL AND status <> 'superseded';
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_run_outbox_delivery
  ON kokoro_agent_run_outbox (status, published_at, run_id, durable_seq);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_receipt (
  run_id       TEXT NOT NULL,
  durable_seq  BIGINT NOT NULL,
  event_id     TEXT NOT NULL,
  status       TEXT NOT NULL,
  reason       TEXT,
  created_at   TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_run_receipt PRIMARY KEY (run_id, durable_seq),
  CONSTRAINT ck_kokoro_agent_run_receipt_status CHECK (
    status IN ('persisted', 'projected', 'consumed', 'rejected')
  )
);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_receipt_manifest (
  run_id                    TEXT PRIMARY KEY,
  persisted_seq             BIGINT NOT NULL DEFAULT 0,
  projected_seq             BIGINT NOT NULL DEFAULT 0,
  consumed_seq              BIGINT NOT NULL DEFAULT 0,
  producer_close_requested  BOOLEAN NOT NULL DEFAULT FALSE,
  producer_closed           BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at                TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT ck_kokoro_agent_run_receipt_manifest_sequence CHECK (
    persisted_seq >= 0 AND projected_seq >= 0 AND consumed_seq >= 0
  )
);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_control_command (
  run_id          TEXT NOT NULL,
  command_id      TEXT NOT NULL,
  request_digest  TEXT NOT NULL,
  fingerprint     TEXT,
  status          TEXT NOT NULL,
  body            TEXT NOT NULL,
  error_code      TEXT,
  resume_pause_revision BIGINT,
  resume_pause_ref TEXT,
  resume_decisions_bytes BYTEA,
  resume_decisions_digest TEXT,
  resume_intent_status TEXT,
  resume_pause_snapshot_json JSONB,
  resume_pause_collection_digest TEXT,
  resume_accepted_revision BIGINT,
  resume_accepted_source_index BIGINT,
  resume_attempt_id TEXT,
  resume_attempt_generation BIGINT,
  resume_dispatch_plan_json JSONB,
  resume_accepted_at TIMESTAMPTZ(3),
  resume_started_at TIMESTAMPTZ(3),
  resume_observation_digest TEXT,
  resume_result_kind TEXT,
  resume_result_revision BIGINT,
  resume_result_source_index BIGINT,
  resume_probe_count SMALLINT NOT NULL DEFAULT 0,
  resume_probe_progress_digest TEXT,
  resume_probe_quiescence_json JSONB,
  resume_probe_last_read_id TEXT,
  resume_probe_checked_at TIMESTAMPTZ(3),
  CONSTRAINT ck_control_resume_observation CHECK (
    (resume_observation_digest IS NULL OR resume_observation_digest ~ '^[0-9a-f]{64}$') AND
    (resume_intent_status IS DISTINCT FROM 'native_observed' OR resume_observation_digest IS NOT NULL)),
  CONSTRAINT ck_control_resume_result CHECK (
    num_nonnulls(resume_result_kind,resume_result_revision,resume_result_source_index)=0 OR
    (num_nonnulls(resume_result_kind,resume_result_revision,resume_result_source_index)=3 AND
     resume_result_kind IN ('accepted','native_consumed','validation_failed','unknown','cancelled') AND
     resume_result_revision>0 AND resume_result_source_index>=0 AND resume_intent_status IS NOT NULL)),
  CONSTRAINT ck_control_resume_probe CHECK (
    resume_probe_count BETWEEN 0 AND 3 AND
    ((num_nonnulls(resume_probe_progress_digest,resume_probe_quiescence_json,resume_probe_last_read_id,resume_probe_checked_at)=0 AND resume_probe_count=0) OR
     (num_nonnulls(resume_probe_progress_digest,resume_probe_quiescence_json,resume_probe_last_read_id,resume_probe_checked_at)=4 AND
      resume_probe_progress_digest ~ '^[0-9a-f]{64}$' AND length(resume_probe_last_read_id)>0 AND
      jsonb_typeof(resume_probe_quiescence_json)='object' AND resume_attempt_id IS NOT NULL))),
  CONSTRAINT ck_control_resume_intent CHECK (
    (resume_intent_status IS NULL AND num_nonnulls(resume_pause_revision,resume_pause_ref,resume_decisions_bytes,
      resume_decisions_digest,resume_pause_snapshot_json,resume_pause_collection_digest,resume_accepted_revision,
      resume_accepted_source_index,resume_attempt_id,resume_attempt_generation,resume_dispatch_plan_json,resume_accepted_at,resume_started_at) = 0) OR
    (resume_intent_status IS NOT NULL AND resume_intent_status IN ('accepted','dispatch_started','native_observed','unknown','reconciled','terminal') AND
      num_nonnulls(resume_pause_revision,resume_pause_ref,resume_decisions_bytes,resume_decisions_digest,resume_pause_snapshot_json,
        resume_pause_collection_digest,resume_accepted_revision,resume_accepted_source_index,resume_accepted_at) = 9 AND
      resume_pause_revision > 0 AND length(resume_pause_ref) > 0 AND octet_length(resume_decisions_bytes) BETWEEN 1 AND 8388608 AND
      resume_decisions_digest ~ '^[0-9a-f]{64}$' AND resume_pause_collection_digest ~ '^[0-9a-f]{64}$' AND
      jsonb_typeof(resume_pause_snapshot_json) = 'object' AND resume_accepted_revision > resume_pause_revision AND resume_accepted_source_index >= 0)),
  CONSTRAINT ck_control_resume_attempt CHECK (
    (num_nonnulls(resume_attempt_id,resume_attempt_generation,resume_dispatch_plan_json,resume_started_at) = 0 AND
      (resume_intent_status IS NULL OR resume_intent_status IN ('accepted','terminal'))) OR
    (num_nonnulls(resume_attempt_id,resume_attempt_generation,resume_dispatch_plan_json,resume_started_at) = 4 AND
      resume_intent_status IS NOT NULL AND resume_intent_status IN ('dispatch_started','native_observed','unknown','reconciled','terminal') AND
      length(resume_attempt_id) > 0 AND resume_attempt_generation > 0 AND jsonb_typeof(resume_dispatch_plan_json) = 'object' AND resume_started_at >= resume_accepted_at)),
  created_at      TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at      TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_run_control_command PRIMARY KEY (run_id, command_id),
  CONSTRAINT ck_kokoro_agent_run_control_command_status CHECK (
    status IN ('admitted', 'persisted', 'applied', 'succeeded', 'failed', 'superseded')
  )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_control_resume_pause
  ON kokoro_agent_run_control_command (run_id, resume_pause_revision) WHERE resume_intent_status IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_control_resume_attempt
  ON kokoro_agent_run_control_command (run_id, resume_attempt_id) WHERE resume_attempt_id IS NOT NULL;


CREATE TABLE IF NOT EXISTS kokoro_agent_run_steer (
  run_id      TEXT NOT NULL,
  message_id  TEXT NOT NULL,
  content     TEXT NOT NULL,
  created_at  TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_run_steer PRIMARY KEY (run_id, message_id)
);

CREATE TABLE IF NOT EXISTS kokoro_agent_run_usage_segment (
  run_id            TEXT NOT NULL,
  lease_generation  BIGINT NOT NULL,
  input_tokens      BIGINT NOT NULL,
  output_tokens     BIGINT NOT NULL,
  created_at        TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_run_usage_segment PRIMARY KEY (run_id, lease_generation),
  CONSTRAINT ck_kokoro_agent_run_usage_segment_tokens CHECK (
    input_tokens >= 0 AND output_tokens >= 0
  )
);

CREATE TABLE IF NOT EXISTS kokoro_agent_sandbox_cleanup_intent (
  cleanup_id       TEXT PRIMARY KEY,
  run_id           TEXT NOT NULL,
  lease_generation BIGINT NOT NULL,
  backend_kind     TEXT NOT NULL,
  sandbox_id       TEXT NOT NULL,
  teardown_ref     TEXT NOT NULL,
  status           TEXT NOT NULL,
  cleanup_owner    TEXT,
  attempt_count    BIGINT NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ(3) NOT NULL,
  last_error       TEXT,
  created_at       TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at       TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT ck_kokoro_agent_sandbox_cleanup_generation CHECK (lease_generation >= 1),
  CONSTRAINT ck_kokoro_agent_sandbox_cleanup_backend CHECK (
    backend_kind IN ('docker', 'e2b', 'custom')
  ),
  CONSTRAINT ck_kokoro_agent_sandbox_cleanup_status CHECK (
    status IN ('pending', 'processing', 'completed')
  ),
  CONSTRAINT ck_kokoro_agent_sandbox_cleanup_attempt CHECK (attempt_count >= 0),
  CONSTRAINT uq_kokoro_agent_sandbox_cleanup_resource UNIQUE (
    run_id, lease_generation, backend_kind, sandbox_id
  )
);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_sandbox_cleanup_due
  ON kokoro_agent_sandbox_cleanup_intent (status, next_attempt_at, created_at, cleanup_id);

CREATE TABLE IF NOT EXISTS kokoro_agent_tool_result (
  run_id      TEXT NOT NULL,
  tool_id     TEXT NOT NULL,
  result      TEXT NOT NULL,
  is_error    BOOLEAN NOT NULL,
  created_at  TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_tool_result PRIMARY KEY (run_id, tool_id)
);

CREATE TABLE IF NOT EXISTS kokoro_agent_tool_journal (
  run_id        TEXT NOT NULL,
  tool_call_id  TEXT NOT NULL,
  name          TEXT NOT NULL,
  status        TEXT NOT NULL,
  result        TEXT NOT NULL,
  is_error      BOOLEAN NOT NULL,
  created_at    TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT pk_kokoro_agent_tool_journal PRIMARY KEY (run_id, tool_call_id),
  CONSTRAINT ck_kokoro_agent_tool_journal_status CHECK (
    status IN ('started', 'succeeded', 'failed')
  )
);

CREATE TABLE IF NOT EXISTS kokoro_agent_chat_session (
  tenant_id    TEXT NOT NULL,
  namespace    TEXT NOT NULL,
  session_id   TEXT NOT NULL,
  project_ref  TEXT,
  title        TEXT NOT NULL,
  created_at   TIMESTAMPTZ(3) NOT NULL,
  updated_at   TIMESTAMPTZ(3) NOT NULL,
  CONSTRAINT pk_kokoro_agent_chat_session PRIMARY KEY (tenant_id, namespace, session_id),
  CONSTRAINT ck_kokoro_agent_chat_session_tenant_id CHECK (btrim(tenant_id) <> '')
);
CREATE INDEX IF NOT EXISTS ix_kokoro_agent_chat_session_page
  ON kokoro_agent_chat_session (tenant_id, namespace, updated_at DESC, session_id ASC);

CREATE TABLE IF NOT EXISTS kokoro_agent_chat_event (
  chat_event_id   TEXT NOT NULL,
  tenant_id       TEXT NOT NULL,
  namespace       TEXT NOT NULL,
  session_id      TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  source_index    BIGINT NOT NULL,
  chat_message_id TEXT,
  event_type      TEXT NOT NULL,
  payload_json    TEXT NOT NULL,
  created_at      TIMESTAMPTZ(3) NOT NULL,
  seq             BIGINT NOT NULL,
  CONSTRAINT pk_kokoro_agent_chat_event PRIMARY KEY (tenant_id, namespace, run_id, source_index),
  CONSTRAINT uq_kokoro_agent_chat_event_id UNIQUE (tenant_id, chat_event_id),
  CONSTRAINT uq_kokoro_agent_chat_event_session_seq UNIQUE (tenant_id, namespace, session_id, seq),
  CONSTRAINT ck_kokoro_agent_chat_event_tenant_id CHECK (btrim(tenant_id) <> '')
);

CREATE TABLE IF NOT EXISTS kokoro_agent_chat_message (
  tenant_id        TEXT NOT NULL,
  chat_message_id  TEXT NOT NULL,
  namespace        TEXT NOT NULL,
  session_id       TEXT NOT NULL,
  run_id           TEXT NOT NULL,
  role             TEXT NOT NULL,
  content          TEXT NOT NULL,
  status           TEXT NOT NULL,
  created_at       TIMESTAMPTZ(3) NOT NULL,
  updated_at       TIMESTAMPTZ(3) NOT NULL,
  seq              BIGINT NOT NULL,
  CONSTRAINT pk_kokoro_agent_chat_message PRIMARY KEY (tenant_id, chat_message_id),
  CONSTRAINT uq_kokoro_agent_chat_message_session_seq UNIQUE (tenant_id, namespace, session_id, seq),
  CONSTRAINT ck_kokoro_agent_chat_message_tenant_id CHECK (btrim(tenant_id) <> ''),
  CONSTRAINT ck_kokoro_agent_chat_message_role CHECK (role IN ('user', 'assistant', 'system', 'tool')),
  CONSTRAINT ck_kokoro_agent_chat_message_status CHECK (
    status IN ('pending', 'streaming', 'completed', 'failed', 'cancelled')
  )
);

CREATE TABLE IF NOT EXISTS kokoro_agent_chat_sequence (
  kind        TEXT NOT NULL,
  tenant_id   TEXT NOT NULL,
  namespace   TEXT NOT NULL,
  session_id  TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  CONSTRAINT pk_kokoro_agent_chat_sequence PRIMARY KEY (kind, tenant_id, namespace, session_id),
  CONSTRAINT ck_kokoro_agent_chat_sequence_tenant_id CHECK (btrim(tenant_id) <> ''),
  CONSTRAINT ck_kokoro_agent_chat_sequence_kind CHECK (kind IN ('event', 'message')),
  CONSTRAINT ck_kokoro_agent_chat_sequence_value CHECK (seq >= 0)
);

CREATE TABLE IF NOT EXISTS kokoro_agent_memory (
  namespace   TEXT[] NOT NULL,
  key         TEXT NOT NULL,
  value_json  TEXT NOT NULL,
  created_at  TIMESTAMPTZ(3) NOT NULL,
  updated_at  TIMESTAMPTZ(3) NOT NULL,
  CONSTRAINT pk_kokoro_agent_memory PRIMARY KEY (namespace, key)
);

-- LangGraph's pinned PostgreSQL saver uses these final-state tables. Its
-- migration ledger is intentionally absent in Kokoro V1 clean-slate installs.
CREATE TABLE IF NOT EXISTS checkpoints (
  thread_id             TEXT NOT NULL,
  checkpoint_ns         TEXT NOT NULL DEFAULT '',
  checkpoint_id         TEXT NOT NULL,
  parent_checkpoint_id  TEXT,
  type                   TEXT,
  checkpoint             JSONB NOT NULL,
  metadata               JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT pk_checkpoints PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);
CREATE INDEX IF NOT EXISTS ix_checkpoints_thread
  ON checkpoints (thread_id);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
  thread_id      TEXT NOT NULL,
  checkpoint_ns  TEXT NOT NULL DEFAULT '',
  channel        TEXT NOT NULL,
  version        TEXT NOT NULL,
  type           TEXT NOT NULL,
  blob           BYTEA,
  CONSTRAINT pk_checkpoint_blobs PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);
CREATE INDEX IF NOT EXISTS ix_checkpoint_blobs_thread
  ON checkpoint_blobs (thread_id);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
  thread_id      TEXT NOT NULL,
  checkpoint_ns  TEXT NOT NULL DEFAULT '',
  checkpoint_id  TEXT NOT NULL,
  task_id        TEXT NOT NULL,
  task_path      TEXT NOT NULL DEFAULT '',
  idx            INTEGER NOT NULL,
  channel        TEXT NOT NULL,
  type           TEXT,
  blob           BYTEA NOT NULL,
  CONSTRAINT pk_checkpoint_writes PRIMARY KEY (
    thread_id, checkpoint_ns, checkpoint_id, task_id, idx
  )
);
CREATE INDEX IF NOT EXISTS ix_checkpoint_writes_thread
  ON checkpoint_writes (thread_id);

CREATE INDEX IF NOT EXISTS idx_control_resume_unsettled
  ON kokoro_agent_run_control_command(run_id,command_id)
  WHERE resume_intent_status IN ('accepted','dispatch_started','native_observed','unknown');

CREATE TABLE IF NOT EXISTS kokoro_agent_run_checkpoint_observation (
  run_id TEXT NOT NULL,
  observation_digest TEXT NOT NULL,
  generation BIGINT NOT NULL,
  command_id TEXT,
  attempt_id TEXT,
  kind TEXT NOT NULL,
  disposition TEXT NOT NULL,
  evidence_bytes BYTEA NOT NULL,
  probe_read_id TEXT,
  created_at TIMESTAMPTZ(3) NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(run_id,observation_digest),
  CONSTRAINT ck_checkpoint_observation_identity CHECK
    (length(run_id)>0 AND generation>0 AND observation_digest ~ '^[0-9a-f]{64}$'),
  CONSTRAINT ck_checkpoint_observation_target CHECK
    (((command_id IS NULL AND attempt_id IS NULL) OR
      (command_id IS NOT NULL AND attempt_id IS NOT NULL AND length(command_id)>0 AND length(attempt_id)>0)) AND
     (kind <> 'pause' OR command_id IS NULL) AND
     (kind NOT IN ('resume','probe') OR command_id IS NOT NULL)),
  CONSTRAINT ck_checkpoint_observation_kind CHECK
    (kind IN ('pause','read','resume','probe') AND disposition IN ('current','audit')),
  CONSTRAINT ck_checkpoint_observation_bytes CHECK (octet_length(evidence_bytes) BETWEEN 1 AND 8388608),
  CONSTRAINT ck_checkpoint_observation_probe CHECK
    ((kind='probe' AND probe_read_id IS NOT NULL AND length(probe_read_id)>0) OR
     (kind<>'probe' AND probe_read_id IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_observation_probe
  ON kokoro_agent_run_checkpoint_observation(run_id,command_id,attempt_id,probe_read_id) WHERE kind='probe';
CREATE INDEX IF NOT EXISTS idx_checkpoint_observation_target
  ON kokoro_agent_run_checkpoint_observation(run_id,command_id,attempt_id,generation,observation_digest);
