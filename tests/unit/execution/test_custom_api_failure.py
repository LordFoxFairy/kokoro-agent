from kokoro_agent.clients.custom_api_runtime import (
    CustomApiConfigurationError,
    CustomApiDependencyError,
)
from kokoro_agent.execution.failures import failure_code, run_failed_payload


def test_custom_api_configuration_error_survives_assembly_default() -> None:
    error = CustomApiConfigurationError("CUSTOM_API_CONFIGURATION_INVALID")
    assert failure_code(error) == "assembly_failed"
    assert run_failed_payload(error, code="assembly_failed").model_dump() == {
        "code": "assembly_failed",
        "retryable": False,
    }


def test_custom_api_dependency_error_is_nonretryable_after_owner_failure() -> None:
    error = CustomApiDependencyError("CUSTOM_API_OWNER_UNAVAILABLE")
    assert failure_code(error) == "dependency_unavailable"
    assert run_failed_payload(error, code="assembly_failed").model_dump() == {
        "code": "dependency_unavailable",
        "retryable": False,
    }
