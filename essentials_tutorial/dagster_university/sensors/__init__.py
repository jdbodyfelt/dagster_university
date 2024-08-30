from dagster import (
    RunRequest,
    SensorResult,
    sensor,
    SensorEvaluationContext
)


import json
from pathlib import Path

from ..jobs import adhoc_request_job

#/*****************************************************/
@sensor(
    job=adhoc_request_job
)
def adhoc_request_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    This sensor will trigger the `adhoc_request_job` when a new file is added to the
    `requests` directory.
    """
    # Directory to observe
    PATH_TO_REQUESTS = Path(__file__).parents[2] / "data" / "requests"
    # Statefuls
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []
    file_paths = [*PATH_TO_REQUESTS.glob("*.json")]
    last_mods = [obj.stat().st_mtime for obj in file_paths]
    for path, mod in zip(file_paths, last_mods):
        current_state[path.name] = mod
        if path.name not in previous_state or previous_state[path.name] != mod:
            with open(path, "r") as fid:
                request_config = json.load(fid)
                runs_to_request.append(RunRequest(
                    run_key=f"adhoc_request_{path.name}_{mod}",
                    run_config={
                        "ops": {
                            "adhoc_request": {
                                "config": {
                                    "filename": path.name,
                                    **request_config
                                }
                            }
                        }
                    }
                ))
    return SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state)
    )