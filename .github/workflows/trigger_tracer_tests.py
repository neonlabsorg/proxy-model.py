import argparse
import time
from datetime import datetime

from requests import Session

DATE_FORMAT = "%Y-%m-%d"


def get_tracer_workflows(session: Session):
    """
    Get all avialable workflows for Tracer CI.
    """
    workflows = session.get(f"https://api.github.com/repos/neonlabsorg/tracer-api/actions/workflows").json()
    return workflows


def find_workflow_id_by_key(workflows, key: str, value):
    """
    Get workflow ID by key and value.
    """
    for workflow in workflows["workflows"]:
        if workflow[key] == value:
            return workflow["id"]
    return None


def run_workflow(
    session: Session,
    workflow_id: int,
    ref: str = "main",
    neon_tests_image: str = "latest",
    proxy_image: str = "latest",
    evm_loader_image: str = "latest",
    geyser_neon_plugin_image: str = "main",
):
    """
    Start a new dispatch workflow by it's ID.
    """
    payload = {
        "ref": ref,
        "inputs": {
            "neon_tests_image": neon_tests_image,
            "proxy_image": proxy_image,
            "evm_loader_image": evm_loader_image,
            "geyser_neon_plugin_image": geyser_neon_plugin_image,
        },
    }

    # Running workflow
    resp = session.post(
        f"https://api.github.com/repos/neonlabsorg/tracer-api/actions/workflows/{workflow_id}/dispatches", json=payload
    )
    assert (
        resp.status_code == 204
    ), f"Failed to start workflow. Status code: {resp.status_code} with message: {resp.text}"
    return resp


def get_runs_count(session: Session, created_time: str = datetime.now().strftime(DATE_FORMAT)):
    """
    Get all runs for Tracer CI from particular date.
    """
    payload = {"created": created_time}
    resp = session.get("https://api.github.com/repos/neonlabsorg/tracer-api/actions/runs", params=payload).json()
    return resp["total_count"]


def get_runs_by_workflow_id(
    session: Session, workflow_id: int, created_time: str = datetime.now().strftime(DATE_FORMAT)
):
    """
    Get all runs for Tracer CI and filter out them by workflow ID.
    """
    result = set()
    payload = {"created": created_time}
    resp = session.get("https://api.github.com/repos/neonlabsorg/tracer-api/actions/runs", params=payload).json()
    if resp["total_count"] > 0:
        for run in resp["workflow_runs"]:
            if run["workflow_id"] == workflow_id:
                result.add(run["id"])
    print(f"Workflow id {workflow_id} has runs: {result}")
    return result


def wait_for_run_completion(session: Session, run_id: int, timeout: int = 1000):
    """
    Wait for run execution completion within a timeout.
    """
    initial_time = time.time()
    run_url = f"https://api.github.com/repos/neonlabsorg/tracer-api/actions/runs/{run_id}"
    print(f"Waiting {timeout} sec. for {run_url}.")

    r = session.get(run_url)
    assert r.status_code == 200

    status = r.json()["status"]
    statuses = ["completed", "action_required", "cancelled", "failure", "skipped", "stale", "timed_out"]
    while time.time() - initial_time < timeout and status not in statuses:
        r = session.get(run_url)
        assert r.status_code == 200
        status = r.json()["status"]
        print(f"Time elapsed: {time.time() - initial_time}. Status: {status}")
        time.sleep(10)
    assert status in ["completed", "success"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for running workflow by trigger.")
    parser.add_argument("--ref", action="store", default="main", dest="ref")
    parser.add_argument("--neon_tests_image", action="store", default="latest", dest="neon_tests_image")
    parser.add_argument("--proxy_image", action="store", default="latest", dest="proxy_image")
    parser.add_argument("--evm_loader_image", action="store", default="latest", dest="evm_loader_image")
    parser.add_argument("--geyser_neon_plugin_image", action="store", default="main", dest="geyser_neon_plugin_image")
    parser.add_argument("--token", action="store", dest="token")

    results = parser.parse_args()

    s = Session()
    s.headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": f"Bearer {results.token}",
    }
    created_time = datetime.now().strftime(DATE_FORMAT)
    workflows = get_tracer_workflows(s)
    workflow_id = find_workflow_id_by_key(workflows, "name", "tracer api main pipeline")

    runs_before_start = get_runs_by_workflow_id(s, workflow_id, created_time)
    runs_count = get_runs_count(s, created_time)

    start_workflow = run_workflow(
        s,
        workflow_id,
        ref=results.ref,
        neon_tests_image=results.neon_tests_image,
        proxy_image=results.proxy_image,
        evm_loader_image=results.evm_loader_image,
        geyser_neon_plugin_image=results.geyser_neon_plugin_image,
    )
    assert start_workflow.status_code == 204

    initial_time = time.time()
    while runs_count == get_runs_count(s, created_time) and time.time() - initial_time < 100:
        time.sleep(10)

    runs_after_start = get_runs_by_workflow_id(s, workflow_id, created_time)
    active_runs = runs_after_start - runs_before_start
    # Wait for results
    assert len(active_runs) == 1, f"Expected 1 active run but got: {active_runs}."
    active_run = active_runs.pop()
    wait_for_run_completion(s, active_run)
