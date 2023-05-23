import click
import requests


class GithubClient():
    TESTS_ENDPOINT = "https://api.github.com/repos/neonlabsorg/neon-tests"

    def __init__(self, token):
        self.headers = {"Authorization": f"Bearer {token}",
                        "Accept": "application/vnd.github+json"}

    def get_dapps_runs_list(self, branch="develop"):
        response = requests.get(
            f"{self.TESTS_ENDPOINT}/actions/workflows/dapps.yml/runs?branch={branch}", headers=self.headers)
        if response.status_code != 200:
            raise RuntimeError(f"Can't get dapps tests runs list. Error: {response.json()}")
        runs = [item['id'] for item in response.json()['workflow_runs']]
        return runs

    def get_dapps_runs_count(self, branch="develop"):
        response = requests.get(
            f"{self.TESTS_ENDPOINT}/actions/workflows/dapps.yml/runs?branch={branch}", headers=self.headers)
        return int(response.json()["total_count"])

    def run_dapps_dispatches(self, proxy_url, solana_url, faucet_url, network_id='111', branch='develop'):

        data = {"ref": branch,
                "inputs": {"proxy_url": proxy_url,
                           "solana_url": solana_url,
                           "faucet_url": faucet_url,
                           "network_id": network_id}
                }
        response = requests.post(
            f"{self.TESTS_ENDPOINT}/actions/workflows/dapps.yml/dispatches", json=data, headers=self.headers)
        click.echo(f"Sent data: {data}")
        click.echo(f"Status code: {response.status_code}")
        if response.status_code != 204:
            raise RuntimeError("proxy-model.py action is not triggered")

    def get_dapps_run_info(self, id):
        response = requests.get(
            f"{self.TESTS_ENDPOINT}/actions/runs/{id}", headers=self.headers)
        return response.json()
