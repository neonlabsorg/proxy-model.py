import multiprocessing
import os
import re
import time
import sys

import docker
import subprocess
import pathlib
import requests
import json
import typing as tp
import logging
from urllib.parse import urlparse
from python_terraform import Terraform
from paramiko import SSHClient
from scp import SCPClient

try:
    import click
except ImportError:
    print("Please install click library: pip install click==8.0.3")
    sys.exit(1)


@click.group()
def cli():
    pass


ERR_MSG_TPL = {
    "blocks": [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": ""},
        },
        {"type": "divider"},
    ]
}

DOCKER_USERNAME = os.environ.get("DOCKER_USERNAME")
DOCKER_PASSWORD = os.environ.get("DOCKER_PASSWORD")
DOCKERHUB_ORG_NAME = os.environ.get("DOCKERHUB_ORG_NAME")

NEON_TEST_RUN_LINK = os.environ.get("NEON_TEST_RUN_LINK")

TFSTATE_BUCKET = os.environ.get("TFSTATE_BUCKET")
TFSTATE_KEY_PREFIX = os.environ.get("TFSTATE_KEY_PREFIX")
TFSTATE_REGION = os.environ.get("TFSTATE_REGION")
IMAGE_NAME = os.environ.get("IMAGE_NAME")

FAUCET_COMMIT = os.environ.get("FAUCET_COMMIT")

NEON_TESTS_IMAGE = os.environ.get("NEON_TESTS_IMAGE")

GH_ORG_NAME  = os.environ.get("GH_ORG_NAME")

CONTAINERS = ['proxy', 'solana', 'neon_test_invoke_program_loader',
              'dbcreation', 'faucet', 'gas_tank', 'indexer']

docker_client = docker.APIClient()
terraform = Terraform(working_dir=pathlib.Path(
    __file__).parent / "full_test_suite")
VERSION_BRANCH_TEMPLATE = r"[vt]{1}\d{1,2}\.\d{1,2}\.x.*"


def docker_compose(args: str):
    command = f'docker-compose --compatibility {args}'
    click.echo(f"run command: {command}")
    out = subprocess.run(command, shell=True)
    click.echo("return code: " + str(out.returncode))
    if out.returncode != 0:
        raise RuntimeError(f"Command {command} failed. Err: {out.stderr}")

    return out


def check_neon_evm_tag(tag):
    response = requests.get(
        url=f"https://registry.hub.docker.com/v2/repositories/{DOCKERHUB_ORG_NAME}/evm_loader/tags/{tag}")
    if response.status_code != 200:
        raise RuntimeError(
            f"evm_loader image with {tag} tag isn't found. Response: {response.json()}")


def is_branch_exist(branch, repo):
    if branch:
        proxy_branches_obj = requests.get(
            f"https://api.github.com/repos/{GH_ORG_NAME}/{repo}/branches?per_page=100").json()
        proxy_branches = [item["name"] for item in proxy_branches_obj]

        if branch in proxy_branches:
            click.echo(f"The same branch {branch} is found in {repo} repository")
            return True
    else:
        return False


def update_neon_evm_tag_if_same_branch_exists(branch, neon_evm_tag):
    if is_branch_exist(branch, "neon-evm"):
        neon_evm_tag = branch.split('/')[-1]
        check_neon_evm_tag(neon_evm_tag)
    return neon_evm_tag

def update_faucet_tag_if_same_branch_exists(branch, faucet_tag):
    if is_branch_exist(branch, "neon-faucet"):
        faucet_tag = branch.split('/')[-1]
    print(f"faucet image tag: {faucet_tag}")
    return faucet_tag


@cli.command(name="build_docker_image")
@click.option('--neon_evm_tag', help="the neon evm_loader image tag that will be used for the build")
@click.option('--proxy_tag', help="a tag to be generated for the proxy image")
@click.option('--head_ref_branch')
@click.option('--skip_pull', is_flag=True, default=False, help="skip pulling of docker images from the docker-hub")
def build_docker_image(neon_evm_tag,  proxy_tag, head_ref_branch, skip_pull):
    neon_evm_tag = update_neon_evm_tag_if_same_branch_exists(head_ref_branch, neon_evm_tag)
    neon_evm_image = f'{DOCKERHUB_ORG_NAME}/evm_loader:{neon_evm_tag}'
    click.echo(f"neon-evm image: {neon_evm_image}")
    if not skip_pull:
        click.echo('pull docker images...')
        out = docker_client.pull(neon_evm_image, stream=True, decode=True)
        process_output(out)

    else:
        click.echo('skip pulling of docker images')

    buildargs = {"NEON_EVM_COMMIT": neon_evm_tag,
                 "DOCKERHUB_ORG_NAME": DOCKERHUB_ORG_NAME,
                 "PROXY_REVISION": proxy_tag}

    click.echo("Start build")

    output = docker_client.build(
        tag=f"{IMAGE_NAME}:{proxy_tag}", buildargs=buildargs, path="./", decode=True, network_mode='host')
    process_output(output)


@cli.command(name="publish_image")
@click.option('--proxy_tag')
@click.option('--head_ref')
@click.option('--github_ref_name')
def publish_image(proxy_tag, head_ref, github_ref_name):
    push_image_with_tag(proxy_tag, proxy_tag)
    branch_name_tag = None
    if head_ref:
        branch_name_tag = head_ref.split('/')[-1]
    elif re.match(VERSION_BRANCH_TEMPLATE,  github_ref_name):
        branch_name_tag = github_ref_name
    if branch_name_tag:
        push_image_with_tag(proxy_tag, branch_name_tag)


def push_image_with_tag(sha, tag):
    click.echo(f"The tag for publishing: {tag}")
    docker_client.login(username=DOCKER_USERNAME, password=DOCKER_PASSWORD)
    docker_client.tag(f"{IMAGE_NAME}:{sha}", f"{IMAGE_NAME}:{tag}")
    out = docker_client.push(f"{IMAGE_NAME}:{tag}", decode=True, stream=True)
    process_output(out)

@cli.command(name="finalize_image")
@click.option('--github_ref')
@click.option('--proxy_tag')
def finalize_image(github_ref, proxy_tag):
    final_tag = ""
    if 'refs/tags/' in github_ref:
        final_tag = github_ref.replace("refs/tags/", "")
    elif github_ref == 'refs/heads/develop':
        final_tag = 'latest'

    if final_tag:
        out = docker_client.pull(f"{IMAGE_NAME}:{proxy_tag}", decode=True, stream=True)
        process_output(out)
        push_image_with_tag(proxy_tag, final_tag)
    else:
        click.echo(f"Nothing to finalize, github_ref {github_ref} is not a tag or develop ref")


@cli.command(name="terraform_infrastructure")
@click.option('--dockerhub_org_name')
@click.option('--head_ref_branch')
@click.option('--github_ref_name')
@click.option('--proxy_tag')
@click.option('--neon_evm_tag')
@click.option('--faucet_tag')
@click.option('--run_number')
def terraform_build_infrastructure(dockerhub_org_name, head_ref_branch, github_ref_name, proxy_tag, neon_evm_tag, faucet_tag, run_number):
    branch = head_ref_branch if head_ref_branch != "" else github_ref_name
    neon_evm_tag = update_neon_evm_tag_if_same_branch_exists(head_ref_branch, neon_evm_tag)
    if branch not in ['master', 'develop']:
        faucet_tag = update_faucet_tag_if_same_branch_exists(branch, faucet_tag)
    os.environ["TF_VAR_branch"] = branch.replace('_', '-')
    os.environ["TF_VAR_proxy_image_tag"] = proxy_tag
    os.environ["TF_VAR_neon_evm_commit"] = neon_evm_tag
    os.environ["TF_VAR_faucet_model_commit"] = faucet_tag
    os.environ["TF_VAR_dockerhub_org_name"] = dockerhub_org_name
    thstate_key = f'{TFSTATE_KEY_PREFIX}{proxy_tag}-{run_number}'

    backend_config = {"bucket": TFSTATE_BUCKET,
                      "key": thstate_key, "region": TFSTATE_REGION}
    terraform.init(backend_config=backend_config)
    return_code, stdout, stderr = terraform.apply(skip_plan=True)
    click.echo(f"code: {return_code}")
    click.echo(f"stdout: {stdout}")
    click.echo(f"stderr: {stderr}")
    with open(f"terraform.log", "w") as file:
        file.write(stdout)
        file.write(stderr)
    if return_code != 0:
        print("Terraform infrastructure is not built correctly")
        sys.exit(1)
    output = terraform.output(json=True)
    click.echo(f"output: {output}")
    proxy_ip = output["proxy_ip"]["value"]
    solana_ip = output["solana_ip"]["value"]
    infra = dict(solana_ip=solana_ip, proxy_ip=proxy_ip)
    set_github_env(infra)


def set_github_env(envs: tp.Dict, upper=True) -> None:
    """Set environment for github action"""
    path = os.getenv("GITHUB_ENV", str())
    if os.path.exists(path):
        with open(path, "a") as env_file:
            for key, value in envs.items():
                env_file.write(f"\n{key.upper() if upper else key}={str(value)}")


@cli.command(name="destroy_terraform")
@click.option('--proxy_tag')
@click.option('--run_number')
def destroy_terraform(proxy_tag, run_number):
    log = logging.getLogger()
    log.handlers = []
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)4s %(name)4s [%(filename)s:%(lineno)s - %(funcName)s()] %(levelname)4s %(message)4s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    def format_tf_output(output):
        return re.sub(r'(?m)^', ' ' * TF_OUTPUT_OFFSET, str(output))

    TF_OUTPUT_OFFSET = 16
    os.environ["TF_VAR_proxy_image_tag"] = proxy_tag
    os.environ["TF_VAR_dockerhub_org_name"] = DOCKERHUB_ORG_NAME
    thstate_key = f'{TFSTATE_KEY_PREFIX}{proxy_tag}-{run_number}'

    backend_config = {"bucket": TFSTATE_BUCKET,
                      "key": thstate_key, "region": TFSTATE_REGION}
    terraform.init(backend_config=backend_config)
    tf_destroy = terraform.apply('-destroy', skip_plan=True)
    log.info(format_tf_output(tf_destroy))


@cli.command(name="get_container_logs")
def get_all_containers_logs():
    home_path = os.environ.get("HOME")
    artifact_logs = "./logs"
    ssh_key = f"{home_path}/.ssh/ci-stands"
    os.mkdir(artifact_logs)
    proxy_ip = os.environ.get("PROXY_IP")
    solana_ip = os.environ.get("SOLANA_IP")

    subprocess.run(
        f'ssh-keygen -R {solana_ip} -f {home_path}/.ssh/known_hosts', shell=True)
    subprocess.run(
        f'ssh-keygen -R {proxy_ip} -f {home_path}/.ssh/known_hosts', shell=True)
    subprocess.run(
        f'ssh-keyscan -H {solana_ip} >> {home_path}/.ssh/known_hosts', shell=True)
    subprocess.run(
        f'ssh-keyscan -H {proxy_ip} >> {home_path}/.ssh/known_hosts', shell=True)
    ssh_client = SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname=solana_ip, username='root',
                       key_filename=ssh_key, timeout=120)

    upload_remote_logs(ssh_client, "opt_solana_1", artifact_logs)

    ssh_client.connect(hostname=proxy_ip, username='root',
                       key_filename=ssh_key, timeout=120)
    services = ["postgres", "dbcreation", "indexer", "proxy", "faucet"]
    for service in services:
        upload_remote_logs(ssh_client, service, artifact_logs)


def upload_remote_logs(ssh_client, service, artifact_logs):
    scp_client = SCPClient(transport=ssh_client.get_transport())
    click.echo(f"Upload logs for service: {service}")
    ssh_client.exec_command(f"touch /tmp/{service}.log.bz2")
    stdin, stdout, stderr = ssh_client.exec_command(
        f'sudo docker logs {service} 2>&1 | pbzip2 -f > /tmp/{service}.log.bz2')
    print(stdout.read())
    print(stderr.read())
    stdin, stdout, stderr = ssh_client.exec_command(f'ls -lh /tmp/{service}.log.bz2')
    print(stdout.read())
    print(stderr.read())
    scp_client.get(f'/tmp/{service}.log.bz2', artifact_logs)


@cli.command(name="deploy_check")
@click.option('--proxy_tag', help="the neon proxy image tag")
@click.option('--neon_evm_tag', help="the neon evm_loader image tag")
@click.option('--faucet_tag', help="the neon faucet image tag")
@click.option('--head_ref_branch')
@click.option('--github_ref_name')
@click.option('--test_files', help="comma-separated file names if you want to run a specific list of tests")
@click.option('--skip_pull', is_flag=True, default=False, help="skip pulling of docker images from the docker-hub")
def deploy_check(proxy_tag, neon_evm_tag, faucet_tag, head_ref_branch, github_ref_name, test_files, skip_pull):
    feature_branch = head_ref_branch if head_ref_branch != "" else github_ref_name
    neon_evm_tag = update_neon_evm_tag_if_same_branch_exists(head_ref_branch, neon_evm_tag)
    if feature_branch not in ['master', 'develop']:
        faucet_tag = update_faucet_tag_if_same_branch_exists(feature_branch, faucet_tag)

    os.environ["REVISION"] = proxy_tag
    os.environ["NEON_EVM_COMMIT"] = neon_evm_tag
    os.environ["FAUCET_COMMIT"] = faucet_tag
    project_name = proxy_tag
    cleanup_docker(project_name)

    if not skip_pull:
        click.echo('pull docker images...')
        out = docker_compose(f"-p {project_name} -f docker-compose/docker-compose-ci.yml pull")
        click.echo(out)
    else:
        click.echo('skip pulling of docker images')

    try:
        docker_compose(f"-p {project_name} -f docker-compose/docker-compose-ci.yml up -d")
    except:
        raise RuntimeError("Docker-compose failed to start")

    containers = ["".join(item['Names']).replace("/", "")
                  for item in docker_client.containers() if item['State'] == 'running']
    click.echo(f"Running containers: {containers}")

    for service_name in ['SOLANA', 'PROXY', 'FAUCET']:
        wait_for_service(project_name, service_name)

    if test_files is None:
        test_list = get_test_list(project_name)
    else:
        test_list = test_files.split(',')

    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        errors_count_list = p.starmap(run_test, [(project_name, x) for x in test_list])

    errors_count = sum(errors_count_list)
    if errors_count > 0:
        raise RuntimeError(f"Tests failed! Errors count: {errors_count}")


def get_test_list(project_name):
    inst = docker_client.exec_create(
        f"{project_name}_proxy_1", 'find . -type f -name "test_*.py" -printf "%f\n"')
    out = docker_client.exec_start(inst['Id'])
    test_list = out.decode('utf-8').strip().split('\n')
    return test_list


def run_test(project_name, file_name):
    click.echo(f"Running {file_name} tests")
    env = {"TESTNAME": file_name}
    local_docker_client = docker.APIClient()
    inst = local_docker_client.exec_create(
        f"{project_name}_proxy_1", './proxy/deploy-test.sh', environment=env)
    out, test_logs = local_docker_client.exec_start(inst['Id'], demux=True)
    test_logs = test_logs.decode('utf-8')
    click.echo(out)
    click.echo(test_logs)
    errors_count = 0
    for line in test_logs.split('\n'):
        if re.match(r"FAILED \(.+=\d+", line):
            errors_count += int(re.search(r"\d+", line).group(0))
    return errors_count


@cli.command(name="dump_apps_logs")
@click.option('--proxy_tag', help="the neon proxy image tag")
def dump_apps_logs(proxy_tag):
    for container in [f"{proxy_tag}_{item}_1" for item in CONTAINERS]:
        dump_docker_logs(container)


def dump_docker_logs(container):
    try:
        logs = docker_client.logs(container).decode("utf-8")
        with open(f"{container}.log", "w") as file:
            file.write(logs)
    except (docker.errors.NotFound):
        click.echo(f"Container {container} does not exist")


@cli.command(name="stop_containers")
@click.option('--proxy_tag', help="the neon proxy image tag")
def stop_containers(proxy_tag):
    cleanup_docker(proxy_tag)


def cleanup_docker(project_name):
    click.echo(f"Cleanup docker-compose...")

    docker_compose(f"-p {project_name} -f docker-compose/docker-compose-ci.yml down -t 1")
    click.echo(f"Cleanup docker-compose done.")

    click.echo(f"Removing temporary data volumes...")
    command = "docker volume prune -f"
    subprocess.run(command, shell=True)
    click.echo(f"Removing temporary data done.")


def get_service_url(project_name: str, service_name: str):
    inspect_out = docker_client.inspect_container(f"{project_name}_proxy_1")
    env = inspect_out["Config"]["Env"]
    service_url = ""
    for item in env:
        if f"{service_name}_URL=" in item:
            service_url = item.replace(f"{service_name}_URL=", "")
            break
    click.echo(f"service_url: {service_url}")
    return service_url


def wait_for_service(project_name: str, service_name: str):
    service_url = get_service_url(project_name, service_name)
    service_info = urlparse(service_url)
    service_ip, service_port = service_info.hostname, service_info.port

    command = f'docker exec {project_name}_proxy_1 nc -zvw1 {service_ip} {service_port}'
    timeout_sec = 120
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_sec:
            raise RuntimeError(f'Service {service_name} {service_url} is unavailable - time is over')
        try:
            if subprocess.run(command, shell=True, capture_output=True, text=True).returncode == 0:
                click.echo(f"Service {service_name} is available")
                break
            else:
                click.echo(f"Service {service_name} {service_url} is unavailable - sleeping")
        except:
            raise RuntimeError(f"Error during run command {command}")
        time.sleep(1)


@cli.command(name="send_notification", help="Send notification to slack")
@click.option("-u", "--url", help="slack app endpoint url.")
@click.option("-b", "--build_url", help="github action test build url.")
def send_notification(url, build_url):
    tpl = ERR_MSG_TPL.copy()

    parsed_build_url = urlparse(build_url).path.split("/")
    build_id = parsed_build_url[-1]
    repo_name = f"{parsed_build_url[1]}/{parsed_build_url[2]}"

    tpl["blocks"][0]["text"]["text"] = (
        f"*Build <{build_url}|`{build_id}`> of repository `{repo_name}` is failed.*"
        f"\n<{build_url}|View build details>"
    )
    requests.post(url=url, data=json.dumps(tpl))


def process_output(output):
    for line in output:
        if line:
            errors = set()
            try:
                if "status" in line:
                    click.echo(line["status"])

                elif "stream" in line:
                    stream = re.sub("^\n", "", line["stream"])
                    stream = re.sub("\n$", "", stream)
                    stream = re.sub("\n(\x1B\[0m)$", "\\1", stream)
                    if stream:
                        click.echo(stream)

                elif "aux" in line:
                    if "Digest" in line["aux"]:
                        click.echo("digest: {}".format(line["aux"]["Digest"]))

                    if "ID" in line["aux"]:
                        click.echo("ID: {}".format(line["aux"]["ID"]))

                else:
                    click.echo("not recognized (1): {}".format(line))

                if "error" in line:
                    errors.add(line["error"])

                if "errorDetail" in line:
                    errors.add(line["errorDetail"]["message"])

                    if "code" in line:
                        error_code = line["errorDetail"]["code"]
                        errors.add("Error code: {}".format(error_code))

            except ValueError as e:
                click.echo("not recognized (2): {}".format(line))

            if errors:
                message = "problem executing Docker: {}".format(". ".join(errors))
                raise SystemError(message)


if __name__ == "__main__":
    cli()
