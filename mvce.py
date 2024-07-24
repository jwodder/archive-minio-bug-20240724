from __future__ import annotations
from base64 import b64encode
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import subprocess
from time import sleep
from dandi.consts import DandiInstance, known_instances
from dandi.dandiapi import DandiAPIClient, RESTFullAPIClient
from dandischema.consts import DANDI_SCHEMA_VERSION
import requests

LOCAL_DOCKER_ENV = "docker-archive"
LOCAL_DOCKER_DIR = Path(__file__).with_name(LOCAL_DOCKER_ENV)

TEST_INSTANCE = "dandi-api-local-docker-tests"


@dataclass
class Archive:
    instance: DandiInstance
    api_token: str

    @property
    def instance_id(self) -> str:
        iid = self.instance.name
        assert isinstance(iid, str)
        return iid

    @property
    def api_url(self) -> str:
        url = self.instance.api
        assert isinstance(url, str)
        return url


@dataclass
class Entry:
    path: str
    blob: bytes
    base64md5: str

    @classmethod
    def make(cls, path: str, blob: bytes) -> Entry:
        base64md5 = b64encode(hashlib.md5(blob).digest()).decode("us-ascii")
        return cls(path=path, blob=blob, base64md5=base64md5)


def main() -> None:
    # logging.basicConfig(
    #     format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    #     datefmt="%H:%M:%S",
    #     level=logging.DEBUG,
    # )
    ENTRIES_ONE = [Entry.make("foo", b"This is foo.\n")]
    ENTRIES_TWO = [Entry.make("foo/bar", b"This is foo/bar.\n")]
    with docker_archive() as archive, DandiAPIClient.for_dandi_instance(
        archive.instance, token=archive.api_token
    ) as client:

        print("Creating Dandiset ...")
        d = client.create_dandiset(
            "Test Dandiset",
            {
                "schemaKey": "Dandiset",
                "name": "Test Dandiset",
                "description": "A test Dandiset",
                "contributor": [
                    {
                        "schemaKey": "Person",
                        "name": "Wodder, John",
                        "roleName": ["dcite:Author", "dcite:ContactPerson"],
                    }
                ],
                "license": ["spdx:CC0-1.0"],
            },
        )
        dandiset_id = d.identifier

        print("Creating Zarr ...")
        zarr_id = client.post(
            "/zarr/", json={"name": "conflicted.zarr", "dandiset": dandiset_id}
        )["zarr_id"]
        client.post(
            f"{d.version_api_path}assets/",
            json={
                "metadata": {
                    "path": "conflicted.zarr",
                    "description": "A Zarr with path conflicts",
                },
                "zarr_id": zarr_id,
            },
        )

        upload(client, zarr_id, ENTRIES_ONE)
        list_entries(client, zarr_id, expected=ENTRIES_ONE)
        print("Deleting all entries from Zarr")
        client.delete(
            f"/zarr/{zarr_id}/files/", json=[{"path": e.path} for e in ENTRIES_ONE]
        )
        upload(client, zarr_id, ENTRIES_TWO)
        list_entries(client, zarr_id, expected=ENTRIES_TWO)


def upload(client: DandiAPIClient, zarr_id: str, entries: list[Entry]) -> None:
    print("Uploading to Zarr:", ", ".join(e.path for e in entries))
    uploading = [{"path": e.path, "base64md5": e.base64md5} for e in entries]
    r = client.post(f"/zarr/{zarr_id}/files/", json=uploading)
    with RESTFullAPIClient(
        "http://nil.nil",
        headers={"X-Amz-ACL": "bucket-owner-full-control"},
    ) as storage:
        for signed_url, e in zip(r, entries):
            try:
                print(f"Uploading {e.path} to S3 backend ...")
                storage.put(
                    signed_url,
                    data=e.blob,
                    json_resp=False,
                    headers={"Content-MD5": e.base64md5},
                )
            except Exception as exc:
                print(f"ERROR UPLOADING {e.path}: {exc}")
    client.post(f"/zarr/{zarr_id}/finalize/")


def list_entries(client: DandiAPIClient, zarr_id: str, expected: list[Entry]) -> None:
    print(
        "Files in Zarr, per Archive:",
        [e["Key"] for e in client.paginate(f"/zarr/{zarr_id}/files/")],
    )
    print("  Expected files:", [e.path for e in expected])


@contextmanager
def docker_archive() -> Iterator[Archive]:
    env = {**os.environ, "DJANGO_DANDI_SCHEMA_VERSION": DANDI_SCHEMA_VERSION}
    try:
        if os.environ.get("DANDI_TESTS_PULL_DOCKER_COMPOSE", "1") not in ("", "0"):
            subprocess.run(
                ["docker", "compose", "pull"], cwd=LOCAL_DOCKER_DIR, check=True
            )
        subprocess.run(
            ["docker", "compose", "run", "--rm", "createbuckets"],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "django",
                "./manage.py",
                "migrate",
            ],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "django",
                "./manage.py",
                "createcachetable",
            ],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "-e",
                "DJANGO_SUPERUSER_PASSWORD=nsNc48DBiS",
                "django",
                "./manage.py",
                "createsuperuser",
                "--no-input",
                "--email",
                "admin@nil.nil",
            ],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            check=True,
        )
        r = subprocess.check_output(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "-T",
                "django",
                "./manage.py",
                "drf_create_token",
                "admin@nil.nil",
            ],
            cwd=LOCAL_DOCKER_DIR,
            env=env,
            text=True,
        )
        m = re.search(r"^Generated token (\w+) for user admin@nil.nil$", r, flags=re.M)
        if not m:
            raise RuntimeError(
                "Could not extract Django auth token from drf_create_token"
                f" output: {r!r}"
            )
        django_api_key = m[1]
        instance = known_instances[TEST_INSTANCE]
        subprocess.run(
            ["docker", "compose", "up", "-d", "django", "celery"],
            cwd=str(LOCAL_DOCKER_DIR),
            env=env,
            check=True,
        )
        for _ in range(25):
            try:
                requests.get(f"{instance.api}/dandisets/")
            except requests.ConnectionError:
                sleep(1)
            else:
                break
        else:
            raise RuntimeError("Django container did not start up in time")
        os.environ["DANDI_API_KEY"] = django_api_key  # For uploading
        yield Archive(instance=instance, api_token=django_api_key)
    finally:
        subprocess.run(
            ["docker", "compose", "down", "-v"], cwd=LOCAL_DOCKER_DIR, check=True
        )


if __name__ == "__main__":
    main()
