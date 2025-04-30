#!/usr/bin/env python3

import collections
import dataclasses
import json
import hashlib
import logging
import os
import re
import time
import urllib.parse

import tarfile
import tempfile

import boto3
import click

from pydantic import BaseModel, Field
from contextlib import contextmanager


# Partial schema of goreleaser metadata.json
class Metadata(BaseModel):
    tag: str
    version: str

class ArtifactExtra(BaseModel):
    id: str | None = Field(alias='ID')

# Partial schema of goreleaser artifacts.json
class Artifact(BaseModel):
    name: str
    path: str
    type: str
    goos: str | None = None
    goarch: str | None = None
    extra: ArtifactExtra | None = None


@dataclasses.dataclass
class PluginConfig:
    """Encapsulates config specific to a plugin (like `connect`)"""

    plugin_name: str
    binary_name: str

    # All these path methods return S3 paths
    def get_manifest_path(self) -> str:
        return f"{self.plugin_name}/manifest.json"

    def get_archives_root_path(self) -> str:
        return f"{self.plugin_name}/archives"

    def get_archives_version_dir_path(self, version: str) -> str:
        return f"{self.get_archives_root_path()}/{version}"

    def get_archive_full_path(self, binary_artifact: Artifact, version: str) -> str:
        return f"{self.get_archives_version_dir_path(version)}/{binary_artifact.name}-{binary_artifact.goos}-{binary_artifact.goarch}.tar.gz"


def get_plugin_config(plugin_name: str) -> PluginConfig:
    return PluginConfig(plugin_name=plugin_name, binary_name=f"redpanda-{plugin_name}")


def get_binary_sha256_digest(filepath: str) -> str:
    with open(filepath, "rb") as f:
        s = hashlib.sha256(f.read())
    return s.hexdigest()


def get_artifacts(artifacts_file: str) -> list[Artifact]:
    with open(artifacts_file, "r") as f:
        data = json.load(f)
    assert isinstance(data, list), f"Expected {artifacts_file} to contain a JSON list payload"
    result = []
    for item in data:
        artifact = Artifact(**item)
        result.append(artifact)
    return result


def get_metadata(metadata_file: str) -> Metadata:
    with open(metadata_file, "r") as f:
        data = json.load(f)
    assert isinstance(data, dict), f"Expected {metadata_file} to contain a JSON dict payload"
    return Metadata(**data)


class S3BucketClient:
    """A wrapper around boto3 S3 client that knows the bucket it works with.
    Comes with higher level methods as needed."""

    def __init__(self, bucket: str, region: str):
        self._client = boto3.client("s3", region_name=region)
        self._bucket = bucket

    def upload_file_with_tags(
            self, file: str, object_path: str, tags: dict[str, str] = {}
    ):
        with open(file, "rb") as f:
            return self.upload_blob_with_tags(f.read(), object_path, tags=tags)

    def upload_blob_with_tags(
            self, data: bytes, object_path: str, tags: dict[str, str] = {}
    ):
        self._client.put_object(
            Bucket=self._bucket,
            Body=data,
            Key=object_path,
            # We want users to receive latest stuff promptly.
            # This minimizes inconsistencies between manifest.json and archives when served over
            # Cloudfront
            CacheControl="max-age=1",
            Tagging=urllib.parse.urlencode(tags),
        )

    def list_dir_recursive(self, s3_dir_path: str | None = None) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        if s3_dir_path is None:
            pages = paginator.paginate(Bucket=self._bucket)
        else:
            pages = paginator.paginate(Bucket=self._bucket, Prefix=s3_dir_path)

        keys = []
        for page in pages:
            # Indicates empty results, break out immediately
            if "Contents" not in page:
                break
            for obj in page["Contents"]:
                keys.append(obj["Key"])
        return keys

    def get_object_tags(self, object_path: str) -> dict[str, str]:
        response = self._client.get_object_tagging(
            Bucket=self._bucket,
            Key=object_path,
        )
        result = {}
        for tag in response["TagSet"]:
            result[tag["Key"]] = tag["Value"]
        return result


def create_tar_gz_archive(single_filepath: str) -> str:
    tmp_archive = tempfile.mktemp()
    with tarfile.open(tmp_archive, "w:gz") as tar:
        tar.add(single_filepath, arcname=os.path.basename(single_filepath))
    return tmp_archive


TAG_BINARY_NAME = "redpanda/binary_name"
TAG_BINARY_SHA256 = "redpanda/binary_sha256"
TAG_GOOS = "redpanda/goos"
TAG_GOARCH = "redpanda/goarch"
TAG_VERSION = "redpanda/version"


@contextmanager
def cwd(new_dir: str):
    # Code to acquire resource, e.g.:
    old_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(old_dir)


def create_and_upload_one_archive(artifact: Artifact, plugin_config: PluginConfig, project_root_dir: str, version: str,
                                  bucket: str, region: str, dry_run: bool):
    if dry_run:
        s3_bucket_client = None
    else:
        s3_bucket_client = S3BucketClient(bucket, region)
    logging.info(f"Processing {artifact}")

    with cwd(project_root_dir):
        binary_sha256 = get_binary_sha256_digest(artifact.path)
        logging.info(f"Binary SHA256 = {binary_sha256}")
        tmp_archive = None
        try:
            tmp_archive = create_tar_gz_archive(artifact.path)
            logging.info(f"Created archive {tmp_archive}")
            s3_path_for_archive = plugin_config.get_archive_full_path(
                binary_artifact=artifact, version=version
            )

            tags = {
                TAG_BINARY_NAME: plugin_config.binary_name,
                TAG_BINARY_SHA256: binary_sha256,
                TAG_GOOS: artifact.goos,
                TAG_GOARCH: artifact.goarch,
                TAG_VERSION: version,
            }
            if dry_run:
                logging.info(
                    f"DRY-RUN - Would have uploaded archive to S3 bucket {bucket} as {s3_path_for_archive}"
                )
                logging.info(f"Tags: {json.dumps(tags, indent=4)}")
            else:
                logging.info(
                    f"Uploading archive to S3 bucket {bucket} as {s3_path_for_archive}"
                )
                assert (
                        s3_bucket_client is not None
                ), "s3_bucket_client should be initialized in non-dry-run mode"
                s3_bucket_client.upload_file_with_tags(
                    file=tmp_archive, object_path=s3_path_for_archive, tags=tags
                )
        finally:
            if tmp_archive and os.path.exists(tmp_archive):
                os.unlink(tmp_archive)
        logging.info("DONE")


def create_and_upload_archives(
        project_root_dir: str,
        plugin_config: PluginConfig,
        artifacts: list[Artifact],
        bucket: str,
        region: str,
        version: str,
        dry_run: bool,
):
    for artifact in artifacts:
        create_and_upload_one_archive(
            artifact=artifact,
            plugin_config=plugin_config,
            project_root_dir=project_root_dir,
            version=version,
            bucket=bucket,
            region=region,
            dry_run=dry_run,
        )


def get_max_version_str(version_strs: list[str]) -> str | None:
    max_version = None
    max_version_tuple = None
    for version in version_strs:
        # Only real releases are eligible to be latest.  E.g. no RCs.
        m = re.search(r"^(\d+)\.(\d+).(\d+)$", version)
        if not m:
            continue
        version_tuple = (int(m[1]), int(m[2]), int(m[3]))
        if max_version_tuple is None or version_tuple > max_version_tuple:
            max_version_tuple = version_tuple
            max_version = version
    return max_version


def get_object_tags_for_keys(
        s3_bucket_client: S3BucketClient, keys: list[str]
) -> dict[str, dict[str, str]]:
    return {k: s3_bucket_client.get_object_tags(k) for k in keys}


def create_and_upload_manifest_json(
        plugin_config: PluginConfig,
        bucket: str,
        region: str,
        repo_hostname: str,
        dry_run: bool,
):
    # Even for dry-run mode, we will READ from S3 bucket. We just won't write anything to S3.
    # Therefore, S3 creds are needed even for --dry-run
    s3_bucket_client = S3BucketClient(bucket, region)
    list_path = plugin_config.get_archives_root_path().rstrip("/") + "/"
    logging.info(f"Listing all objects in bucket {bucket} under path {list_path}")
    keys = s3_bucket_client.list_dir_recursive(list_path)

    object_tags_for_keys = get_object_tags_for_keys(s3_bucket_client, keys)

    archives = []
    manifest = {
        "created_at": int(time.time()),
        "archives": archives,
    }
    version_to_artifact_infos: dict[str, list[dict[str, str]]] = (
        collections.defaultdict(list)
    )
    for key, tag_map in object_tags_for_keys.items():
        try:
            binary_name = tag_map[TAG_BINARY_NAME]
            if binary_name != plugin_config.binary_name:
                logging.info(f"Skipping {key}, wrong binary name: {binary_name}")
                continue
            logging.info(f"Found {key} with tags: {tag_map}")
            version_to_artifact_infos[tag_map[TAG_VERSION]].append(
                {
                    "binary_name": tag_map[TAG_BINARY_NAME],
                    "binary_sha256": tag_map[TAG_BINARY_SHA256],
                    "goos": tag_map[TAG_GOOS],
                    "goarch": tag_map[TAG_GOARCH],
                    "path": key,
                }
            )
        except KeyError as ke:
            logging.info(f"Skipping {key}, missing tag: {ke}")
            continue

    max_version: str | None = None
    if not version_to_artifact_infos:
        logging.warning(f"No artifacts found in bucket {bucket} for {plugin_config.plugin_name}")
    else:
        max_version = get_max_version_str(list(version_to_artifact_infos))
        if max_version is None:
            logging.warning("No real releases found (may be only RCs?)")
            logging.info(f"All versions found: {list(version_to_artifact_infos)}")

    for version, artifact_infos in version_to_artifact_infos.items():
        artifacts: dict[str, dict[str, str]] = {}
        for artifact_info in artifact_infos:
            artifacts[f"{artifact_info['goos']}-{artifact_info['goarch']}"] = {
                "path": f"https://{repo_hostname}/{artifact_info["path"]}",
                "sha256": artifact_info["binary_sha256"],
            }
        archive = {
            "version": version,
            "artifacts": artifacts,
        }
        if version == max_version:
            archive["is_latest"] = True
        archives.append(archive)
    logging.info("Manifest:")
    manifest_json = json.dumps(manifest, indent=4, sort_keys=True)
    logging.info(manifest_json)
    if dry_run:
        logging.info(
            f"DRY-RUN - Would have uploaded manifest.json to {plugin_config.get_manifest_path()}"
        )
    else:
        logging.info(f"Uploading manifest.json to {plugin_config.get_manifest_path()}")
        s3_bucket_client.upload_blob_with_tags(
            object_path=plugin_config.get_manifest_path(),
            data=manifest_json.encode("utf-8"),
        )


@click.group(help="CLI tool to upload/index goreleaser-built binaries to/in S3.")
def cli():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )


@cli.command(name="upload-archives", help="Create tar.gz archives from binaries and upload to S3")
@click.option(
    "--artifacts-file",
    required=True,
    help="artifacts.json file produced by `goreleaser`",
)
@click.option(
    "--metadata-file", required=True, help="metadata.json file produced by `goreleaser`"
)
@click.option(
    "--project-root-dir", required=True,
    help="Root directory of the Go project.  File paths within artifacts.json are relative to this directory."
)
@click.option("--region", required=True)
@click.option("--bucket", required=True)
@click.option("--plugin", required=True, help="Plugin to process. E.g. `connect`")
@click.option(
    "--goos",
    required=True,
    help="CSV list of OSes to process binaries for. E.g. 'linux,darwin'",
)
@click.option(
    "--goarch",
    required=True,
    help="CSV list of architectures to process binaries for. E.g. 'amd64,arm64'",
)
@click.option(
    "--deduce-version-from-tag",
    is_flag=True,
    help="Deduce version from tag in metadata.json",
)
@click.option("--dry-run", is_flag=True, )
def upload_archives(
        artifacts_file: str,
        metadata_file: str,
        project_root_dir: str,
        region: str,
        bucket: str,
        plugin: str,
        goos: str,
        goarch: str,
        deduce_version_from_tag: bool,
        dry_run: bool,
):
    goos_list = goos.split(",")
    goarch_list = goarch.split(",")
    plugin_config = get_plugin_config(plugin)
    artifacts = get_artifacts(artifacts_file)
    if deduce_version_from_tag:
        version = get_metadata(metadata_file).tag.lstrip("v")
    else:
        version = get_metadata(metadata_file).version
    artifacts_to_process = [
        a
        for a in artifacts
        if a.type == "Binary"
           and a.name == plugin_config.binary_name
           and (a.extra.id if a.extra else None) == plugin_config.plugin_name
           and a.goos in goos_list
           and a.goarch in goarch_list
    ]
    logging.info(f"Found {len(artifacts_to_process)} artifacts to process")
    for a in artifacts_to_process:
        logging.info(f"  {a}")
    create_and_upload_archives(
        project_root_dir=project_root_dir,
        plugin_config=plugin_config,
        artifacts=artifacts_to_process,
        version=version,
        region=region,
        bucket=bucket,
        dry_run=dry_run,
    )


@cli.command(name="upload-manifest", help="Create manifest.json and upload to S3")
@click.option("--bucket", required=True)
@click.option("--region", required=True)
@click.option("--repo-hostname", required=True)
@click.option("--plugin", required=True, help="Plugin to process. E.g. `connect`")
@click.option("--dry-run", is_flag=True)
def upload_manifest(
        bucket: str, region: str, repo_hostname: str, plugin: str, dry_run: bool
):
    plugin_config = get_plugin_config(plugin)
    create_and_upload_manifest_json(
        plugin_config=plugin_config,
        bucket=bucket,
        region=region,
        repo_hostname=repo_hostname,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    cli()
