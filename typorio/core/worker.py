import os
import csv
import json
import boto3
import socket
import logging
import pathlib
import asyncio

from typing import List
from datetime import datetime

from typorio.core import constants
from typorio.core.models import User
from typorio import utils

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
            self,
            env: str,
            profile: str = "typo",
            max_rows: int = 1000,
            verbose: bool = False,
            dry_run: bool = False,
            shuffle: bool = True,
            home: str = "microtypo",
            push_interval: int = 60 * 10
    ):
        self.env = env
        self.user = None
        self.profile = profile
        self.rows: List[List[str]] = []
        self.bucket = self.get_bucket_name()

        self.shuffle = shuffle
        self.verbose = verbose
        self.dry_run = dry_run
        self.max_rows = max_rows
        self.push_interval = push_interval

        self.os_dir = pathlib.Path.home()
        self.home_dir = self.os_dir / home
        self.records_dir = self.os_dir / home / "records"
        os.makedirs(self.records_dir, exist_ok=True)

    def get_session(self):
        return boto3.Session(profile_name=self.profile)

    def get_user(self):
        if self.user is None:
            try:
                iam = self.get_session().client("iam")
                sts = self.get_session().client("sts")
                response = sts.get_caller_identity()
                user_name = response.pop("Arn").split("/").pop()
                response = iam.list_user_tags(UserName=user_name)
                user_id = dict((item["Key"], item["Value"]) for item in response["Tags"]).get("Id")
                self.user = User(id=user_id, username=user_name)
            except Exception as exc:
                logger.error(exc)
        return self.user

    def get_bucket_name(self):
        buckets = dict(
            dev="microtypo-dev-bucket",
            stage="microtypo-stage-bucket",
            prod="microtypo-prod-bucket",
        )

        return buckets.get(self.env, buckets["prod"])

    def get_records_path(self):
        date = datetime.utcnow().date()
        path = self.records_dir / f"records.{date}.csv"
        return path

    def get_backup_path(self):
        date = datetime.utcnow().date()
        path = self.records_dir / f"records.{date}.bak.csv"
        return path

    def get_object_name(self):
        now = datetime.utcnow()
        timestamp = now.isoformat()
        partition = now.strftime("%Y-%m")
        filename = f"records.{timestamp}.csv"
        object_name = f"records/{partition}/{self.user.id}/{filename}"
        return object_name

    def write_data(self, event: str, key: str, meta: dict):
        hostname = socket.gethostname()
        timestamp = datetime.utcnow()
        meta = json.dumps(meta or {})

        row = [
            event,
            key,
            meta,
            hostname,
            str(timestamp),
        ]
        self.rows.append(row)

        if self.verbose:
            print(f" Row: {row} | Total: {len(self.rows)}")

        if self.dry_run:
            return

        if len(self.rows) >= self.max_rows:
            self.flush_data()

    def flush_data(self):
        records_path = self.get_records_path()
        records_path_exists = records_path.exists()

        try:
            with open(records_path, "a+", encoding="utf-8", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=constants.HEADERS)

                if not records_path_exists:
                    writer.writeheader()

                if self.shuffle:
                    self.rows = utils.shuffle_rows(self.rows)

                for row in self.rows:
                    writer.writerow(dict(zip(constants.HEADERS, row)))
        except Exception as exc:
            logger.error(exc)
        else:
            self.rows = []

    def push_data(self):
        if not self.get_user():
            return

        object_name = self.get_object_name()
        records_path = self.get_records_path()
        client = self.get_session().client("s3", region_name="ap-southeast-1")

        if not records_path.exists():
            return

        try:
            client.upload_file(records_path, self.bucket, object_name)
        except Exception as exc:
            logger.error(exc)
        else:
            self.move_data()

    def move_data(self):
        read_path = self.get_records_path()
        write_path = self.get_backup_path()
        write_path_exist = write_path.exists()

        try:
            with (
                open(read_path, "r", encoding="utf-8", newline="") as readfile,
                open(write_path, "a+", encoding="utf-8", newline="") as writefile,
            ):
                reader = csv.DictReader(readfile, fieldnames=constants.HEADERS)
                writer = csv.DictWriter(writefile, fieldnames=constants.HEADERS)

                if write_path_exist:
                    next(reader)  # avoid duplicated headers

                for row in reader:
                    writer.writerow(row)
        except Exception as exc:
            logger.error(exc)
        else:
            read_path.unlink()

    async def sync_data(self):
        while True:
            await asyncio.sleep(self.push_interval)
            self.push_data()

    async def backup_data(self):
        # TODO: retrieve .bak files
        pass
