import os
import csv
import json
import boto3
import socket
import random
import logging
import pathlib
import asyncio

from typing import List
from datetime import datetime
from botocore.exceptions import ClientError

from typorio.core import constants
from typorio.core.models import User

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
            self,
            user: User,
            bucket: str,
            max_rows: int = 1000,
            verbose: bool = False,
            dry_run: bool = False,
            shuffle: bool = True,
            home: str = "microtypo",
            push_interval: int = 60 * 30
    ):
        self.rows: List[List[str]] = []

        self.user = user
        self.bucket = bucket
        self.shuffle = shuffle
        self.verbose = verbose
        self.dry_run = dry_run
        self.max_rows = max_rows
        self.push_interval = push_interval

        self.os_dir = pathlib.Path.home()
        self.home_dir = self.os_dir / home
        self.records_dir = self.os_dir / home / "records"
        os.makedirs(self.records_dir, exist_ok=True)

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

    def write(self, event: int, key: str, meta: dict):
        hostname = socket.gethostname()
        timestamp = datetime.utcnow()
        meta = json.dumps(meta or {})

        row = [
            event,
            key,
            meta,
            hostname,
            timestamp,
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
            with open(records_path, "a+") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=constants.HEADERS)

                if not records_path_exists:
                    writer.writeheader()

                if self.shuffle:
                    random.shuffle(self.rows)

                for row in self.rows:
                    writer.writerow(dict(zip(constants.HEADERS, row)))
        except Exception as exc:
            logger.error(exc)
        finally:
            self.rows = []

    def push_data(self):
        object_name = self.get_object_name()
        records_path = self.get_records_path()
        client = boto3.client("s3", region_name="ap-southeast-1")

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

        try:
            with open(read_path, "r") as readfile, open(write_path, "a+") as writefile:
                reader = csv.DictReader(readfile, fieldnames=constants.HEADERS)
                writer = csv.DictWriter(writefile, fieldnames=constants.HEADERS)

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
