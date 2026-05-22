import os
import pytz
import json
import boto3
import logging
import hashlib

from abc import ABC
from pathlib import Path
from fnmatch import fnmatch
from datetime import datetime
from urllib.parse import urlparse

from backup_reporter.utils import exec_cmd
from backup_reporter.dataclass import BackupMetadata, BackupFileInfo


class BackupReporter(ABC):
    '''
        Base backup reporter with common functionality.
        It is highly recommended not to rewrite methods (except _gather_metadata) in child classes.
    '''
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            type: str,
            customer: str,
            supposed_backups_count: str,
            description: str,
            aws_endpoint_url: str = None,
            destination_type: str = "s3",
            upload_path = str) -> None:

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.aws_endpoint_url = aws_endpoint_url
        self.s3_path = s3_path
        self.destination_type = destination_type
        self.upload_path = Path(upload_path)

        self.metadata = BackupMetadata()
        self.metadata.type = type
        self.metadata.customer = customer
        self.metadata.supposed_backups_count = supposed_backups_count
        self.metadata.description = description

    def _gather_metadata(self) -> BackupMetadata:
        '''
            Gather information about backup to dict of variables
            This method exists to be overridden in childs classes
        '''
        raise Exception('Method _gather_metadata must be overwritten in child class')

    def _upload_metadata(self, metadata: BackupMetadata) -> None:
        '''Upload metadata file to place, where backups stored'''

        if self.destination_type == "s3":
            logging.info(f"Upload metadata to {self.s3_path} ...")
            kwargs = {
                "aws_access_key_id": self.aws_access_key_id,
                "aws_secret_access_key": self.aws_secret_access_key,
                "region_name": self.aws_region,
                "endpoint_url": self.aws_endpoint_url
            }
            s3 = boto3.resource(
                's3',
                **{k:v for k,v in kwargs.items() if v is not None}
            )
            metadata_file_name = urlparse(self.s3_path).path.lstrip("/")
            bucket_name = urlparse(self.s3_path).netloc

            content_type = "text/plain; version=0.0.4" if metadata.format == "prom" else "application/json"

            s3.Object(bucket_name, metadata_file_name).put(Body=str(metadata), ContentType=content_type)

        elif self.destination_type == "host":
            logging.info(f"Upload metadata on host to {self.upload_path} ...")
            metadata_dir = self.upload_path.parents[0]
            if not metadata_dir.is_dir():
                try:
                    metadata_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise e

            with open(self.upload_path, "w", encoding="utf-8") as f:
                f.write(str(metadata))

        logging.info(f"Upload metadata success")

    def report(self) -> None:
        '''Check backup status, compile it to json metadata file and upload'''
        metadata = self._gather_metadata()
        self._upload_metadata(metadata)


class DockerPostgresBackupReporter(BackupReporter):
    '''
        Reporter for Postgresql running in containers.
        For working reporter require permissions to work with docker socket.
    '''
    def __init__(
            self,
            container_name: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            customer: str,
            supposed_backups_count: str,
            description: str,
            aws_endpoint_url: str = None,
            destination_type: str = "s3",
            upload_path: str = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "DockerPostgres",
            description = description,
            aws_endpoint_url = aws_endpoint_url,
            destination_type = destination_type,
            upload_path = upload_path)

        self.container_name = container_name
        self.metadata.last_backup_date = None

    def _gather_metadata(self) -> BackupMetadata:
        '''Gather information about backup to dict of variables'''
        logging.info(f"Gather metadata from {self.container_name} ...")
        wal_show = exec_cmd(["docker", "exec", "-i", self.container_name, "wal-g", "wal-show", "--detailed-json" ])
        wal_show = json.loads(wal_show)
        full_backup_count = 0
        last_full_backup_date = None
        incremental_backup_count = 0
        for backup in wal_show[0]['backups']:
            backup_time = datetime.strptime(backup.get('time').rstrip('Z').split('.')[0] + 'Z', '%Y-%m-%dT%H:%M:%SZ')
            backup_time = backup_time.replace(tzinfo=pytz.UTC, microsecond=0)
            if not self.metadata.last_backup_date or backup_time > self.metadata.last_backup_date:
                self.metadata.last_backup_date = backup_time  # Beware, this is ALWAYS about LAST backup - full or incremental
                self.metadata.backup_name = backup.get("backup_name", "None")  # Also ALWAYS about LAST backup
                self.metadata.size = round(backup.get("compressed_size", "None") /1024/1024, 1)  # Can be overridden below
                finish_time = datetime.strptime(backup.get('finish_time'), backup.get('date_fmt'))  # Can be overridden below
                start_time = datetime.strptime(backup.get('start_time'), backup.get('date_fmt'))  # Can be overridden below
                self.metadata.time = str(finish_time - start_time)  # Can be overridden below

            backup_wal_file_name = backup.get("wal_file_name", "Unknown")
            if backup['backup_name'].endswith(backup_wal_file_name):  # If so, we're looking at full backup
                full_backup_count += 1
                if not last_full_backup_date or backup_time > last_full_backup_date:  # Override backup info with the size of last full backup
                    last_full_backup_date = backup_time
                    self.metadata.size = round(backup.get("compressed_size", "None") /1024/1024, 1)
                    finish_time = datetime.strptime(backup.get('finish_time'), backup.get('date_fmt'))  # Can be overridden below
                    start_time = datetime.strptime(backup.get('start_time'), backup.get('date_fmt'))  # Can be overridden below
                    self.metadata.time = str(finish_time - start_time)  # Can be overridden below
            else:
                incremental_backup_count += 1
        # Now we have to serialize last backup date
        self.metadata.last_backup_date = str(self.metadata.last_backup_date)

        self.metadata.count_of_backups = f"{len(wal_show[0]['backups'])} total / {full_backup_count} full / {incremental_backup_count} incremental"

        bucket_name = urlparse(self.s3_path).netloc
        self.metadata.placement = bucket_name

        if self.upload_path:
            self.metadata.format = self.upload_path.suffix.split('.')[-1]
        elif self.s3_path:
            self.metadata.format = self.s3_path.split('.')[-1]

        logging.info("Gather metadata success")
        logging.debug(self.metadata)

        return self.metadata


class FilesBucketReporterBackupReporter(BackupReporter):
    '''
        Report about backups from S3 bucket with plain files. Usually they are 1 file per 1 backup, but different schemes are available.
    '''
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            customer: str,
            supposed_backups_count: str,
            description: str,
            files_mask: str,
            aws_endpoint_url: str = None,
            destination_type: str = "s3",
            upload_path: Path = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "FilesBucket",
            description = description,
            aws_endpoint_url = aws_endpoint_url,
            destination_type = destination_type,
            upload_path = upload_path)

        self.metadata.last_backup_date = None
        self.files_mask = files_mask

    def _gather_metadata(self) -> BackupMetadata:
        '''
            Gather information about backup from files in S3
        '''
        kwargs = {
           "aws_access_key_id": self.aws_access_key_id,
           "aws_secret_access_key": self.aws_secret_access_key,
           "region_name": self.aws_region,
           "endpoint_url": self.aws_endpoint_url
        }
        s3 = boto3.resource(
            's3',
            **{k:v for k,v in kwargs.items() if v is not None}
        )

        bucket_name = urlparse(self.s3_path).netloc
        s3 = s3.Bucket(bucket_name)

        latest_backup = {"key": None, "last_modified": datetime(2000, 1, 1, tzinfo=pytz.UTC), "size": 0} # Default latest backup
        count_of_backups = 0
        # Get latest backup file
        for obj in s3.objects.all():
            if fnmatch(obj.key, self.files_mask): # Check if object name matches with files mask from config file
                if latest_backup["last_modified"] < obj.last_modified:
                    latest_backup = {"key": obj.key, "last_modified": obj.last_modified, "size": obj.size}
                self.metadata.backups.append(BackupFileInfo(
                    size=round(obj.size/1024/1024, 1),
                    backup_date=obj.last_modified,
                    backup_name=obj.key
                ))
                count_of_backups += 1

        self.metadata.count_of_backups = count_of_backups
        self.metadata.last_backup_date = latest_backup["last_modified"]
        self.metadata.backup_name = latest_backup["key"]
        self.metadata.placement = bucket_name
        self.metadata.size = round(latest_backup["size"]/1024/1024, 1)
        self.metadata.time = 0

        if self.upload_path:
            self.metadata.format = self.upload_path.suffix.split('.')[-1]
        elif self.s3_path:
            self.metadata.format = self.s3_path.split('.')[-1]

        return self.metadata

class FilesReporterBackupReporter(BackupReporter):
    '''
        Report about backups from plain files. Usually they are 1 file per 1 backup, but different schemes are available.
    '''
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            customer: str,
            supposed_backups_count: str,
            description: str,
            files_mask: str,
            backups_dir: str,
            aws_endpoint_url: str = None,
            destination_type: str = "s3",
            upload_path: Path = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "Files",
            description = description,
            aws_endpoint_url = aws_endpoint_url,
            destination_type = destination_type,
            upload_path = upload_path)

        self.files_mask = files_mask
        self.backups_dir = backups_dir

    def _gather_metadata(self) -> BackupMetadata:
        '''
            Gather information about backup from files in the host
        '''
        latest_backup = {"key": None, "last_modified": datetime(2000, 1, 1, tzinfo=pytz.UTC), "size": 0} # Default latest backup
        count_of_backups = 0
        # Get latest backup file
        for dirpath, _, filenames in os.walk(self.backups_dir):
            for filename in filenames:
                if fnmatch(filename, self.files_mask):
                    count_of_backups += 1
                    filepath = os.path.join(dirpath, filename)
                    try:
                        stat = os.stat(filepath)
                    except FileNotFoundError:
                        continue

                    sha1 = hashlib.sha1()
                    try:
                        with open(filepath, "rb") as f:
                            while chunk := f.read(8192):
                                sha1.update(chunk)
                        sha1sum = sha1.hexdigest()
                    except Exception as e:
                        logging.error(f"Can not read the file!")

                    mtime = datetime.fromtimestamp(stat.st_mtime, tz=pytz.UTC)
                    if mtime > latest_backup["last_modified"]:
                        latest_backup = {"key": filename, "last_modified": mtime, "size": stat.st_size, "sha1sum": sha1sum}

                    self.metadata.backups.append(BackupFileInfo(
                        size=round(stat.st_size/1024/1024, 1),
                        backup_date=mtime.replace(microsecond=0),
                        backup_name=filename,
                        sha1sum=sha1sum
                    ))

        if latest_backup["key"]: # If at least one file exists
            self.metadata.count_of_backups = count_of_backups
            self.metadata.last_backup_date = latest_backup["last_modified"]
            self.metadata.backup_name = latest_backup["key"]
            self.metadata.placement = self.backups_dir
            self.metadata.size = round(latest_backup["size"]/1024/1024, 1)
            self.metadata.time = 0
            self.metadata.sha1sum = latest_backup["sha1sum"]
            if self.upload_path:
                self.metadata.format = self.upload_path.suffix.split('.')[-1]
            elif self.s3_path:
                self.metadata.format = self.s3_path.split('.')[-1]

        logging.info("Gather metadata success")
        logging.debug(self.metadata)

        return self.metadata


class S3MariadbBackupReporter(BackupReporter):
    '''
        Reporter for uploaded to S3 files with backups.
    '''
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            customer: str,
            supposed_backups_count: str,
            description: str,
            aws_endpoint_url: str = None,
            destination_type: str = "s3",
            upload_path: Path = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "DockerMariadb",
            description = description,
            aws_endpoint_url = aws_endpoint_url,
            destination_type = destination_type,
            upload_path = upload_path)

        self.metadata.last_backup_date = None

    def _gather_metadata(self) -> BackupMetadata:
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.aws_region,
            "endpoint_url": self.aws_endpoint_url
        }
        s3 = boto3.client(
            's3',
            **{k:v for k,v in kwargs.items() if v is not None}
        )

        bucket_name = self.s3_path.split("/")[2]
        directories = []
        count_of_backups = 0
        backup_total_size = 0
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='mariadb/full/', Delimiter='/')
        if 'CommonPrefixes' in response:
            directories = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
            latest_full_backup = directories[-1]
            latest_date_of_backup = latest_full_backup.split('/')[-2]
            count_of_backups = len(directories)
            inc_path = 'mariadb/inc/' + latest_date_of_backup
            response_from_inc_path = s3.list_objects_v2(Bucket=bucket_name, Prefix=inc_path, Delimiter='/')
            if 'CommonPrefixes' in response_from_inc_path:
                directories = [prefix['Prefix'] for prefix in response_from_inc_path['CommonPrefixes']]
                latest_backup = directories[-1]
                latest_date_of_backup = latest_backup.split('/')[-2]
            else:
                logging.info("No directories found in the incremental path.")
                latest_backup = latest_full_backup
            objects_of_backup = s3.list_objects_v2(Bucket=bucket_name, Prefix=latest_backup)
            if 'Contents' in objects_of_backup:
                for obj in objects_of_backup['Contents']:
                    backup_total_size += obj['Size']
        else:
            logging.info("No directories found in the specified path.")
            latest_backup = 'None'
            count_of_backups = 0
            backup_total_size = 0
        self.metadata.count_of_backups = count_of_backups
        self.metadata.last_backup_date = latest_date_of_backup
        self.metadata.backup_name = latest_backup
        self.metadata.placement = bucket_name
        self.metadata.size = round(backup_total_size/1024/1024, 1)
        self.metadata.time = 0
        if self.upload_path:
            self.metadata.format = self.upload_path.suffix.split('.')[-1]
        elif self.s3_path:
            self.metadata.format = self.s3_path.split('.')[-1]

        return self.metadata
