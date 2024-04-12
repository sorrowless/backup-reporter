import boto3
import json
import logging
import pytz

from abc import ABC
from datetime import datetime
from backup_reporter.dataclass import BackupMetadata
from backup_reporter.utils import exec_cmd
from fnmatch import fnmatch


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
            aws_endpoint_url: str = None) -> None:
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.aws_endpoint_url = aws_endpoint_url
        self.s3_path = s3_path

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
        logging.info(f"Uploud metadata to {self.s3_path} ...")
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
        metadata_file_name = "/".join(self.s3_path.split("/")[3:])
        s3_path = self.s3_path.split("/")[2]
        s3.Object(s3_path, metadata_file_name).put(Body=str(metadata))
        logging.info(f"Uploud metadata success")

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
            aws_endpoint_url: str = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "DockerPostgres",
            description = description,
            aws_endpoint_url = aws_endpoint_url)

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
            backup_time = datetime.strptime(backup.get('time'), '%Y-%m-%dT%H:%M:%SZ')
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

        s3_path = "/".join(self.s3_path.split("/")[:3])
        self.metadata.placement = s3_path

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
            aws_endpoint_url: str = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "FilesBucket",
            description = description,
            aws_endpoint_url = aws_endpoint_url)

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

        bucket_name = self.s3_path.split("/")[2]
        s3 = s3.Bucket(bucket_name)

        latest_backup = {"key": None, "last_modified": datetime(2000, 1, 1, tzinfo=pytz.UTC), "size": 0} # Default latest backup
        count_of_backups = 0
        # Get latest backup file
        for object in s3.objects.all():
            if fnmatch(object.key, self.files_mask): # Check if object name matches with files mask from config file
                if latest_backup["last_modified"] < object.last_modified:
                    latest_backup = {"key": object.key, "last_modified": object.last_modified, "size": object.size}
                count_of_backups += 1

        self.metadata.count_of_backups = count_of_backups
        self.metadata.last_backup_date = latest_backup["last_modified"]
        self.metadata.backup_name = latest_backup["key"]
        self.metadata.placement = bucket_name
        self.metadata.size = round(latest_backup["size"]/1024/1024, 1)
        self.metadata.time = 0

        return self.metadata
