import boto3
import json
import logging

from abc import ABC
from datetime import datetime
from backup_reporter.dataclass import BackupMetadata
from backup_reporter.utils import exec_cmd


class BackupReporter(ABC):
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            type: str,
            customer: str,
            supposed_backups_count: str,
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
    def __init__(
            self, 
            container_name: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            customer: str,
            supposed_backups_count: str,
            aws_endpoint_url: str = None) -> None:

        super().__init__(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            aws_region = aws_region,
            s3_path = s3_path,
            customer = customer,
            supposed_backups_count = supposed_backups_count,
            type = "DockerPostgres",
            aws_endpoint_url = aws_endpoint_url)

        self.container_name = container_name

    def _gather_metadata(self) -> BackupMetadata:
        '''Gather information about backup to dict of variables'''
        logging.info(f"Gather metadata from {self.container_name} ...")
        wal_show = exec_cmd(["docker", "exec", "-it", self.container_name, "wal-g", "wal-show", "--detailed-json" ])
        wal_show = json.loads(wal_show)
        last_backup = wal_show[0]['backups'][-1]

        self.metadata.backup_name = last_backup.get("backup_name", "None")
        self.metadata.last_backup_date = last_backup.get("time", "None")
        self.metadata.size = last_backup.get("compressed_size", "None") /1024/1024
        self.metadata.count_of_backups = len(wal_show[0])

        s3_path = "/".join(self.s3_path.split("/")[:3])
        self.metadata.placement = s3_path

        finish_time = datetime.strptime(last_backup.get('finish_time'), last_backup.get('date_fmt'))
        start_time = datetime.strptime(last_backup.get('start_time'), last_backup.get('date_fmt'))
        self.metadata.time = str(finish_time - start_time)

        logging.info("Gather metadata success")
        return self.metadata
