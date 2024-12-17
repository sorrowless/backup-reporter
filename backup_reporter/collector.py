import os
import openpyxl
from openpyxl.styles import PatternFill
import json
import boto3
import logging
import datetime
import dateparser
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import openpyxl.worksheet

from backup_reporter.dataclass import BackupMetadata


class BackupCollector:
    def __init__(self, buckets: list,
            google_spreadsheet_credentials_path: str,
            spreadsheet_name: str,
            worksheet_name: str,
            sheet_owner: str) -> None:
        self.buckets = buckets
        self.credentials_path = google_spreadsheet_credentials_path
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_name = worksheet_name
        self.sheet_owner = sheet_owner

        self.color_neutral = "FFFFFF" # White
        self.color_warning = "f4ff00" # Orange
        self.color_alarm = "FF0004" # Red

    def _collect_from_bucket(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str,
            aws_endpoint_url: str = None) -> BackupMetadata:
        
        kwargs = {
           "aws_access_key_id": aws_access_key_id,
           "aws_secret_access_key": aws_secret_access_key,
           "region_name": aws_region,
           "endpoint_url": aws_endpoint_url
        }

        s3 = boto3.resource(
            's3',
            **{k:v for k,v in kwargs.items() if v is not None}
        )

        logging.info(f"Collect metadata from {s3_path} ...")

        metadata_file_name = "/".join(s3_path.split("/")[3:])
        s3_path = s3_path.split("/")[2]
        metadata = s3.Object(s3_path, metadata_file_name).get()['Body'].read().decode("utf-8")
        metadata = json.loads(metadata) 

        result = BackupMetadata()
        result.type = metadata.get("type", "None")
        result.size = metadata.get("size", "None")
        result.time = metadata.get("time", "None")
        result.customer = metadata.get("customer", "None")
        result.placement = metadata.get("placement", "None")
        result.backup_name = metadata.get("backup_name", "None")
        result.description = metadata.get("description", "None")
        result.count_of_backups = metadata.get("count_of_backups", "None")
        result.last_backup_date = metadata.get("last_backup_date", "None")
        result.supposed_backups_count = metadata.get("supposed_backups_count", "None")

        logging.info(f"Collect metadata from {s3_path} complete")
        return result

    def _get_backups_count(self, metadata: BackupMetadata) -> int:
        '''
            Return count of backups
        '''
        try:
            return int(metadata.count_of_backups)
        except ValueError as exc:
            # there are cases when total count string looks like "67 total / 10 full / 57 incremental"
            # so we need to parse it explicitly
            return int(metadata.count_of_backups.split(" ")[0])

    def _color_backup_count(self, metadata: BackupMetadata) -> str:
        '''
            Select color for Backup count cell
        '''
        if self._get_backups_count(metadata) < 3:
            return self.color_alarm
        return self.color_neutral

    def _color_supposed_backups_count(self, metadata: BackupMetadata) -> str:
        '''
            Select color for Supposed Backups Count
        '''
        if self._get_backups_count(metadata) <= int(metadata.supposed_backups_count) - 3:
            return self.color_alarm
        elif self._get_backups_count(metadata) <  int(metadata.supposed_backups_count) - 2:
            return self.color_warning
        return self.color_neutral

    def _color_last_backup_date(self, metadata: BackupMetadata) -> str:
        '''
            Select color for Last Backup Date
        '''
        last_backup_date = dateparser.parse(metadata.last_backup_date)
        time_delta = datetime.datetime.now() - last_backup_date.replace(tzinfo=None)
        if time_delta.days > 7:
            return self.color_alarm
        return self.color_neutral

    def _set_color_row(self, metadata: list) -> list:
        '''
            Compile color row by collected metadata for
        '''
        return [
            self.color_neutral, # Customer
            self.color_neutral, # DB type
            self.color_neutral, # Backup Placement
            self.color_neutral, # Size in MB
            self.color_neutral, # Backup time spent
            self.color_neutral, # Backup name
            self._color_backup_count(metadata), # Backup count
            self._color_supposed_backups_count(metadata), # Supposed Backups Count
            self._color_last_backup_date(metadata) , # Last Backup Date
            self.color_neutral, # Description
        ]


    def _compile_xlsx(self, metadata: list) -> str:
        logging.info(f"Compile xlsx file")
        wb = openpyxl.Workbook()
        sheet = wb.active
        sheet.title = self.worksheet_name
        sheet.append([
            "Customer",
            "DB type",
            "Backup Placement",
            "Size in MB",
            "Backup time spent",
            "Backup name",
            "Backups count",
            "Supposed Backups Count",
            "Last Backup Date",
            "Description"
        ])

        for row, data_row in enumerate(metadata):
            data_row_color = self._set_color_row(data_row)
            data_row = [
                data_row.customer,
                data_row.type,
                data_row.placement,
                data_row.size,
                data_row.time,
                data_row.backup_name,
                data_row.count_of_backups,
                data_row.supposed_backups_count,
                data_row.last_backup_date,
                data_row.description
            ]

            for col, data_col in enumerate(data_row):
                cell = sheet.cell(row=row+2, column=col+1)
                cell.value = data_col
                cell.fill = PatternFill(patternType='solid', fgColor=data_row_color[col])
        
        wb.save(self.spreadsheet_name + ".xlsx")
        wb.close()

        return self.spreadsheet_name + ".xlsx"

    def _upload_xlsx(self, xlsx_path: str) -> None:
        logging.info(f"Upload xlsx to google sheet")
        gauth = GoogleAuth()
        scope = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
        drive = GoogleDrive(gauth)

        file = drive.CreateFile({'title': self.spreadsheet_name + ".xlsx"})
        file.SetContentFile(xlsx_path)
        file.Upload({'convert': True})
        file.InsertPermission({
            'type': 'anyone',
            'value': 'anyone',
            'role': 'reader'})

        table_link = file['alternateLink']
        logging.info(f"Xlsx file uploaded and the table is available at the link - {table_link}")
        print(f"Xlsx file uploaded and the table is available at the link - {table_link}")

    def collect(self):
        metadata = []
        for bucket in self.buckets:
            metadata.append(
                self._collect_from_bucket(
                    aws_access_key_id=bucket.get("aws_access_key_id"),
                    aws_secret_access_key=bucket.get("aws_secret_access_key"),
                    aws_region=bucket.get("aws_region"),
                    s3_path=bucket.get("s3_path"),
                    aws_endpoint_url=bucket.get("aws_endpoint_url")
                )
            )

        xlsx = self._compile_xlsx(metadata)
        self._upload_xlsx(xlsx)
        os.remove(xlsx)
