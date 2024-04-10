import os
import csv
import json
import boto3
import gspread
import logging
import datetime
from time import sleep
from gspread_formatting import Color, CellFormat, format_cell_range
from oauth2client.service_account import ServiceAccountCredentials

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

    def _csv_write(self, data: list, csv_path: str) -> None:
        with open(csv_path, 'a') as csvfile:
            csv_file = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in data:
                csv_file.writerow(row)

    def _compile_csv(self, metadata: list) -> str:
        logging.info(f"Compile csv file")
        csv_path = "tmp_report.csv"
        self._csv_write([[ "Customer", "DB type", "Backup Placement", "Size in MB", "Backup time spent", "Backup name", "Backups count", "Supposed Backups Count", "Last Backup Date", "Description" ]], csv_path)

        backups_info = []
        for data in metadata:
            row = [ data.customer, data.type, data.placement, data.size, data.time, data.backup_name, data.count_of_backups, data.supposed_backups_count, data.last_backup_date, data.description ]
            backups_info.append(row)
        
        self._csv_write(backups_info, csv_path)

        return csv_path

    def _upload_csv(self, csv_path: str) -> None:
        logging.info(f"Upload csv to google sheet")
        scope = ["https://spreadsheets.google.com/feeds", 
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
        client = gspread.authorize(credentials)
        try:
            spreadsheet = client.open(self.spreadsheet_name)
            logging.debug("List current permissions")
            permissions = spreadsheet.list_permissions()
            logging.debug(permissions)
            for user in permissions:
                if user.get("emailAddress", None) == self.sheet_owner and user["role"] != "owner":
                    logging.info(f"Change owner to {self.sheet_owner}")
                    spreadsheet.transfer_ownership(user["id"])
                    break
        except gspread.exceptions.SpreadsheetNotFound as e:
            spreadsheet = client.create(self.spreadsheet_name)
            spreadsheet.share(self.sheet_owner, perm_type='user', role='writer')

        try:
            logging.debug(f"Worksheets are: {spreadsheet.worksheets()}")
            spreadsheet.worksheet(self.worksheet_name)
        except gspread.exceptions.WorksheetNotFound as e:
            spreadsheet.add_worksheet(title=self.worksheet_name, rows="100", cols="20")
        
        spreadsheet.values_clear(self.worksheet_name + "!A1:L10000")
        spreadsheet.values_update(
            self.worksheet_name,
            params={'valueInputOption': 'USER_ENTERED'},
            body={'values': list(csv.reader(open(csv_path)))}
        )

    def _color_backup_count(self, metadata: BackupMetadata) -> Color:
        if int(metadata.count_of_backups) < 3:
            return Color(1, 0, 0)
        return Color(1, 1, 1)

    def _color_supposed_backups_count(self, metadata: BackupMetadata) -> Color:
        if int(metadata.count_of_backups) <= int(metadata.supposed_backups_count) - 3:
            return Color(1, 0, 0)
        elif int(metadata.count_of_backups) <  int(metadata.supposed_backups_count) - 2:
            return Color(1, 0.5, 0)
        return Color(1, 1, 1)

    def _color_last_backup_date(self, metadata: BackupMetadata) -> Color:
        last_backup_date = datetime.datetime.strptime(metadata.last_backup_date, '%Y-%m-%d %H:%M:%S%z')
        time_delta = datetime.datetime.now() - last_backup_date.replace(tzinfo=None)
        if time_delta.days > 7:
            return Color(1, 0, 0)
        return Color(1, 1, 1)

    def _set_color_matrix(self, metadata: list) -> list:
        result = [[Color(1, 1, 1), Color(1, 1, 1), Color(1, 1, 1), Color(1, 1, 1), Color(1, 1, 1)]]
        for data in metadata:
            result.append([
                Color(1, 1, 1), # Customer
                Color(1, 1, 1), # DB type
                Color(1, 1, 1), # Backup Placement
                Color(1, 1, 1), # Size in MB
                Color(1, 1, 1), # Backup time spent
                Color(1, 1, 1), # Backup name
                self._color_backup_count(data), # Backup count
                self._color_supposed_backups_count(data), # Supposed Backups Count
                self._color_last_backup_date(data) , # Last Backup Date
                Color(1, 1, 1), # Description
            ])

        return result
    
    def _get_column_name(self, n):
        result = ''
        while n > 0:
            index = (n - 1) % 26
            result += chr(index + ord('A'))
            n = (n - 1) // 26

        return result[::-1]
    
    def _colorize_spreadsheet(self, color_matrix: list) -> None:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
        spreadsheet = gspread.authorize(credentials).open(self.spreadsheet_name)
        worksheet = spreadsheet.worksheet(self.worksheet_name)
        
        format_cell_range(
            worksheet=worksheet, 
            name="0",
            cell_format=CellFormat(backgroundColor=Color(1, 1, 1))
        )

        for y, row in enumerate(color_matrix):
            sleep(5)
            for x, col in enumerate(row):
                format_cell_range(
                    worksheet=worksheet, 
                    name=self._get_column_name(x+1)+str(y+1),
                    cell_format=CellFormat(backgroundColor=col)
                )

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

        csv = self._compile_csv(metadata)
        self._upload_csv(csv)
        os.remove(csv)

        color_matrix = self._set_color_matrix(metadata)
        self._colorize_spreadsheet(color_matrix)
