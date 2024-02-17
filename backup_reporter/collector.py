import os
import csv
import json
import boto3
import gspread
import logging

from backup_reporter.dataclass import BackupMetadata
from oauth2client.service_account import ServiceAccountCredentials


class BackupCollector:
    def __init__(self, buckets: list,
            google_spreadsheet_credenrials_path: str,
            spreadsheet_name: str,
            worksheet_name: str) -> None:
        self.buckets = buckets
        self.credenrials_path = google_spreadsheet_credenrials_path
        self.spreadsheet_name = spreadsheet_name
        self.worksheet_name = worksheet_name

    def _collect_from_bucket(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_region: str,
            s3_path: str) -> BackupMetadata:

        s3 = boto3.resource('s3',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = aws_region)

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
        self._csv_write([[ "Customer", "DB type", "Backup Placement", "Size in MB", "Backup time spent", "Backup name", "Backups count", "Supposed Backups Count", "Last Backup Date" ]], csv_path)

        backups_info = []
        for data in metadata:
            row = [ data.customer, data.type, data.placement, data.size, data.time, data.backup_name, data.count_of_backups, data.supposed_backups_count, data.last_backup_date ]
            backups_info.append(row)
        
        self._csv_write(backups_info, csv_path)

        return csv_path

    def _upload_csv(self, csv_path: str) -> None:
        logging.info(f"Upload csv to google sheet")
        scope = ["https://spreadsheets.google.com/feeds", 
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credenrials_path, scope)
        client = gspread.authorize(credentials)
        spreadsheet = client.open(self.spreadsheet_name)

        try:
            spreadsheet.add_worksheet(title=self.worksheet_name, rows="100", cols="20")
        except:
            pass
        
        spreadsheet.values_clear(self.worksheet_name + "!A1:L10000")
        spreadsheet.values_update(
            self.worksheet_name,
            params={'valueInputOption': 'USER_ENTERED'},
            body={'values': list(csv.reader(open(csv_path)))}
        )

    def collect(self):
        metadata = []
        for bucket in self.buckets:
            metadata.append(self._collect_from_bucket(
                aws_access_key_id=bucket.get("aws_access_key_id"),
                aws_secret_access_key=bucket.get("aws_secret_access_key"),
                aws_region=bucket.get("aws_region"),
                s3_path=bucket.get("s3_path")
            ))

        csv = self._compile_csv(metadata)
        self._upload_csv(csv)
        os.remove(csv)
