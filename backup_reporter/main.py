import argparse
import sys
import logging
import ast

from backup_reporter.reporters import *
from backup_reporter.collector import BackupCollector
from backup_reporter.utils import set_confs


def start():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--config",
        type=str,
        default="",
        help="Set config path"
    )

    arguments = arg_parser.parse_known_args()[0]
    confs = set_confs(arguments)

    logging.basicConfig(
        level=getattr(logging, confs.get("logging_level", None)),
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if confs["collector"]:
        logging.info("Run collector")
        collector = BackupCollector(buckets = confs.get('bucket', None),
            google_spreadsheet_credentials_path = confs.get('google_spreadsheet_credentials_path', None),
            spreadsheet_name = confs.get('spreadsheet_name', None),
            worksheet_name = confs.get('worksheet_name', None),
            sheet_owner = confs.get('sheet_owner', None))
        collector.collect()

    elif confs["docker_postgres"]:
        logging.info("Report about docker-postgres backups")
        reporter = DockerPostgresBackupReporter(
            aws_access_key_id = confs["bucket"][0].get("aws_access_key_id", None),
            aws_secret_access_key = confs["bucket"][0].get("aws_secret_access_key", None),
            aws_region = confs["bucket"][0].get("aws_region", None),
            s3_path = confs["bucket"][0].get("s3_path", None),
            container_name = confs.get("container_name", None),
            customer = confs.get("customer", None),
            supposed_backups_count = confs.get("supposed_backups_count", None),
            aws_endpoint_url = confs["bucket"][0].get("aws_endpoint_url", None),
            description = confs.get("description", None)
        )
        reporter.report()

    elif confs["files_bucket"]:
        logging.info("Report about docker-postgres backups")
        reporter = FilesBucketReporterBackupReporter(
            aws_access_key_id = confs["bucket"][0].get("aws_access_key_id", None),
            aws_secret_access_key = confs["bucket"][0].get("aws_secret_access_key", None),
            aws_region = confs["bucket"][0].get("aws_region", None),
            s3_path = confs["bucket"][0].get("s3_path", None),
            customer = confs.get("customer", None),
            supposed_backups_count = confs.get("supposed_backups_count", None),
            aws_endpoint_url = confs["bucket"][0].get("aws_endpoint_url", None),
            description = confs.get("description", None),
            files_mask = confs.get("files_mask", None)
        )
        reporter.report()

    else:
        print("You MUST choose either reporter mode or collector mode")
        exit(1)

if __name__ == "__main__":
    start()
