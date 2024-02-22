import argparse
import sys
import logging
import ast

from backup_reporter.reporters import DockerPostgresBackupReporter
from backup_reporter.collector import BackupCollector
from backup_reporter.utils import set_confs


def start():
    arg_parser = argparse.ArgumentParser()
    execution_mode = arg_parser.add_mutually_exclusive_group()

    arg_parser.add_argument("--config",
        type=str,
        default="",
        help="Set config path"
    )

    is_config_set = any('--config' in arg for arg in sys.argv)

    arg_parser.add_argument("--logging_level",
        type=str,
        default="INFO",
        help="Python loggin facility level. Possible values: NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL"
    )

    arg_parser.add_argument("--bucket",
        type=ast.literal_eval,
        default=[],
        required=not is_config_set,
        action='append',
        help="""The bucket from which metadata will be taken.
        You can pass that any --bucket arguments.
        Format MUST be like following: --bucket={'s3_path': 's3://bucket_name/in_bucket_path/metadata_file_name.txt', 'aws_access_key_id': 'key', 'aws_secret_access_key': 'key', 'aws_region': 'region'}"""
    )

    arg_parser.add_argument("--customer",
        type=str,
        default="",
        required=not is_config_set,
        help="Backup customer"
    )

    arg_parser.add_argument("--supposed_backups_count",
        type=str,
        default="",
        required=not is_config_set,
        help="Supposed backups count"
    )

    ### Reported specific args

    execution_mode.add_argument("--docker_postgres",
        default=False,
        action='store_true',
        help="Setup reporter for dockerized postgres"
    )

    arg_parser.add_argument("--container_name",
        type=str,
        default="",
        required=not is_config_set and '--docker_postgres' in sys.argv,
        help="Set container name for databases inside docker containers"
    )

    ### Collector mode args

    execution_mode.add_argument("--collector",
        default=False,
        action='store_true',
        help="Setup collector"
    )

    arg_parser.add_argument("--google_spreadsheet_credentials_path",
        type=str,
        default="",
        required=not is_config_set and '--collector' in sys.argv,
        help="""Path to credentials json to connect to google sheet"""
    )

    arg_parser.add_argument("--spreadsheet_name",
        type=str,
        default="",
        required=not is_config_set and '--collector' in sys.argv,
        help="""Spreadsheet name where backups info will store"""
    )

    arg_parser.add_argument("--worksheet_name",
        type=str,
        default="",
        required=not is_config_set and '--collector' in sys.argv,
        help="""Worksheet name where backups info will store"""
    )

    arg_parser.add_argument("--sheet_owner",
        type=str,
        default="",
        required=not is_config_set and '--collector' in sys.argv,
        help="""Spreadsheet owner to share with"""
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

    else:
        print("You MUST choose either reporter mode or collector mode")
        exit(1)

if __name__ == "__main__":
    start()
