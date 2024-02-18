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

    arg_parser.add_argument("--google_spreadsheet_credenrials_path",
        type=str,
        default="",
        required='--collector' in sys.argv,
        help="""Path to credentials json to connect to google sheet"""
    )

    arg_parser.add_argument("--spreadsheet_name",
        type=ast.literal_eval,
        default=[],
        required='--collector' in sys.argv,
        action='append',
        help="""Spreadsheet name where backups info will store"""
    )

    arg_parser.add_argument("--worksheet_name",
        type=ast.literal_eval,
        default=[],
        required='--collector' in sys.argv,
        action='append',
        help="""Worksheet name where backups info will store"""
    )

    arguments = arg_parser.parse_known_args()[0]
    confs = set_confs(arguments) 

    logging.basicConfig(
        encoding='utf-8', 
        level=getattr(logging, confs["logging_level"]), 
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    if confs["collector"]:
        logging.info("Run collector")
        collector = BackupCollector(buckets = confs['bucket'],
            google_spreadsheet_credenrials_path = confs['google_spreadsheet_credenrials_path'],
            spreadsheet_name = confs['spreadsheet_name'],
            worksheet_name = confs['worksheet_name'])
        collector.collect()

    elif confs["docker_postgres"]:
        logging.info("Report about docker-postgress backups")
        reporter = DockerPostgresBackupReporter(
            aws_access_key_id = confs["bucket"][0]["aws_access_key_id"],
            aws_secret_access_key = confs["bucket"][0]["aws_secret_access_key"],
            aws_region = confs["bucket"][0]["aws_region"],
            s3_path = confs["bucket"][0]["s3_path"],
            container_name = confs["container_name"],
            customer = confs["customer"],
            supposed_backups_count = confs["supposed_backups_count"],
            aws_endpoint_url = confs["bucket"][0]["aws_endpoint_url"]
        )
        reporter.report()

    else:
        print("You MUST chose one reporter mode or collector mode")
        exit(1)

if __name__ == "__main__":
    start()