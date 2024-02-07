#!/usr/bin/env python3
import json
import os
import subprocess
from datetime import datetime
import boto3
from datetime import datetime
import sys

def exec_cmd(args: list):
    # Exec input comand
    out = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    if out.returncode != 0:
        stdout_msg = stdout.decode('utf-8') if stdout is not None else ''
        stderr_msg = stderr.decode('utf-8') if stderr is not None else ''
        print(out.returncode)
        raise Exception(f"Command returned code {out.returncode}. Stdout: '{stdout_msg}' Stderr: '{stderr_msg}'")

    return stdout.decode("utf-8")


def create_resulting_dict(json_output):
    list_backups=json.loads(json_output)
    output_dict = {
    "backup_name": list_backups[0]['backups'][-1]['backup_name'],
    "last_backup_date": list_backups[0]['backups'][-1]['time'],
    "size": list_backups[0]['backups'][-1]['compressed_size'],
    "count_of_backups": len(list_backups[0]),
    "time": str(datetime.strptime(list_backups[0]['backups'][-1]['finish_time'], list_backups[0]['backups'][-1]['date_fmt']) - datetime.strptime(list_backups[0]['backups'][-1]['start_time'], list_backups[0]['backups'][-1]['date_fmt']))
    }
    return output_dict


def write_dict_to_file(resulting_dict):
    output_json = json.dumps(resulting_dict)
    print(output_json)
    with open('temporary.json', 'w') as file:
        # Write the variable's content to the file
        file.write(output_json)


def define_vars_for_s3_client(string_output):
    # Define vars for s3 client
    for var in string_output.split('\n'):
        if 'AWS_ACCESS_KEY_ID' in var:
            global AWS_ACCESS_KEY_ID
            AWS_ACCESS_KEY_ID = var.split('=')[1].strip('\r')
        if 'AWS_SECRET_ACCESS_KEY' in var:
            global AWS_SECRET_ACCESS_KEY
            AWS_SECRET_ACCESS_KEY = var.split('=')[1].strip('\r')
        if 'AWS_REGION' in var:
            global AWS_REGION
            AWS_REGION = var.split('=')[1].strip('\r')
        if 'WALG_S3_PREFIX' in var:
            global WALG_S3_PREFIX
            WALG_S3_PREFIX = var.split('=')[1].strip('\r')


def split_bucket_and_prefix(s3_full_path:str):
    # Split bucket name and prefix
    striped_name = s3_full_path.replace("s3://", "")
    if striped_name.find('/') > 1:
        bucket = (striped_name[:striped_name.find('/')])
        prefix = (striped_name[striped_name.find('/') + 1:])
    else:
        bucket = striped_name
        prefix = ''
    return bucket,prefix


def list_objects_in_bucket(bucket_name, directory_path):
    # Create an S3 client
    s3 = boto3.client('s3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,)

    try:
        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)

        # Print object details
        for obj in response.get('Contents', []):
            print(f"Object Key: {obj['Key']}, Last Modified: {obj['LastModified']}, Size: {obj['Size']} bytes")

    except Exception as e:
        print(f"Error listing objects in S3 bucket: {e}")
        raise


def put_objects_in_bucket(file_path, bucket_name, directory_path):
    # Create an S3 client
    s3 = boto3.client('s3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION)
    try:
        # Upload the file
        s3.upload_file(file_path, bucket_name, directory_path,)
        print(f"File uploaded to S3 bucket: {bucket_name}/{directory_path}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise


def define_s3_target_path(prefix):
    if len(prefix) > 0 and prefix[-1] != '/':
        directory_path = prefix + '/' + 'test_metadata'
    else:
        directory_path = 'test_metadata'
    file_name = 'test-json-output-' + datetime.now().strftime("%d_%m_%Y-%H_%M_%S")
    target_path = directory_path + '/' + file_name
    return target_path


if __name__ == "__main__":
    container_name = sys.argv[1]
    result_walg_cmd = exec_cmd(["docker", "exec", "-it", container_name, "wal-g", "wal-show", "--detailed-json" ])
    resulting_dict_for_json = create_resulting_dict(result_walg_cmd)
    write_dict_to_file(resulting_dict_for_json)

    result_env_cmd = exec_cmd(["docker", "exec", "-it", container_name, "env" ])
    define_vars_for_s3_client(result_env_cmd)

    bucket_name,prefix_name = split_bucket_and_prefix(WALG_S3_PREFIX)
    s3_target_path = define_s3_target_path(prefix_name)
    print(s3_target_path)
    put_objects_in_bucket('./temporary.json', bucket_name, s3_target_path)
    os.remove('./temporary.json')
