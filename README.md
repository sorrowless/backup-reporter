# Backup Reporter

This repository contains source code for backup reporter tool. That tool can collect backup information, upload it to S3 buckets, than collect bunch of backup information files, get them together into one csv file and upload it to google spreadsheet.

Backup reporter has two working modes: reporter and collector.

## Configuration
Reporter can be configured with two ways: script arguments or configuration file. Possible configuration options you can find by typing `main.py -h`. To use config file just pass `--config your_config_file.yml` as script argument.
All options from cli-help are same for config-file. As example following command: 
- `python3 main.py --bucket="{'s3_path': 's3://bucket_name/in_bucket_path/metadata_file_name.txt', 'aws_access_key_id': 'key', 'aws_secret_access_key': 'key', 'aws_region': 'region'}" --docker_postgres` 

can be written in file:
```
docker_postgres: true
bucket:
    - s3_path: s3://bucket_name/in_bucket_path/metadata_file_name.txt
      aws_access_key_id: key
      aws_secret_access_key: key
      aws_region: region
```
