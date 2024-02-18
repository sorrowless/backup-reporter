# Backup Reporter

This repository contains source code for backup reporter tool. That tool can collect backup information, upload it to S3 buckets, then collect bunch of backup information files, get them together into one csv file and upload it to google spreadsheet.

Backup reporter has two working modes: reporter and collector.

## Installation

To install backup-reporter to some machine (either in reporter or collector mode), ensure you have a python 3.8+ installed on that machine. If so, simply run `pip3 install backup-reporter` and wait to the end of setup process. After installation will be completed, run `backup-reporter -h` to get further steps help.

### Installation as user

Beware that standard python packages installations which are ran by mean user, won't install console scripts to the PATH, so ensure to do so manually - or run installation as root.

## Configuration

### Reporter

Reporter can be configured with two ways: script arguments or configuration file. Possible configuration options you can find by typing `backup-reporter -h`. To use config file just pass `--config your_config_file.yml` as script argument.
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
      customer: "Customer name"
```

More examples can be found at `docs/config-examples/reporter-*.conf`

### Collector

Collector can be configured the same way as reporter - with arguments passed to executable file or with config file (which, though, has to be passed as argument too). Example of config for collector with comments:

```
# Sheet owner is an email of user to whom ownership will be transfered
sheet_owner: s@example.com

# Credentials file is a JSON key which should be given to some service account. 
# To understand how to create service account, try to google about a bit
google_spreadsheet_credentials_path: ~/Development/personal/backupreporter_key.json

# This is a name for a target spreadsheet
spreadsheet_name: "Backup-Reports"

# Sheet name in a spreadsheet
worksheet_name: Customers

bucket:
    - s3_path: s3://bucket/metadata/metadata.json
      aws_access_key_id: access-key
      aws_secret_access_key: secret-key
      aws_region: ru-1
      aws_endpoint_url: https://s3.ru-1.storage.selcloud.ru
      customer: Personal
```

### Owner transfership at Google Drive

Owner transfership in case of spreadsheets is a two-step process. First, collector will set flag on spreadsheet which will indicate that permission have to be transferred. After that user which should became an owner has to accept that by write `pendingowner:me` in his google drive, find according spreadsheet and get the ownership. One can read some details at https://support.google.com/docs/answer/2494892?hl=en&co=GENIE.Platform%3DDesktop.

## Development

Install poetry first, then simply run `poetry install` in repository root - and start to develop.