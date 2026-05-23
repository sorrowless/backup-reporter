# Backup Reporter

This repository contains source code for backup reporter tool. That tool can
collect backup information, upload it to S3 buckets or host, then collect bunch of
backup information files, get them together into one csv file and upload it to
google spreadsheet. Can save information to `json` and `prom` format.

Backup reporter has two working modes: reporter and collector.

## Installation

To install backup-reporter to some machine (either in reporter or collector
mode), ensure you have a python 3.8+ installed on that machine. If so, simply
run `pip3 install backup-reporter` and wait to the end of setup process. After
installation will be completed, run `backup-reporter -h` to get further steps
help.

### Installation as user

Beware that standard python packages installations which are ran by mean user,
won't install console scripts to the PATH, so ensure to do so manually - or run
installation as root.

## Configuration

### Reporter

Reporter can be configured with two ways: script arguments or configuration
file. Possible configuration options you can find by typing `backup-reporter
-h`. To use config file just pass `--config your_config_file.yml` as script
argument.
All options from cli-help are same for config-file. As example following
command:

- `python3 main.py --destination="{'type': 's3'}" --bucket="{'s3_path':
  's3://bucket_name/in_bucket_path/metadata_file_name.txt',
  'aws_access_key_id': 'key', 'aws_secret_access_key': 'key', 'aws_region':
  'region'}" --docker_postgres`

can be written in file:

```yml
docker_postgres: true
bucket:
    - s3_path: s3://bucket_name/in_bucket_path/metadata_file_name.txt
      aws_access_key_id: key
      aws_secret_access_key: key
      aws_endpoint_url: url
      aws_region: region
      customer: "Customer name"
```

More examples can be found at `docs/config-examples/reporter-*.conf`

### Collector

Collector can be configured the same way as reporter - with arguments passed to
executable file or with config file (which, though, has to be passed as
argument too). Example of config for collector with comments:

```yml
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

## Owner transfership at Google Drive

### Spreadsheet Ownership Transfer

Transferring ownership of a Google Spreadsheet is a two-step process:

1. **Initiation**
   The collector marks the spreadsheet with a flag indicating that ownership needs to be transferred.

2. **Acceptance**
   The intended new owner must manually accept ownership by:
   - Opening [Google Drive](https://drive.google.com)
   - Searching for `pendingowner:me`
   - Locating the corresponding spreadsheet
   - Accepting the ownership transfer

> **More information:**
> [Google Docs Help: Transfer ownership of a file](https://support.google.com/docs/answer/2494892?hl=en&co=GENIE.Platform%3DDesktop)

---

### Alternative: Share Spreadsheet with Service Account (No Ownership Transfer)

Instead of transferring ownership, you can create the spreadsheet manually and **share it with the service account**.

- **Note:** Ownership remains with you — the service account is only granted access.
- **Required permissions:** `Editor`
- **Service account email:** Found in the JSON credentials file referenced by the
  `google_spreadsheet_credentials_path` configuration option.

This method allows the service account to read and update the spreadsheet content,
but not to manage sharing settings or transfer ownership.

## Development

Install dependencies for local development:

```bash
make prepare
poetry install   # or: . .venv/bin/activate && poetry install
```

Run the tool via `poetry run backup-reporter` (or activate `.venv` first).

### Releasing to PyPI

Release flow (local macOS or CI on Ubuntu):

```bash
make prepare
make build          # patch bump, commit, tag, build dist/
make publish        # requires PYPI_API_TOKEN
make push-release   # push commit and tags to origin
```

Requirements:

- Clean git working tree (no uncommitted changes).
- At least one new commit since the latest release tag.
- Always run `make push-release` after a successful publish so git tags stay in sync with PyPI.

Set credentials locally:

```bash
export PYPI_API_TOKEN=pypi-...
```

CI uses GitHub Actions workflow [`.github/workflows/release.yml`](.github/workflows/release.yml) (`workflow_dispatch`) with repository secret `PYPI_API_TOKEN`.

If PyPI is ahead of git tags (e.g. tag was not pushed), the next `make build` creates a recovery release with the next patch version. Use `RELEASE_STRICT=1` to fail instead of auto-recovery.

## Authors

Made in cooperation with:

* Dmitry Razin - https://github.com/one-mINd
* Stan Bogatkin - https://github.com/sorrowless
