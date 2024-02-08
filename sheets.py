#!/usr/bin/env python3
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import json
import os

project_name = os.environ.get("PROJECT_NAME", "test_project")
# clietn_name = os.environ.get("CLIENT_NAME")
spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "Backup_sheet")
sheet_name = os.environ.get("SHEET_NAME", "Test_sheet")
print(sheet_name)
# Replace these variables with your own values
json_file_path = './credantionals.json'
# spreadsheet_name = 'Backup_sheet'
json_data_reports_file = './backup_report.json'
json_data_validate_file = './validate_report.json'


# Authenticate with Google Sheets API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json_file_path, scope)
client = gspread.authorize(creds)

# Open the Google Sheets spreadsheet
spreadsheet = client.open(spreadsheet_name)

# Open the first sheet in the spreadsheet
sheet = spreadsheet.worksheet(sheet_name)

# Read data from JSON file
with open(json_data_reports_file, 'r') as json_file:
    data_reports = json.load(json_file)

with open(json_data_validate_file, 'r') as json_file:
    data_validation = json.load(json_file)

full_update_data = [project_name, datetime.now().strftime("%d_%m_%Y-%H_%M_%S")] + list(data_reports.values()) + list(data_validation.values())

target_cell = sheet.find(data_reports["project_name"], in_column=1)
print(target_cell)

if target_cell != None:
    print("No")
    sheet.update(("A" + str(target_cell.row)), [full_update_data])
else:
    sheet.append_row(full_update_data)

print("Data written to Google Sheets successfully.")
