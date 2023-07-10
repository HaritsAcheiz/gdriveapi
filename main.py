import os
import pymysql
from googleapiclient.discovery import build
from google.oauth2 import service_account
import csv
import tabulate
from googleapiclient.http import MediaIoBaseDownload
import io

file_id = '1CBhKirWul7AkxUqm3iC8pVBAVe2MgPgBqKwTpbHW1Lk'

credentials = service_account.Credentials.from_service_account_file('D:/Naru/gdriveapi/tiso-1-d59232a5a19e.json')
drive_service = build('drive', 'v3', credentials=credentials)

results = drive_service.files().list(pageSize=5, fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
items = results.get('files', [])
if items:
    file_id = items[0]['id']
    file_name = items[0]['name']
    request = drive_service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    fh = io.FileIO(file_name + '.xlsx', mode='wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print('Download %d%%.' % int(status.progress() * 100))