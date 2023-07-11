import os
import pandas as pd
import pymysql
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import io
import creds
from dataclasses import dataclass

@dataclass
class Tiso:
    file_path: str = os.path.join(os.getcwd(), "UPLOAD REKAP TIKET SOBEK MIKROTRANS (Responses).xlsx")
    def download_tiso(self):
        print('Downloading Tiso File...')
        # Check if the file exists
        if os.path.exists(self.file_path):
            # Delete the file
            os.remove(self.file_path)
        file_id = '1CBhKirWul7AkxUqm3iC8pVBAVe2MgPgBqKwTpbHW1Lk'

        cred_path = os.path.join(os.getcwd(), 'tiso-1-d59232a5a19e.json')
        credentials = service_account.Credentials.from_service_account_file(cred_path)
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

    def import_database(self):
        print('Importing to Database...')
        # Establish MySQL database connection
        db_connection = pymysql.connect(
            host=creds.host,
            database=creds.db,
            user=creds.user,
            password=creds.password
        )

        # Read Excel file into a DataFrame
        data = pd.read_excel(self.file_path)

        # Replace nan with None
        data = data.fillna('')

        # Specify MySQL table name
        table_name = 'trx_tiso'

        # Open a cursor to perform database operations
        cursor = db_connection.cursor()

        #truncate_table
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_sql)

        # Iterate over each row in the DataFrame
        for _, row in data.iterrows():
            # Extract data from the row
            col1_value = row.iloc[0]
            col2_value = row.iloc[1]
            col3_value = row.iloc[2]
            col4_value = row.iloc[3]
            if row.iloc[8] != '':
                col5_value = row.iloc[8]
            else:
                col5_value = 0

            # Create SQL INSERT statement
            sql = f"INSERT INTO {table_name} (timestamp, nik_petugas, nama_petugas, rute, total) VALUES (%s, %s, %s, %s, %s)"
            values = (col1_value, col2_value, col3_value, col4_value, col5_value)

            # Execute the SQL statement
            cursor.execute(sql, values)

        # Commit the changes to the database
        db_connection.commit()

        # Close the cursor and database connection
        cursor.close()
        db_connection.close()

    def main(self):
        self.download_tiso()
        self.import_database()
        print('Update Database Tiso Completed')

if __name__ == '__main__':
    tiso = Tiso()
    tiso.main()