from datetime import datetime, timedelta
from ftplib import FTP

FTP_SITE = 'ftp.nasdaqtrader.com'


def get_latest_universe_of_stocks(s3, filename, bucket):
    # untested, pseudo code
    directory = 'symboldirectory'
    filename = "nasdaqlisted.txt"
    # get yesterday's date and format as YYYY/MM/DD
    s3_key = f"{datetime.now().strftime('%Y%m%d')}/universe"

    # download latest file from nasdaq
    with FTP(FTP_SITE) as ftp:
        ftp.login()
        ftp.cwd(directory)
        with open(filename, 'rb') as f:
            ftp.storbinary('STOR ' + s3_key, f)
    # upload the file to s3
    with open(filename, 'rb') as data:
        s3.upload_fileobj(data, bucket, key)
    