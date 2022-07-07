"""
previous commit done on 2021-Feb-24 commit 197393b8700c7ed4f3db6b00f1e2de7b85a57f4e
"""
import airflow
import argparse
import boto3
import os
import re
import zipfile
import logging
import sys
import urllib
import urllib.parse 
import json
from dateutil import relativedelta
from datetime import date
from googleapiclient import discovery, http
from google.oauth2.credentials import Credentials
from hooks.secret_manager_hook import SecretManagerHook


# setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(processName)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


def access_token_from_refresh_token(refresh_token):

    creds = SecretManagerHook(secret_name='exo_google_earnings', region='us-east-1').get_secret()

    req = urllib.request.Request('https://accounts.google.com/o/oauth2/token',
        data=urllib.parse.urlencode({
            'grant_type'   : 'refresh_token',
            'client_id'    : creds["client_id"],
            'client_secret': creds["client_secret"],
            'refresh_token': refresh_token
        }).encode("utf-8"),
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept'      : 'application/json'
        }
    )
    '''
    html = ur.urlopen(url).read()
    print(type(html))
    data = json.loads(html.decode('utf-8'))
    '''
    dat=urllib.request.urlopen(req).read()
    response = json.loads(dat.decode('utf-8'))
    return response['access_token']


def get_object(service, bucket, filename, out_file):
    try:
        req = service.objects().get_media(bucket=bucket, object=filename)
        downloader = http.MediaIoBaseDownload(out_file, req)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info("Status: "+str(status)+", Download {}%.".format(int(status.progress() * 100)))
        return out_file
    except Exception as exc:
        logger.info(str(exc))


def main():
    # setup arguments
    parser = argparse.ArgumentParser(description="todo")
    parser.add_argument('--yearmonth', default=date.today().strftime('%Y%m'),
                        help="Year and month to process, YYYYMM or 'lastmonth'")
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--refresh_token', required=True)
    parser.add_argument('--file_folder', required=True)
    parser.add_argument('--month_back', required=True)
    args = parser.parse_args()
    month_back=int(args.month_back)
    if args.yearmonth == 'lastmonth':
        yearmonth = (date.today() + relativedelta.relativedelta(months=-month_back)).strftime('%Y%m')
    else:
        yearmonth = args.yearmonth


    if args.yearmonth == 'lastmonth':
        data_urls = {"earnings": r"earnings/earnings_"+yearmonth+r"_.*\.zip"}
    else:
        data_urls = {"sales": r"sales/salesreport_"+yearmonth+r"\.zip",
                 "stats_installs_country": r"stats/installs/installs_.*_"+yearmonth+r"_country.csv",
                 "stats_installs_device" : r"stats/installs/installs_.*_"+yearmonth+r"_device.csv"}

    working_dir = args.file_folder  
    #working_dir = tempfile.mkdtemp()
    os.chdir(working_dir)
    bucket = args.bucket
    access_token = access_token_from_refresh_token(args.refresh_token)
    logger.info("access token"+access_token)
    #--------------------------------------------------------------------------------------
    credentials = Credentials(access_token)
    #--------------------------------------------------------------------------------------
    service = discovery.build('storage', 'v1', credentials=credentials)
    req = service.buckets().get(bucket=bucket)
    logger.info("RES EXECUTED ***")
    res = req.execute()
    fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
    objlist = service.objects().list(bucket=bucket, fields=fields_to_return)
    s3_client = boto3.client('s3')
    logger.info("--------------------------------------------------------------------------------")
    logger.info("processing daily load for month "+yearmonth)

    while objlist:
        resp = objlist.execute()
        for table, pattern in data_urls.items():
            for obj in resp.get('items', []):
                if re.match(pattern, obj['name']):
                    req = service.objects().get_media(bucket=bucket, object=obj['name'])
                    if table == 'earnings':
                        sub_folder = "earnings"
                    elif table == 'sales':
                        sub_folder = "sales"
                    else:
                        sub_folder = "stats/installs"

                    logger.info("writing "+working_dir+'/'+sub_folder+'/'+os.path.basename(obj['name']))
                    filename = working_dir+'/'+sub_folder+'/'+os.path.basename(obj['name'])
                    fh = open(filename, 'w+b')
                    fh.write(req.execute())
                    fh.close()

                    # if not earnings or sales, convert from utf-16le to utf-8
                    if table != "sales" and table != "earnings":
                        out_f = filename+'.decoded'

                        with open(filename, 'r') as source_file:
                            with open(out_f, 'w') as dest_file:
                                contents = source_file.read()
                                contents.replace('\xef','')
                                dest_file.write(contents.decode('utf-16').encode('utf-8'))

                        with open(out_f, 'r') as source_file:
                            with open(filename, 'w') as dest_file:
                                contents = source_file.read()
                                dest_file.write(contents)

                        os.remove(out_f)

                    logger.info("will be uploading "+os.path.basename(obj['name']))
                    # unzip if necessary
                    if filename.endswith('.zip') is True:
                        zip_ref = zipfile.ZipFile(filename, 'r')
                        # This does not work since now GP sends multiple files per month with the same uncompressed name 
                        #leading to extractall overwriting the other files of the same month
                        #zip_ref.extractall(working_dir+'/'+sub_folder) 
                        archive_filenumber = filename.replace('.zip','').split('-')[-1]
                        
                        #Instead we will get the filnumber suffix of the archive and add it to uncompress filename
                        zipinfos = zip_ref.infolist()
                        for zipinfo in zipinfos:
                            extension = zipinfo.filename.split('.')[-1]
                            basename = zipinfo.filename.replace('.'+extension,'')
                            new_filename = basename+'_'+archive_filenumber+'.'+extension
                            zipinfo.filename = new_filename
                            zip_ref.extract(zipinfo,path=working_dir+'/'+sub_folder)

        objlist = service.objects().list_next(objlist, resp)

    #os.chdir('..')
    #shutil.rmtree(working_dir)


if __name__ == '__main__':
    main()