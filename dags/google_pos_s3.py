#!/usr/bin/env python

import argparse
import glob
import time
import boto3
import os
import re
import zipfile
import tempfile
import shutil
import logging
import sys

from datetime import date
from googleapiclient import discovery, http
from oauth2client.client import GoogleCredentials
from dateutil import relativedelta
from googleapiclient.discovery import build
from oauth2client.client import AccessTokenCredentials
from urllib import urlencode
from urllib2 import Request , urlopen, HTTPError
import json


def setup_logger(name):
    # setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # log to stdout so that airflow can capture it
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    #logformatter = logging.Formatter('%(asctime)s:%(processName)s:%(message)s')
    #loghandler = TimedRotatingFileHandler(name, when='d', interval=1, backupCount=5)
    #loghandler.setFormatter(logformatter)
    #logger.addHandler(loghandler)

    return logger

def access_token_from_refresh_token(client_id, client_secret, refresh_token):

    # these are the new parameters for Matt Howell account
    #client_id = "32555940559.apps.googleusercontent.com"
    #client_secret = "ZmssLNjJy2998hD4CTg2ejr2"
    #refresh_token = "1/CLoD2BXcyZLO6cMxe82GSg1HixzZQvw4kdlE1XAGN1sGY62Rj5kwDGWVw0VRrrpx"

    request = Request('https://accounts.google.com/o/oauth2/token',
        data=urlencode({
            'grant_type': 'refresh_token',
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token
        }),
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
    )
    response = json.load(urlopen(request))
    return response['access_token']

def get_object(service, bucket, filename, out_file):
    try:
        req = service.objects().get_media(bucket=bucket, object=filename)

        downloader = http.MediaIoBaseDownload(out_file, req)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print "Status: "+str(status)+", Download {}%.".format(int(status.progress() * 100))

        return out_file
    except Exception as exc:
        print str(exc)


def main():

    s3_bucket = 'analyst-jean-michel'
    s3_prefix = 'google'

    # setup arguments
    parser = argparse.ArgumentParser(description="todo")
    parser.add_argument('--yearmonth', default=date.today().strftime('%Y%m'),
                        help="Year and month to process, YYYYMM or 'lastmonth'")
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--client_id', required=True)
    parser.add_argument('--client_secret', required=True)
    parser.add_argument('--refresh_token', required=True)
    parser.add_argument('--file_folder', required=True)
    args = parser.parse_args()

    if args.yearmonth == 'lastmonth':
        yearmonth = (date.today() + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
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

    logger = setup_logger('remove')

    bucket = args.bucket
    access_token = access_token_from_refresh_token(args.client_id, args.client_secret, args.refresh_token)
    logger.info("access token"+access_token)
    credentials = AccessTokenCredentials(access_token, "MyAgent/1.0", None)
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
                        zip_ref.extractall(working_dir+'/'+sub_folder)
                    #    for zfile in zip_ref.namelist():
                    #        # send to s3
                    #        logger.info("uploading "+zfile)
                    #        s3_client.upload_file(zfile, s3_bucket, s3_prefix+'/google_pos/'+bucket+'/'+yearmonth+'/'+obj['name'])
                    #    continue

                    # send to s3
                    #logger.info("uploading "+os.path.basename(obj['name']))
                    #s3_client.upload_file(os.path.basename(obj['name']), s3_bucket, s3_prefix+'/google_pos/'+bucket+'/'+yearmonth+'/'+obj['name'])

        objlist = service.objects().list_next(objlist, resp)

    #os.chdir('..')
    #shutil.rmtree(working_dir)

if __name__ == '__main__':
    main()
