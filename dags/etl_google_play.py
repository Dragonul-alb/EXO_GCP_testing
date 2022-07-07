"""
Last version commit 337fd551e05e5b8886bc0bacfe93b80867d9f947
"""
import datetime
import requests
import json
import logging
import logging.handlers
import os
import sys
import shutil
import hashlib
import csv
import zipfile
import argparse
import io
import pandas as pd
import boto
from boto.s3.key import Key
from airflow.hooks.S3_hook import S3Hook
from hooks.secret_manager_hook import SecretManagerHook
from google.cloud import storage
from google.oauth2 import service_account


######  Initialize Logger  ######

# Configure output level
logger = logging.getLogger('GoogleLogger')
logger.setLevel(logging.INFO)

### NOTE: Device install code is commented out in case of desire/request for it in the future
# Decision not to include was based on load of rows/usefulness

class GooglePlay(object):

    def __init__(self, config_ini, aws_creds, google_auth):
        config = json.loads(config_ini)

        self._local_path = config["local_path"]
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


        # Google play details
        self._google_auth = google_auth
        # Buckets
        self._google_buckets = [config["google_buckets"]["wbie"],config["google_buckets"]["playdemic"]]
        # Bucket descriptions (wbie v playdemic)
        self._google_buckets_desc = [config["google_buckets_name"]["wbie"],config["google_buckets_name"]["playdemic"]]

        self._sales_date_file = config["sales_date_file"]
        self._installs_date_file = config["installs_date_file"]
        self._check_file = config["check_file"]
        self._run_date = str(datetime.datetime.now())[:10]

        self._request_dates = []

        self._load = config["load"]

        self._load_format = config["date_format"]
        # Depending on date format start and end dates differ
        if self._load_format == 'Past':
            self._backfill = config["backfill"]

            month = datetime.datetime.now().month
            year = datetime.datetime.now().year
            # Dates in months
            self._start_date = year*12 + month - int(self._backfill) + 1
            self._end_date = year*12 + month + 1
        elif self._load_format == 'Range':
            strt = datetime.datetime.strptime(config["start_date"], "%Y-%m")
            end = datetime.datetime.strptime(config["end_date"], "%Y-%m")

            self._start_date = strt.year*12 + strt.month
            self._end_date = end.year*12 + end.month + 1
        else:
            logger.error('Unknown date format')

        # Note no such thing as time-delta month, must manually generate list of year months
        # Do so by calculating out all months and then mod 12 and take remainder (unless 0 and account for december)
        # Produced start/end month counts above, and now will enter years-months into list
        for x in range(self._start_date, self._end_date):
            if x % 12 == 0:
                month = 12
                year = (x//12) - 1
            else:
                month = x % 12
                year = x // 12

            self._request_dates.append(str(year).zfill(4) + str(month).zfill(2))

        # S3 Details
        self._s3_access_key = aws_creds["access_key_id"]
        self._s3_secret_key = aws_creds["secret_access_key"]
        self._s3_bucket = config["s3_bucket"]
        self._s3_folder_root = config["s3_folder_root"]

        self._pdkey = config["pagerduty_service"]
        self._pdtoken = config["pagerduty_token"]

        # Read in redshift column mappings
        self._sales_columns = config["sales_columns"]
        #self._installs_device_columns = parser.get('redshift', 'installs device columns')
        self._installs_country_columns = config["installs_country_columns"]

        # Redshift Connection Details
        self._sales_table = config["sales_schema_and_table"]
        self._installs_country_table = config["installs_country_schema_and_table"]
        #self._installs_device_table = parser.get(ENVIRONMENT, 'installs device schema and table')

        # Local file directory
        self._dir0 = config["file_directory"]
        self.build_tmp_dir()

        # Variables for general refernece
        # Local raw files
        self._sales_files = []
        self._installs_country_files = []
        #self._installs_device_files = []

        # Date + desc + rep to date CSV
        self._fileCatalog = {}
        # Local date set
        self._sales_date_set = set()
        self._installs_country_date_set = set()


    # If no directory already, then makes a fresh one, if already, wipes it and re-makes
    def build_tmp_dir(self):
        if not os.path.exists(self._dir0):
             os.makedirs(self._dir0)
             os.chmod(self._dir0, 0o777)
        else:
            shutil.rmtree(self._dir0)
            os.makedirs(self._dir0)
            os.chmod(self._dir0, 0o777)

    def downloadFiles(self):

        credentials = service_account.Credentials.from_service_account_info(self._google_auth)
        client = storage.Client(credentials=credentials)
        for i, bucket in enumerate(self._google_buckets):

            bucket = client.get_bucket(bucket)
            is_playdemic = self._google_buckets_desc[i] == 'playdemic'

            for date in self._request_dates:
                # Sales report
                sales_report = r'sales/salesreport_{0}.zip'.format(date)
                #device_installs = r'stats/installs/installs_.*_{0}_{1}.csv'.format(date, 'installs')
                country_installs = r'stats/installs/installs_.*_{0}_{1}.csv'.format(date, 'country')

                if self._load == 'both' or self._load == 'sales':
                    blb = bucket.get_blob(sales_report)
                    if blb is not None:
                        salesName = f"{self._dir0}sales_{self._google_buckets_desc[i]}_{date}_{self._run_date}"
                        # Download sales to zip
                        with open(f"{salesName}.zip", 'wb') as saleszip:
                            blb.download_to_file(saleszip)
                        # Extract zip
                        with zipfile.ZipFile(f"{salesName}.zip", 'r') as zip_ref:
                            zip_ref.extractall(self._dir0)

                        unzipped_name = f"{self._dir0}salesreport_{date}.csv"
                        df = pd.read_csv(unzipped_name)

                        # remove playdemic except lego
                        if is_playdemic:
                            df = df[df['Product ID'] == "com.ttgames.legoswb"]

                        # Rename extracted file to expected name
                        if df.shape[0] == 0:
                            pass
                        else:
                            df.to_csv(unzipped_name, index=False)
                            os.rename(unzipped_name, f"{salesName}.csv")
                            self._sales_files.append(f"{salesName}.csv")
                    else:
                        logger.info('Warning. Desired salesreport does not exist yet')

                if self._load == 'both' or self._load == 'installs':
                    csvname = f"{self._dir0}installs_country_{self._google_buckets_desc[i]}_{date}_{self._run_date}.csv"

                    with open(csvname, 'wb') as installsCountry:
                        for k, blob in enumerate(bucket.list_blobs()):
                            if 'stats/installs/installs_' in str(blob) and '_{0}_{1}.csv'.format(date, 'country') in str(blob):
                                blob.download_to_file(installsCountry)

                    # Remove headers from mixed in with data
                    with io.open(csvname, 'r', encoding='utf-16le') as f:
                        lines = f.readlines()

                    with io.open(csvname, 'w', encoding='utf-16le') as f:
                        for j, line in enumerate(lines):    
                            # Remove non lego apps
                            if is_playdemic and ("com.ttgames.legoswb" not in line.lower()):
                                continue
                            # Get first header, applies for whole file
                            if 'Date,' not in str(line.encode('utf-8')) or j == 0:
                                d = f.write(line)
                    
                    self._installs_country_files.append(csvname)
        return 0

    def fileCatalog(self, date, rep, desc):
        if (date, rep, desc) in self._fileCatalog:
            return self._fileCatalog[(date, rep, desc)]
        else:
            # All cooked CSVs are UTF-8 Encoded
            file = io.open(self._dir0+ '{0}_{1}_{2}.csv'.format(date,rep,desc), 'w', encoding='utf-8', errors='ignore')
            self._fileCatalog.update({(date, rep, desc) : file})
            return file

    def processFiles(self):
        try:
            # First process sales data
            # Sales file from zip is utf-8
            for j, salesFile in enumerate(self._sales_files):
                sales = csv.reader(io.open(salesFile, newline='', encoding='ascii', errors='ignore'), delimiter=',', quotechar='\"')
                # Find proper desc for file
                for buck_dec in self._google_buckets_desc:
                    if buck_dec in salesFile:
                        desc = buck_dec
                colmax = 17 
                for k, row in enumerate(sales):
                    # Ignore headers
                    if k != 0:
                        # PRIMARY KEY is hash of order number (dashes remove) and financial status
                        hashval = hashlib.md5(str(row[0].replace('-','') + row[3]).encode('utf-8')).hexdigest()

                        # Date field is always 1 column

                        target = self.fileCatalog(row[1], 'sales', desc)

                        colcount = len(row)
                        if colcount > colmax:
                            colcount = colmax
                        line = hashval + ','

                        # FILTER OUT COMMAS FROM NUMBERS BASED ON FILE FORMAT
                        for i, col in enumerate(row):
                            if i >= colcount:
                                break
                            if i + 1 != colcount:
                                if i == 2 or i == 10 or i == 11 or i == 12:
                                    line += col.replace(',', '').replace('%','') + ','
                                else:
                                    # Check if need to quote
                                    if ',' in col:
                                        line += '%{0}%,'.format(col.replace('%',''))
                                    else:
                                        line += col.replace('%','') + ','
                            else:
                                if ',' in col:
                                    line += '%{0}%\n'.format(col.replace('%',''))
                                else:
                                    line += col + '\n'
                        logger.debug('writing sales line' + str(k))
                        #target.write(unicode(line.encode('utf-8')))
                        target.write(line)

            # Then process device installs
            '''
            for i, insDeviceFile in enumerate(self._installs_device_files):
                data = csv.reader(open(insDeviceFile, newline='\n'), delimiter=',', quotechar='\"')
                desc = self._google_buckets_desc[i]
                for i, row in enumerate(data):
                    # Ignore headers
                    if i != 0:
                        # Date is always first column
                        target = self.fileCatalog(row[0], 'installs_device', desc)
                        colcount = len(row)
                        line = ''
                        for i, col in enumerate(row):
                            if i > 2:
                                if i + 1 == colcount:
                                    line += col.replace(',', '')
                                else:
                                    line += col.replace(',', '') + ','
                            else:
                                # Check for need to quote column
                                if ',' in col:
                                    line += '%{0}%,'.format(col)
                                else:
                                    line += col + ','
                        target.write(line)
            '''

            # Then process country installs
            # Install files are unicode/utf-16
            for l, insCountryFile in enumerate(self._installs_country_files):
                #data = csv.reader(open(insCountryFile, newline='\n'), delimiter=',', quotechar='\"')
                data = csv.reader(io.open(insCountryFile, newline='\n', encoding='utf-16le', errors='ignore'), delimiter=',', quotechar='\"')

                # Find proper desc for file
                for buck_dec in self._google_buckets_desc:
                    if buck_dec in insCountryFile:
                        desc = buck_dec

                for k, row in enumerate(data):
                    # Ignore headers
                    # Headers in among the data since multiple downloads
                    # Check that first column is date by casting
                    #notHeader = True
                    #try:
                    #   datetime.datetime.strptime(row[0], "%Y-%m-%d")
                    #except ValueError:
                    #   notHeader = False
                    # Ignore header row
                    if k != 0:
                        # Date is always first column
                        target = self.fileCatalog(row[0], 'installs_country', desc)

                        colcount = len(row)
                        line = ''
                        for i, col in enumerate(row):
                            # Metrics need all commas removed, and metrics are col 3+
                            if i > 2:
                                if i + 1 == colcount:
                                    line += col.replace(',', '').replace('%','') + '\n'
                                else:
                                    line += col.replace(',', '').replace('%','') + ','

                            else:
                                # Check for need to quote column
                                if ',' in col:
                                    line += '%{0}%,'.format(col.replace('%',''))
                                else:
                                    line += col.replace('%','') + ','
                        logger.debug('writing installs line' + str(k))
                        #target.write(line)
                        #target.write(unicode(line.encode('utf-8')))
                        target.write(line)
                        
            # Close all CSVs and add their dates to the date set
            for key, file in self._fileCatalog.items():
                file.close()
                if key[1] == 'sales':
                    self._sales_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))
                if key[1] == 'installs_country':
                    self._installs_country_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))

        except Exception as e:
            logger.exception('Error during file processing')
            pdalert(self._pdtoken, self._pdkey, f"Error during file processing: {e}", 'warning')
            return -1

        return 0

    def newData(self):
        # Checks if the max date seen is there for all data sets
        # And checks if the max date is not already in S3
        # Returns -1 error
        # Returns 0 No new data
        # Returns 1 New Data Sales and Installs
        # Returns 2 New Sales Data
        # Returns 3 New Installs Data

        # NOTE: ALWAYS RETURNS 1 if 'RANGE' (Implied there is user intervention and so must load)
        if self._load_format == 'Range':
            if self._load == 'both':
                return 1
            elif self._load == 'sales':
                return 2
            elif self._load == 'installs':
                return 3

        try:

            connection = boto.connect_s3(self._s3_access_key, self._s3_secret_key)
            # Get s3 bucket
            bucket = connection.get_bucket(self._s3_bucket, validate=True, headers=None)

            # Check for what new date in S3 would be, then check that have that locally
            # Go from most recent, if the data exists FOR ALL DESC, then the new_data is that
            # day plus 1
            for date in sorted(self._sales_date_set, reverse=True):
                sales_data_exists = []
                for desc in self._google_buckets_desc:
                    sKey = Key(bucket)
                    sKey.key = (self._s3_folder_root + 'sales/processed/' + str(date)[:10] +
                        '/' + str(date)[:10] + '_sales_' + desc + '.csv')
                    sales_data_exists.append(sKey.exists())
                if all(sales_data_exists):
                    new_sales_date = date + datetime.timedelta(days=1)
                    break
                else:
                    new_sales_date = date

            for idate in sorted(self._installs_country_date_set, reverse=True):
                inst_data_exists = []
                for desc1 in self._google_buckets_desc:
                    sKey = Key(bucket)
                    sKey.key = (self._s3_folder_root + 'installs_country/processed/' + str(idate)[:10] +
                        '/' + str(idate)[:10] + '_installs_country_' + desc1 + '.csv')
                    inst_data_exists.append(sKey.exists())
                if all(inst_data_exists):
                    new_install_date = idate + datetime.timedelta(days=1)
                    break
                else:
                    new_install_date = idate

            connection.close()

            haveSales = True
            haveInstalls = True
            for desc in self._google_buckets_desc:
                if desc == 'playdemic':
                    continue
                if not os.path.isfile(self._dir0 + str(new_sales_date)[:10] + '_sales_' + desc + '.csv'):
                    haveSales = False
                if not os.path.isfile(self._dir0 + str(new_install_date)[:10] + '_installs_country_' + desc + '.csv'):
                    haveInstalls = False

            if haveSales and haveInstalls and self._load == 'both':
                return 1
            elif haveSales and (self._load == 'both' or self._load == 'sales'):
                return 2
            elif haveInstalls and (self._load == 'both' or self._load == 'installs'):
                return 3
            else:
                return 0

        except Exception as e:
            pdalert(self._pdtoken, self._pdkey, f"Error in funtion newData() checking S3 for max date {e}", 'error')
            raise e

    def getSize(self, file):
        try:
            size = os.fstat(file.fileno()).st_size
        except:
            file.seek(0, os.SEEK_END)
            size = file.tell()
        return size

    def uploadFile(self, filename, bucket, folder):
        logger.debug('Starting upload')
        # Calculate local file sizes
        file = open(self._dir0 + filename, 'r+')
        fileSize = self.getSize(file)
        file.close()
        try:
            # Try to delete old files if necessary
            try:
                logger.debug('Deleting ' + filename)

                # Delete old file
                delKey = Key(bucket)
                delKey.key = folder+ '/' + filename
                bucket.delete_key(delKey)
            except:
                logger.debug('Didnt have to delete')

            logger.debug('Uploading ' + filename)

            # Insert new file
            insKey = Key(bucket)
            insKey.key = folder+ '/' + filename
            sentFile = insKey.set_contents_from_filename(self._dir0 + filename, None, None, False, encrypt_key=True)

        except Exception as e:
            pdalert(self._pdtoken, self._pdkey, f"s3 upload failed {e}", 'error')
            # return -1
            raise e

        # Check upload size matches local size
        if fileSize != sentFile:
            pdalert(self._pdtoken, self._pdkey, 'S3 upload sizes do not match')
            # return -1
            raise Exception("Uploaded file to S3 does not match local file size.")

        return 0

    def uploadManifests(self, bucket, status):
        # 3 Manifests per bucket 3 target tables, installs device, installs country, sales
        #for i, desc in enumerate(self._google_buckets_desc):
        sales = open(self._dir0 + 'manifest_sales_'+ self._run_date + '.txt', 'w')
        installsCountry = open(self._dir0 + 'manifest_installs_country_' + self._run_date + '.txt', 'w')
        #installsDevice = open(self._dir0 + 'manifest_installs_device_' + self._run_date + '.txt', 'w')

        contentsSales = {'entries' : []}
        #contentsInstallsDevice = {'entries' : []}
        contentsInstallsCountry = {'entries' : []}

        for (date, rep, desc2) in self._fileCatalog:
            if rep == 'sales':
                entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'sales/processed/' + date + '/' +
                '{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
                contentsSales['entries'].append(entry)
            #elif rep == 'installs_device':
            #   entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + '/installs_device/processed/' + date + '/' +
            #   '{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
            #   contentsInstallsDevice['entries'].append(entry)
            elif rep == 'installs_country':
                entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'installs_country/processed/' + date + '/' +
                '{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
                contentsInstallsCountry['entries'].append(entry)

        sales.write(json.dumps(contentsSales))
        sales.close()

        installsCountry.write(json.dumps(contentsInstallsCountry))
        installsCountry.close()

        #installsDevice.write(json.dumps(contentsInstallsDevice))
        #installsDevice.close()

        # Try deleting old manifests
        if status == 1 or status == 2:
            try:
                ManifestDel = Key(bucket)
                ManifestDel.key = self._s3_folder_root + 'sales/manifest/' + self._run_date + '/manifest_sales_'+ self._run_date + '.txt'
                bucket.delete_key(ManifestDel)
            except:
                logger.debug('Did not have to delete old sales manifest')
        if status == 1 or status == 3:
            try:
                ManifestDel = Key(bucket)
                ManifestDel.key = (self._s3_folder_root + 'installs_country/manifest/' + self._run_date
                    + '/manifest_installs_country_' + self._run_date + '.txt')
                bucket.delete_key(ManifestDel)
            except:
                logger.debug('Did not have to delete old installs country manifest')

        #try:
        #   ManifestDel = Key(bucket)
        #   ManifestDel.key = (self._s3_folder_root + 'installs_device/manifest/' + self._run_date
        #       + '/manifest_installs_device_' + self._run_date + '.txt')
        #   bucket.delete_key(ManifestDel)
        #except:
        #   logger.debug('Did not have to delete old installs country manifest')

        # Upload new Manifests
        try:
            # Sales
            if status == 1 or status == 2:
                salKey = Key(bucket)
                salKey.key = self._s3_folder_root + 'sales/manifest/' + self._run_date + '/manifest_sales_'+ self._run_date + '.txt'
                salKey.set_contents_from_filename(self._dir0 + 'manifest_sales_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)

            # Installs Country
            if status == 1 or status == 3:
                icKey = Key(bucket)
                icKey.key = self._s3_folder_root + 'installs_country/manifest/' + self._run_date + '/manifest_installs_country_'+ self._run_date + '.txt'
                icKey.set_contents_from_filename(self._dir0 + 'manifest_installs_country_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)

            # Installs Device
            #idKey = Key(bucket)
            #idKey.key = self._s3_folder_root + 'installs_device/manifest/' + self._run_date + '/manifest_' + '_sales_'+ self._run_date + '.txt'
            #idKey.set_contents_from_filename(self._dir0 + 'manifest_installs_device_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)
        except Exception as e:
            pdalert(self._pdtoken, self._pdkey, f"S3 manifest upload failed {e}", 'error')
            # return -1
            raise e

        return 0


    def s3Upload(self, status):
        connection = boto.connect_s3(self._s3_access_key, self._s3_secret_key)
        # Get s3 bucket
        bucket = connection.get_bucket(self._s3_bucket, validate=True, headers=None)
        # for every date, upload files
        for (date, rep, desc) in self._fileCatalog.keys():
            # Check for status of which files are new
            if status == 1 or (status == 2 and rep == 'sales') or (status == 3 and rep =='installs_country'):
                self.uploadFile('{0}_{1}_{2}.csv'.format(date,rep,desc), bucket, self._s3_folder_root + '/' + rep + '/processed/' + date)

        # Upload raw sales files
        if status == 1 or status == 2:
            for i, salesFile in enumerate(self._sales_files):
                # Find proper desc for file
                for buck_dec in self._google_buckets_desc:
                    if buck_dec in salesFile:
                        desc = buck_dec

                self.uploadFile(salesFile[len(self._dir0):], bucket, self._s3_folder_root + '/sales/raw/' + self._run_date)

        # Upload raw device installs files
        #for i, deviceFile in enumerate(self._installs_device_files):
        #   desc = self._google_buckets_desc[i]

        #   if self.uploadFile(deviceFile[len(self._dir0):], bucket, self._s3_folder_root + '/installs_device/raw/' + self._run_date) == -1:
        #       return -1

        # upload raw device installs files
        if status == 1 or status == 3:
            for i, countryFile in enumerate(self._installs_country_files):
                # Find proper desc for file
                for buck_dec in self._google_buckets_desc:
                    if buck_dec in countryFile:
                        desc = buck_dec

                self.uploadFile(countryFile[len(self._dir0):], bucket, self._s3_folder_root + '/installs_country/raw/' + self._run_date)

        self.uploadManifests(bucket, status)

        connection.close()

        return 0

    def datesListToDelete(self):
        try:
            sales_date_str = '('
            sales_date_count = len(self._sales_date_set)
            for i, dt in enumerate(self._sales_date_set):
                if i + 1 != sales_date_count:
                    sales_date_str += '\'{0}\','.format(str(dt)[:10])
                else:
                    sales_date_str +='\'{0}\')'.format(str(dt)[:10])

            ic_date_str = '('
            ic_date_count = len(self._installs_country_date_set)
            for i, dt in enumerate(self._installs_country_date_set):
                if i + 1 != ic_date_count:
                    ic_date_str += '\'{0}\','.format(str(dt)[:10])
                else:
                    ic_date_str +='\'{0}\')'.format(str(dt)[:10])

            # save the sales dates in a file in S3 to be used by the PostgresHook in the Dag file
            s3_hook = S3Hook('aws_conn_s3')
            s3_hook.load_file_obj(io.BytesIO(bytearray(sales_date_str, encoding ='utf-8')), 
                                    key=self._sales_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)

            # save the installs dates in a file in S3 to be used by the PostgresHook in the Dag file
            s3_hook.load_file_obj(io.BytesIO(bytearray(ic_date_str, encoding ='utf-8')), 
                                    key=self._installs_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)


        except Exception as e:
            pdalert(self._pdtoken, self._pdkey, f"Redshift Copy failed {e}", 'error')
            # return -1
            raise e

        return 0

    def clean(self):
        logger.info('Cleaning up temp folder')
        if os.path.isdir(self._dir0):
            shutil.rmtree(self._dir0)

    def write_check_file(self):
        bucket = self._s3_bucket
        key = self._check_file
        s3_hook = S3Hook('aws_conn_s3')
        s3_hook.load_file_obj(io.BytesIO(bytearray(self._msg, encoding ='utf-8')), key, bucket_name=bucket, replace=True, encrypt=False)

    def executeETL(self):
        logger.info('########## Beginning Google ETL ##########')
        a = self.downloadFiles()
        logger.info('Downloaded Files')
        b = self.processFiles()
        logger.info('Processed Files')

        if a==0 and b==0:
            status = self.newData()
            logger.info(f"status: {status}")

        if status == 0:
            self.clean()
            self._msg = 'No New Data'
            self.write_check_file()
            logger.info('### NO New data. Terminating ###')
            return 0
        elif status > 0:
            self._msg = 'New Data'
            self.write_check_file()
        else:
            self._msg = 'Error'
            self.write_check_file()
            
        logger.info('New Data Detected. Beginning S3 Upload and Redshift with status ' + str(status))
        self.s3Upload(status)
        logger.info('Uploaded New Files To S3')
        self.datesListToDelete()
        logger.info('Copied New Files To Redshift')

        self.clean()
        logger.info('########### Finished Google ETL ###########')

        return 0

def pdalert(token, service, msg, severity='info'):
    """
    Handles Pager duty alerts

    Params
    ------
    msg
      string of the alert to post
    severity
        string, it can be one of 4 values: critical, error, warning or info
    Return
    ------
    Bool: True
    """

    output = 'PagerDuty - s: ' + service + ' / m: ' + msg
    logger.info(output)

    headers = {
      'Authorization': 'Token token={0}'.format(token),
      'Content-type': 'application/json',
      'Accept': 'application/vnd.pagerduty+json;version=2'
    }

    payload = json.dumps({
          "routing_key": service,
          "event_action": "trigger",
          "client": "ETL Sales - Google Play",
          "client_url": "https://monitoring.service.com",
          "payload": {
              "summary": msg,
              "timestamp": datetime.datetime.now().isoformat(),
              "severity": severity,
              "group": "etl",
              "source": "Airflow Dag etl_google_play"
            }
        })
    r = requests.post(
                    'https://events.pagerduty.com/v2/enqueue',
                    headers=headers,
                    data=payload
                    )

def main():

    parser = argparse.ArgumentParser(description="todo")
    parser.add_argument('--config_ini', required=True)
    args = parser.parse_args()
    config_ini = args.config_ini
    google_auth = SecretManagerHook(secret_name='google_auth', region='us-east-1').get_secret()
    aws_creds = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()

    a = GooglePlay(config_ini, aws_creds, google_auth)
    a.executeETL()

if __name__ == '__main__':
    main()