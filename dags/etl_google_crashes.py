"""
previous commit done on 2021-Jun-15 10:21 GMT-5  8180d6d47e47c303db88d403ed53edc77c0aa5d7
"""
import os
import io
import sys
import csv
import json
import shutil
import requests
import argparse
import logging
import logging.handlers
import datetime
import boto
from boto.s3.key import Key
from airflow.hooks.S3_hook import S3Hook
from google.cloud import storage
from google.oauth2 import service_account
from hooks.secret_manager_hook import SecretManagerHook

######  Initialize Logger  ######

# Configure output level
logger = logging.getLogger('GoogleLogger')
logger.setLevel(logging.INFO)

### NOTE: Device install code is commented out in case of desire/request for it in the future
# Decision not to include was based on load of rows/usefulness

class GoogleCrash(object):

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

		self._app_version_date_file = config["app_version_date_file"]
		self._device_date_file = config["device_date_file"]
		self._os_version_date_file = config["os_version_date_file"]
		self._overview_date_file = config["overview_date_file"]
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

		# Local file directory
		self._dir0 = config["file_directory"]
		self.build_tmp_dir()

		# Variables for general refernece
		# Local raw files
		self._crashes_files = []

		# Date + desc + rep to date CSV
		self._fileCatalog = {}
		# Local date set
		self._app_version_date_set = set()
		self._device_date_set = set()
		self._os_version_date_set = set()
		self._overview_date_set = set()

	# If no directory already, then makes a fresh one, if already, wipes it and re-makes
	def build_tmp_dir(self):
		if not os.path.exists(self._dir0):
			 os.makedirs(self._dir0)
			 os.chmod(self._dir0, 0o777)
		else:
			shutil.rmtree(self._dir0)
			os.makedirs(self._dir0)
			os.chmod(self._dir0, 0o777)

	def supportedDevices(self):
		logger.info('supportedDevices Starts')
		#gs://play_public/supported_devices.csv
		#curl http://storage.googleapis.com/play_public/supported_devices.csv --output supportedDevices.csv

		url = "http://storage.googleapis.com/play_public/supported_devices.csv"

		csv_file = self._dir0 + 'supported_devices.csv'
		print(csv_file)
		result = requests.request('GET', url=url)
		#print(result.text)
		with open(csv_file, "w", encoding="utf-8") as fr:
			fr.write(result.text.replace('\t', ''))  #added replace method

		return 0

	def downloadFiles(self):
		logger.info('downloadFiles Starts')

		credentials = service_account.Credentials.from_service_account_info(self._google_auth)
		client = storage.Client(credentials=credentials)
		for i, bucket in enumerate(self._google_buckets):

			bucket = client.get_bucket(bucket)
			for date in self._request_dates:
				logger.info(date)
				types=['app_version','device','os_version','overview']

				if self._load == 'crashes':
					for tt in range(0,len(types)):
						crashesFile = open(self._dir0 + 'crashes_'+types[tt]+'_' + self._google_buckets_desc[i] + '_' + date
							+ '_' + self._run_date + '.csv', 'wb')

						# Loop through to find install files
						for k, blob in enumerate(bucket.list_blobs()):
							if 'stats/crashes/crashes_' in str(blob) and '_{0}_{1}.csv'.format(date, types[tt]) in str(blob):
								blob.download_to_file(crashesFile)

						crashesFile.close()

						# Remove headers from mixed in with data
						f = io.open(self._dir0 + 'crashes_'+types[tt]+'_' + self._google_buckets_desc[i] + '_' + date
							+ '_' + self._run_date + '.csv', 'r', encoding='utf-16le')
						lines = f.readlines()
						f.close()
						f = io.open(self._dir0 + 'crashes_'+types[tt]+'_' + self._google_buckets_desc[i] + '_' + date
							+ '_' + self._run_date + '.csv', 'w', encoding='utf-16le')

						for j, line in enumerate(lines):
							# Get first header, applies for whole file
							if 'Date,' not in str(line.encode('utf-8')) or j == 0:
								f.write(line)
						f.close()

						logger.info(self._dir0 + 'crashes_'+types[tt]+'_' + self._google_buckets_desc[i] + '_' + date + '_' + self._run_date + '.csv')
						self._crashes_files.append(self._dir0 + 'crashes_'+types[tt]+'_' + self._google_buckets_desc[i] + '_' + date + '_' + self._run_date + '.csv')

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

		# Process app version crashes
		# files are unicode/utf-16
		for l, crashFile in enumerate(self._crashes_files):

			data = csv.reader(io.open(crashFile, newline='\n', encoding='utf-16', errors='ignore'), delimiter=',', quotechar='\"')

			# Find proper desc for file
			for buck_dec in self._google_buckets_desc:
				#logger.debug('buck_dec ' + buck_dec + ', crashFile ' + crashFile)
				if buck_dec in crashFile:
					desc = buck_dec
					#logger.debug('MATCH buck_dec ' + buck_dec + ', crashFile ' + crashFile)

			for k, row in enumerate(data):
				# Ignore headers
				# Headers in among the data since multiple downloads
				# Check that first column is date by casting
				#notHeader = True
				#try:
				#	datetime.datetime.strptime(row[0], "%Y-%m-%d")
				#except ValueError:
				#	notHeader = False
				# Ignore header row
				if k != 0:
					if 'app_version' in crashFile:
						filetype = 'app_version'
					elif 'device' in crashFile:
						filetype = 'device'
					elif 'os_version' in crashFile:
						filetype = 'os_version'
					elif 'overview' in crashFile:
						filetype = 'overview'
					# Date is always first column
					target = self.fileCatalog(row[0], filetype, desc)

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
					#logger.debug('writing crashes line ' + line) #str(k))
					target.write(line)
					#target.write(str(line.encode('utf-8')))
					#target.write(unicode(line.encode('utf-8')))
			target.close()

		# Close all CSVs and add their dates to the date set
		for key, file in self._fileCatalog.items():
			file.close()
			logger.debug('CHECKING...')
			logger.debug(file.name)
			if 'app_version' in file.name:
				self._app_version_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))
				logger.debug('Adding date to _app_version_date_set')
			elif 'device' in file.name:
				self._device_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))
				logger.debug('Adding date to _device_date_set')
			elif 'os_version' in file.name:
				self._os_version_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))
				logger.debug('Adding date to _os_version_date_set')
			elif 'overview' in file.name:
				self._overview_date_set.add(datetime.datetime.strptime(key[0], "%Y-%m-%d"))
				logger.debug('Adding date to _overview_date_set')

		logger.info('processFiles() Completed')
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
			if self._load == 'crashes':
				return ['app_version','device','os_version','overview']


		connection = boto.connect_s3(self._s3_access_key, self._s3_secret_key)
		# Get s3 bucket
		bucket = connection.get_bucket(self._s3_bucket, validate=True, headers=None)

		# Check for what new date in S3 would be, then check that have that locally
		# Go from most recent, if the data exists FOR ALL DESC, then the new_data is that
		# day plus 1
		#2019-02-01_crashes_app_version_playdemic.csv
		logger.info('Length self._app_version_date_set: ' + str(len(self._app_version_date_set)))
		logger.info('Length self._device_date_set: ' + str(len(self._device_date_set)))
		logger.info('Length self._os_version_date_set: ' + str(len(self._os_version_date_set)))
		logger.info('Length self._overview_date_set: ' + str(len(self._overview_date_set)))
		new_overview_date=""
		new_app_version_date=""
		new_device_date=""
		new_os_version_date=""
		for date in sorted(self._app_version_date_set, reverse=True):
			app_version_data_exists = []
			for desc in self._google_buckets_desc:
				logger.debug('google_buckets_desc AV: ' + desc + ', date: ' + str(date)[:10])
				sKey = Key(bucket)
				sKey.key = (self._s3_folder_root + 'crashes/processed/app_version/' + str(date)[:10] +
					'/' + str(date)[:10] + '_app_version_' + desc + '.csv')
				logger.debug("S3 file: " + sKey.key)
				app_version_data_exists.append(sKey.exists())
				logger.debug('app_version_data_exists: ' + str(app_version_data_exists))

			if all(app_version_data_exists):
				new_app_version_date = date + datetime.timedelta(days=1)
				logger.debug('new_app_version_date found')
				logger.debug(new_app_version_date)
				break
			else:
				logger.debug('new_app_version_date NOT found')

		for date in sorted(self._device_date_set, reverse=True):
			device_data_exists = []
			for desc in self._google_buckets_desc:
				logger.debug('google_buckets_desc DE: ' + desc + ', date: ' + str(date)[:10])
				sKey = Key(bucket)
				sKey.key = (self._s3_folder_root + 'crashes/processed/device/' + str(date)[:10] +
					'/' + str(date)[:10] + '_device_' + desc + '.csv')
				logger.debug("S3 file: " + sKey.key)
				device_data_exists.append(sKey.exists())
				logger.debug('device_data_exists: ' + str(device_data_exists))

			if all(device_data_exists):
				new_device_date = date + datetime.timedelta(days=1)
				logger.debug('new_device_date found')
				logger.debug(new_device_date)
				break
			else:
				logger.debug('new_device_date NOT found')

		for date in sorted(self._os_version_date_set, reverse=True):
			os_version_data_exists = []
			for desc in self._google_buckets_desc:
				logger.debug('google_buckets_desc OS: ' + desc + ', date: ' + str(date)[:10])
				sKey = Key(bucket)
				sKey.key = (self._s3_folder_root + 'crashes/processed/os_version/' + str(date)[:10] +
					'/' + str(date)[:10] + '_os_version_' + desc + '.csv')
				logger.debug("S3 file: " + sKey.key)
				os_version_data_exists.append(sKey.exists())
				logger.debug('os_version_data_exists: ' + str(os_version_data_exists))

			if all(os_version_data_exists):
				new_os_version_date = date + datetime.timedelta(days=1)
				logger.debug('new_os_version_date found')
				logger.debug(new_os_version_date)
				break
			else:
				logger.debug('new_os_version_date NOT found')

		for date in sorted(self._overview_date_set, reverse=True):
			overview_data_exists = []
			for desc in self._google_buckets_desc:
				logger.debug('google_buckets_desc OV: ' + desc + ', date: ' + str(date)[:10])
				sKey = Key(bucket)
				sKey.key = (self._s3_folder_root + 'crashes/processed/overview/' + str(date)[:10] +
					'/' + str(date)[:10] + '_overview_' + desc + '.csv')
				logger.debug("S3 file: " + sKey.key)
				overview_data_exists.append(sKey.exists())
				logger.debug('overview_data_exists: ' + str(overview_data_exists))

			if all(overview_data_exists):
				new_overview_date = date + datetime.timedelta(days=1)
				logger.debug('new_overview_date found')
				logger.debug(new_overview_date)
				break
			else:
				logger.debug('new_overview_date NOT found')

		connection.close()

		haveCrashesAppVersion = True
		haveCrashesDevice = True
		haveCrashesOSVersion = True
		haveCrashesOverview = True
		try:
			new_app_version_date
		except:
			new_app_version_date=None
		try:
			new_device_date
		except:
			new_device_date=None
		try:
			new_os_version_date
		except:
			new_os_version_date=None
		try:
			new_overview_date
		except:
			new_overview_date=None

		for desc in self._google_buckets_desc:
			if new_app_version_date is not None:
				if not os.path.isfile(self._dir0 + str(new_app_version_date)[:10] + '_app_version_' + desc + '.csv'):
					haveCrashesAppVersion = False

			if new_device_date is not None:
				if not os.path.isfile(self._dir0 + str(new_device_date)[:10] + '_device_' + desc + '.csv'):
					haveCrashesDevice = False

			if new_os_version_date is not None:
				if not os.path.isfile(self._dir0 + str(new_os_version_date)[:10] + '_os_version_' + desc + '.csv'):
					haveCrashesOSVersion = False

			if new_overview_date is not None:
				if not os.path.isfile(self._dir0 + str(new_overview_date)[:10] + '_overview_' + desc + '.csv'):
					haveCrashesOverview = False

		crashesData=[]
		if haveCrashesAppVersion:
			logger.info('haveCrashesAppVersion')
			crashesData.append('app_version')
		if haveCrashesDevice:
			logger.info('haveCrashesDevice')
			crashesData.append('device')
		if haveCrashesOSVersion:
			logger.info('haveCrashesOSVersion')
			crashesData.append('os_version')
		if haveCrashesOverview:
			logger.info('haveCrashesOverview')
			crashesData.append('overview')
		else:
			logger.info('NO Crashes')

		logger.info('newData() Completed')

		return crashesData


	def getSize(self, file):
		try:
			size = os.fstat(file.fileno()).st_size
		except:
			file.seek(0, os.SEEK_END)
			size = file.tell()
		return size

	def uploadFile(self, filename, bucket, folder):
		logger.debug('Starting upload ' + filename)
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
			logger.debug('Uploaded: ' + filename + ', size: ' + str(sentFile))

		except Exception as e:
			logger.exception('S3 upload failed')
			#pdalert(self._pdkey, 's3 upload exception %s'%(e))
			# return -1
			raise e

		# Check upload size matches local size
		if fileSize != sentFile:
			logger.error('Uploaded file to S3 does not match local file size.')
			logger.debug('sizes: ' + str(fileSize) + ' ' + str(sentFile))
			#pdalert(self._pdkey, 'S3 upload sizes do not match ' + str(fileSize) + ' ' + str(sentFile))
			# return -1
			raise Exception("Uploaded file to S3 does not match local file size.")

		return 0

	def uploadManifests(self, bucket, status):

		# build supported devices manifest
		supdev_manifest = open(self._dir0 + 'supported_devices_manifest.txt', 'w')
		contentsSupportedDevices = {'entries' : []}

		supdev_entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'crashes/supported_devices/supported_devices.csv'}
		contentsSupportedDevices['entries'].append(supdev_entry)
		supdev_manifest.write(json.dumps(contentsSupportedDevices))
		supdev_manifest.close()
		try:
			ManifestDel = Key(bucket)
			ManifestDel.key = self._s3_folder_root + 'crashes/supported_devices/supported_devices_manifest.txt'
			bucket.delete_key(ManifestDel)
		except:
			logger.debug('Did not have to delete old supported devices manifest')

		try:
			sdKey = Key(bucket)
			sdKey.key = self._s3_folder_root + 'crashes/supported_devices/supported_devices_manifest.txt'
			sdKey.set_contents_from_filename(self._dir0 + 'supported_devices_manifest.txt', None, None, False, encrypt_key=True)
		except:
			logger.exception('S3 supported devices manifest upload failed')
			return -1


		appversionCrashes = open(self._dir0 + 'manifest_app_version_'+ self._run_date + '.txt', 'w')
		deviceCrashes = open(self._dir0 + 'manifest_device_' + self._run_date + '.txt', 'w')
		osversionCrashes = open(self._dir0 + 'manifest_os_version_' + self._run_date + '.txt', 'w')
		overviewCrashes = open(self._dir0 + 'manifest_overview_' + self._run_date + '.txt', 'w')

		contentsAppVersionCrashes = {'entries' : []}
		contentsDeviceCrashes = {'entries' : []}
		contentsOSVersionCrashes = {'entries' : []}
		contentsOverviewCrashes = {'entries' : []}

		for (date, rep, desc2) in self._fileCatalog:
			if rep == 'app_version':
				entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'crashes/processed/app_version/' + date + '/' +
				'{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
				contentsAppVersionCrashes['entries'].append(entry)
			elif rep == 'device':
				entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'crashes/processed/device/' + date + '/' +
				'{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
				contentsDeviceCrashes['entries'].append(entry)
			elif rep == 'os_version':
				entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'crashes/processed/os_version/' + date + '/' +
				'{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
				contentsOSVersionCrashes['entries'].append(entry)
			elif rep == 'overview':
				entry = {'url' : 's3://' + self._s3_bucket + self._s3_folder_root + 'crashes/processed/overview/' + date + '/' +
				'{0}_{1}_{2}.csv'.format(date,rep,desc2), 'mandatory' : True}
				contentsOverviewCrashes['entries'].append(entry)

		appversionCrashes.write(json.dumps(contentsAppVersionCrashes))
		appversionCrashes.close()

		deviceCrashes.write(json.dumps(contentsDeviceCrashes))
		deviceCrashes.close()

		osversionCrashes.write(json.dumps(contentsOSVersionCrashes))
		osversionCrashes.close()

		overviewCrashes.write(json.dumps(contentsOverviewCrashes))
		overviewCrashes.close()

		# Try deleting old manifests
		if len(status) > 0:
			for i in range(0, len(status)):
				if status[i] == 'app_version':
					try:
						ManifestDel = Key(bucket)
						ManifestDel.key = self._s3_folder_root + 'crashes/manifest/app_version/' + self._run_date + '/manifest_app_version_'+ self._run_date + '.txt'
						bucket.delete_key(ManifestDel)
					except:
						logger.debug('Did not have to delete old app_version crashes manifest')
				if status[i] == 'device':
					try:
						ManifestDel = Key(bucket)
						ManifestDel.key = self._s3_folder_root + 'crashes/manifest/device/' + self._run_date + '/manifest_device_'+ self._run_date + '.txt'
						bucket.delete_key(ManifestDel)
					except:
						logger.debug('Did not have to delete old device crashes manifest')
				if status[i] == 'os_version':
					try:
						ManifestDel = Key(bucket)
						ManifestDel.key = self._s3_folder_root + 'crashes/manifest/os_version/' + self._run_date + '/manifest_os_version_'+ self._run_date + '.txt'
						bucket.delete_key(ManifestDel)
					except:
						logger.debug('Did not have to delete old os_version crashes manifest')
				if status[i] == 'overview':
					try:
						ManifestDel = Key(bucket)
						ManifestDel.key = self._s3_folder_root + 'crashes/manifest/overview/' + self._run_date + '/manifest_overview_'+ self._run_date + '.txt'
						bucket.delete_key(ManifestDel)
					except:
						logger.debug('Did not have to delete old overview crashes manifest')

		# Upload new Manifests
		try:
			if len(status) > 0:
				for i in range(0, len(status)):
					# app version
					if status[i] == 'app_version':
						salKey = Key(bucket)
						salKey.key = self._s3_folder_root + 'crashes/manifest/app_version/' + self._run_date + '/manifest_app_version_'+ self._run_date + '.txt'
						salKey.set_contents_from_filename(self._dir0 + 'manifest_app_version_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)
						logger.debug("appversionKey: " + salKey.key)

					# Device
					if status[i] == 'device':
						salKey = Key(bucket)
						salKey.key = self._s3_folder_root + 'crashes/manifest/device/' + self._run_date + '/manifest_device_'+ self._run_date + '.txt'
						salKey.set_contents_from_filename(self._dir0 + 'manifest_device_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)
						logger.debug("deviceKey: " + salKey.key)

					# Device
					if status[i] == 'os_version':
						salKey = Key(bucket)
						salKey.key = self._s3_folder_root + 'crashes/manifest/os_version/' + self._run_date + '/manifest_os_version_'+ self._run_date + '.txt'
						salKey.set_contents_from_filename(self._dir0 + 'manifest_os_version_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)
						logger.debug("os_versionKey: " + salKey.key)

					# Device
					if status[i] == 'overview':
						salKey = Key(bucket)
						salKey.key = self._s3_folder_root + 'crashes/manifest/overview/' + self._run_date + '/manifest_overview_'+ self._run_date + '.txt'
						salKey.set_contents_from_filename(self._dir0 + 'manifest_overview_'+ self._run_date + '.txt', None, None, False, encrypt_key=True)
						logger.debug("overviewKey: " + salKey.key)

		except Exception as e:
			logger.exception('S3 manifest upload failed')
			#pdalert(self._pdkey, 'S3 manifest upload exception %s'%(e))
			return -1

		return 0

	def s3Upload(self, status):
		connection = boto.connect_s3(self._s3_access_key, self._s3_secret_key)
		# Get s3 bucket
		bucket = connection.get_bucket(self._s3_bucket, validate=True, headers=None)

		if self.uploadFile('supported_devices.csv', bucket, self._s3_folder_root + '/crashes/supported_devices') == -1:
			return -1

		# for every date, upload files
		for (date, rep, desc) in self._fileCatalog.keys():
			# Check for status of which files are new
			#if status == 1 or (status == 2 and rep == 'sales') or (status == 3 and rep =='installs_country'):
			if len(status) > 0:
				self.uploadFile('{0}_{1}_{2}.csv'.format(date,rep,desc), bucket, self._s3_folder_root + '/crashes/processed/' + rep + '/' + date)

		# Upload raw crashes files
		#if status == 1 or status == 2:
		if len(status) > 0:
			for i, crashesFile in enumerate(self._crashes_files):
				# Find proper desc for file
				for buck_dec in self._google_buckets_desc:
					if buck_dec in crashesFile:
						desc = buck_dec

				self.uploadFile(crashesFile[len(self._dir0):], bucket, self._s3_folder_root + '/crashes/raw/' + self._run_date)

		self.uploadManifests(bucket, status)

		connection.close()

		return 0

	def datesListToDelete(self, status):

		if len(status) > 0:
			logger.debug('STATUS: ' + str(status))
			for w in range(0, len(status)):
				if status[w] == 'app_version':
					app_version_date_str = '('
					app_version_date_count = len(self._app_version_date_set)
					for i, dt in enumerate(self._app_version_date_set):
						logger.debug('AppVersion date added: ' + str(dt)[:10])
						if i + 1 != app_version_date_count:
							app_version_date_str += '\'{0}\','.format(str(dt)[:10])
						else:
							app_version_date_str +='\'{0}\')'.format(str(dt)[:10])

				if status[w] == 'device':
					device_date_str = '('
					device_date_count = len(self._device_date_set)
					for i, dt in enumerate(self._device_date_set):
						logger.debug('Device date added: ' + str(dt)[:10])
						if i + 1 != device_date_count:
							device_date_str += '\'{0}\','.format(str(dt)[:10])
						else:
							device_date_str +='\'{0}\')'.format(str(dt)[:10])

				if status[w] == 'os_version':
					os_version_date_str = '('
					os_version_date_count = len(self._os_version_date_set)
					for i, dt in enumerate(self._os_version_date_set):
						logger.debug('OSVersion date added: ' + str(dt)[:10])
						if i + 1 != os_version_date_count:
							os_version_date_str += '\'{0}\','.format(str(dt)[:10])
						else:
							os_version_date_str +='\'{0}\')'.format(str(dt)[:10])

				if status[w] == 'overview':
					overview_date_str = '('
					overview_date_count = len(self._overview_date_set)
					for i, dt in enumerate(self._overview_date_set):
						logger.debug('Overview date added: ' + str(dt)[:10])
						if i + 1 != overview_date_count:
							overview_date_str += '\'{0}\','.format(str(dt)[:10])
						else:
							overview_date_str +='\'{0}\')'.format(str(dt)[:10])

		# save the app version dates in a file to be used by the PostgreHook
		s3_hook = S3Hook('aws_conn_s3')
		logger.debug(f"Writing app version date file: {self._app_version_date_file}")
		s3_hook.load_file_obj(io.BytesIO(bytearray(app_version_date_str, encoding ='utf-8')), 
								key=self._app_version_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)

		# save the device dates in a file to be used by the PostgreHook
		logger.debug(f"Writing device date file: {self._device_date_file}")
		s3_hook.load_file_obj(io.BytesIO(bytearray(device_date_str, encoding ='utf-8')), 
								key=self._device_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)

		# save the os version dates in a file to be used by the PostgreHook
		logger.debug(f"Writing os version date file: {self._os_version_date_file}")
		s3_hook.load_file_obj(io.BytesIO(bytearray(os_version_date_str, encoding ='utf-8')), 
								key=self._os_version_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)

		# save the overview dates in a file to be used by the PostgreHook
		logger.debug(f"Writing overview date file: {self._overview_date_file}")
		s3_hook.load_file_obj(io.BytesIO(bytearray(overview_date_str, encoding ='utf-8')), 
								key=self._overview_date_file, bucket_name=self._s3_bucket, replace=True, encrypt=False)

		return 0

	def clean(self):
		logger.info('Cleaning up temp folder')
		if os.path.isdir(self._dir0):
			shutil.rmtree(self._dir0)

	def write_check_file(self):
		bucket = self._s3_bucket
		key = self._check_file
		s3_hook = S3Hook('aws_conn_s3')
		logger.info(f"Message in check_file is : {self._msg}")
		try:
			d = io.BytesIO(bytearray(self._msg, encoding ='utf-8'))
		except:
			d = io.BytesIO(bytearray(str(self._msg), encoding ='utf-8'))	
		s3_hook.load_file_obj(d, key, bucket_name=bucket, replace=True, encrypt=False)

	def executeETL(self):
		logger.info('########## Beginning Google ETL ##########')

		a = self.supportedDevices()
		logger.info('supportedDevices Files')

		b = self.downloadFiles()
		logger.info('Downloaded Files')

		c = self.processFiles()
		logger.info('Processed Files')

		if a==0 and b==0 and c==0:
			status = self.newData()
			logger.info(f"status: {status}")

		if len(status) == 0:
			self.clean()
			self._msg = 'No New Data'
			self.write_check_file()
			logger.info('### NO New data. Terminating ###')
			return 0
		elif len(status) > 0:
			self._msg = status
			self.write_check_file()
		else:
			self._msg = 'Error'
			self.write_check_file()
		
		logger.info('New Data Detected. Beginning S3 Upload and Redshift with status ' + str(status))
		self.s3Upload(status)
		logger.info('Uploaded New Files To S3')
		self.datesListToDelete(status)
		logger.info('Created list of dates to delete')

		self.clean()
		logger.info('########### Finished Google ETL ###########')

		return 0

def main():

	parser = argparse.ArgumentParser(description="todo")
	parser.add_argument('--config_ini', required=True)
	args = parser.parse_args()
	config_ini = args.config_ini
	aws_creds = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
	google_auth = SecretManagerHook(secret_name='google_auth', region='us-east-1').get_secret()
	logger.info('Loading config')

	a = GoogleCrash(config_ini, aws_creds, google_auth)
	a.executeETL()

if __name__ == '__main__':
	main()
