"""
previous commit done on 2021-Jul-12 commit a6711f93c6d90cf22b15578a2a6e45158bf985f8
"""
import os
import shutil
import airflow
import logging
import json
import ast
from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, date
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from hooks.secret_manager_hook import SecretManagerHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    'owner': 'Exostatic',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': Variable.get("email_alert_list"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=45),
}


def check_for_app_version_crashes(bucket, filename, **kwargs):
    s3_hook = S3Hook('aws_conn_s3')
    logging.info(f"Opening file {filename}")
    val = s3_hook.read_key(filename, bucket_name=bucket)
    
    new_data=False
    if val == "No New Data":
        return "no_new_app_version_crashes"
    elif val.find('Error') == 0:
        return 'error'
    val = ast.literal_eval(val)
    val = [n.strip() for n in val]
    for i in range(0,len(val)):
        if val[i] == 'app_version':
            new_data=True
    if new_data:
        return "new_app_version_crashes"
    else:
        return "no_new_app_version_crashes"


def check_for_device_crashes(bucket, filename, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    logging.info(f"Opening file {filename}")
    val = s3_hook.read_key(filename, bucket_name=bucket)

    new_data=False
    if val == "No New Data":
        return "no_new_device_crashes"
    elif val.find('Error') == 0:
        return 'error'
    val = ast.literal_eval(val)
    val = [n.strip() for n in val]
    for i in range(0,len(val)):
        if val[i] == 'device':
            new_data=True
    if new_data:
        return "new_device_crashes"
    else:
        return "no_new_device_crashes"


def check_for_os_version_crashes(bucket, filename, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    logging.info(f"Opening file {filename}")
    val = s3_hook.read_key(filename, bucket_name=bucket)

    new_data=False
    if val == "No New Data":
        return "no_new_os_version_crashes"
    elif val.find('Error') == 0:
        return 'error'
    val = ast.literal_eval(val)
    val = [n.strip() for n in val]
    for i in range(0,len(val)):
        if val[i] == 'os_version':
            new_data=True
    if new_data:
        return "new_os_version_crashes"
    else:
        return "no_new_os_version_crashes"


def check_for_overview_crashes(bucket, filename, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    logging.info(f"Opening file {filename}")
    val = s3_hook.read_key(filename, bucket_name=bucket)

    new_data=False
    if val == "No New Data":
        return "no_new_overview_crashes"
    elif val.find('Error') == 0:
        return 'error'
    val = ast.literal_eval(val)
    val = [n.strip() for n in val]
    for i in range(0,len(val)):
        if val[i] == 'overview':
            new_data=True
    if new_data:
        return "new_overview_crashes"
    else:
        return "no_new_overview_crashes"

def delete_app_version(bucket, date_file, table, config_ini, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    # Get the files with dates
    date_str = s3_hook.read_key(date_file, bucket_name=bucket)

    try:
        config = json.loads(config_ini)
        #pg_hook = PostgresHook(postgres_conn_id='redshift_test')
        #pg_hook = PostgresHook(postgres_conn_id='vertica_redshift_id')
        for x in range(0, int(config['endpoint_count'])):

            conn_id=config['host'+str(x+1)]
            logging.info("HOST: " + conn_id)
            if conn_id == 'rambo_prod':
                continue

            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            sql = '''DELETE FROM {0} WHERE crash_date IN {1};'''.format(table, date_str)
            pg_hook.run(sql)

    except Exception as e:
        logging.info('Error during Redshift processing {0}'.format(e))
        raise ValueError('Error during Redshift processing')

def delete_device(bucket, date_file, table, config_ini, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    # Get the files with dates
    date_str = s3_hook.read_key(date_file, bucket_name=bucket)

    try:
        config = json.loads(config_ini)
        #pg_hook = PostgresHook(postgres_conn_id='redshift_test')
        #pg_hook = PostgresHook(postgres_conn_id='vertica_redshift_id')
        for x in range(0, int(config['endpoint_count'])):

            conn_id=config['host'+str(x+1)]
            logging.info("HOST: " + conn_id)
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            if conn_id == 'rambo_prod':
                continue

            sql = '''DELETE FROM {0} WHERE crash_date IN {1};'''.format(table, date_str)
            pg_hook.run(sql)

    except Exception as e:
        logging.info('Error during Redshift processing {0}'.format(e))
        raise ValueError('Error during Redshift processing')

def delete_os_version(bucket, date_file, table, config_ini, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    # Get the files with dates
    date_str = s3_hook.read_key(date_file, bucket_name=bucket)

    try:
        config = json.loads(config_ini)
        #pg_hook = PostgresHook(postgres_conn_id='redshift_test')
        #pg_hook = PostgresHook(postgres_conn_id='vertica_redshift_id')
        for x in range(0, int(config['endpoint_count'])):

            conn_id=config['host'+str(x+1)]
            logging.info("HOST: " + conn_id)
            if conn_id == 'rambo_prod':
                continue
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            sql = '''DELETE FROM {0} WHERE crash_date IN {1};'''.format(table, date_str)
            pg_hook.run(sql)

    except Exception as e:
        logging.info('Error during Redshift processing {0}'.format(e))
        raise ValueError('Error during Redshift processing')

def delete_overview(bucket, date_file, table, config_ini, **kwargs):

    s3_hook = S3Hook('aws_conn_s3')
    # Get the files with dates
    date_str = s3_hook.read_key(date_file, bucket_name=bucket)

    try:
        config = json.loads(config_ini)
        #pg_hook = PostgresHook(postgres_conn_id='redshift_test')
        #pg_hook = PostgresHook(postgres_conn_id='vertica_redshift_id')
        for x in range(0, int(config['endpoint_count'])):

            conn_id=config['host'+str(x+1)]
            logging.info("HOST: " + conn_id)
            if conn_id == 'rambo_prod':
                continue
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            sql = '''DELETE FROM {0} WHERE crash_date IN {1};'''.format(table, date_str)
            pg_hook.run(sql)

    except Exception as e:
        logging.info('Error during Redshift processing {0}'.format(e))
        raise ValueError('Error during Redshift processing')

def delete_supported_devices(table, config_ini, **kwargs):

    try:
        config = json.loads(config_ini)

        for x in range(0, int(config['endpoint_count'])):

            conn_id=config['host'+str(x+1)]
            logging.info("HOST: " + conn_id)
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            if conn_id == 'rambo_prod':
                table = config['supported_devices_schema_table_rambo']

            sql = '''DELETE FROM {0};'''.format(table)

            pg_hook.run(sql)

    except Exception as e:
        logging.info('Error during Redshift processing {0}'.format(e))
        raise ValueError('Error during Redshift processing')

def redshiftCopyCrashAppVersion(config_ini, **kwargs):
    logging.info('Starting redshiftCopyCrashAppVersion')
    config = json.loads(config_ini)
    conf = {}
    conf['date'] = date.today().strftime('%Y-%m-%d')

    # For every endpoint, perform a manifest copy (and clear old data)
    for x in range(0, int(config['endpoint_count'])):

        conn_id=config['host'+str(x+1)]
        logging.info("HOST: " + conn_id)
        if conn_id == 'rambo_prod':
            continue
        pg_hook = PostgresHook(postgres_conn_id=conn_id)

        aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
        sql = """COPY {0} ({1}) FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ','
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        {5};
        ANALYZE {6} PREDICATE COLUMNS;
        """.format(config['app_version_schema_and_table'],
            config['app_version_columns'],
            's3://' + config['s3_bucket'] + config['s3_folder_root'] + 'crashes/manifest/app_version/' + conf['date'] + '/manifest_app_version_' + conf['date'] + '.txt',
            aws_cred['access_key_id'],
            aws_cred['secret_access_key'],
            "CSV QUOTE AS '%'",
            config['app_version_schema_and_table'])
        
        
        logging.info('Copy: ' + sql)

        try:
            pg_hook.run(sql)
        except Exception as e:
            logging.exception('Redshift COPY CrashAppVersion failed')
            raise ValueError('Redshift COPY CrashAppVersion failed')

def redshiftCopyCrashDevice(config_ini, **kwargs):
    logging.info('Starting redshiftCopyCrashDevice')
    config = json.loads(config_ini)
    conf = {}
    conf['date'] = date.today().strftime('%Y-%m-%d')

    # For every endpoint, perform a manifest copy (and clear old data)
    for x in range(0, int(config['endpoint_count'])):

        conn_id=config['host'+str(x+1)]
        logging.info("HOST: " + conn_id)
        if conn_id == 'rambo_prod':
            continue
        pg_hook = PostgresHook(postgres_conn_id=conn_id)

        aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
        sql = """COPY {0} ({1}) FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ','
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        {5};
        ANALYZE {6} PREDICATE COLUMNS;
        """.format(config['device_schema_and_table'],
            config['device_columns'],
            's3://' + config['s3_bucket'] + config['s3_folder_root'] + 'crashes/manifest/device/' + conf['date'] + '/manifest_device_' + conf['date'] + '.txt',
            aws_cred['access_key_id'],
            aws_cred['secret_access_key'],
            "CSV QUOTE AS '%'",
            config['device_schema_and_table'])


        logging.info('Copy: ' + sql)

        try:
            pg_hook.run(sql)
        except Exception as e:
            logging.exception('Redshift COPY CrashDevice failed')
            raise ValueError('Redshift COPY CrashDevice failed')

def redshiftCopyCrashOsVersion(config_ini, **kwargs):
    logging.info('Starting redshiftCopyCrashOsVersion')
    config = json.loads(config_ini)
    conf = {}
    conf['date'] = date.today().strftime('%Y-%m-%d')

    # For every endpoint, perform a manifest copy (and clear old data)
    for x in range(0, int(config['endpoint_count'])):

        conn_id=config['host'+str(x+1)]
        logging.info("HOST: " + conn_id)
        if conn_id == 'rambo_prod':
            continue
        pg_hook = PostgresHook(postgres_conn_id=conn_id)

        aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
        sql = """COPY {0} ({1}) FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ','
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        {5};
        ANALYZE {6} PREDICATE COLUMNS;
        """.format(config['os_version_schema_and_table'],
            config['os_version_columns'],
            's3://' + config['s3_bucket'] + config['s3_folder_root'] + 'crashes/manifest/os_version/' + conf['date'] + '/manifest_os_version_' + conf['date'] + '.txt',
            aws_cred['access_key_id'],
            aws_cred['secret_access_key'],
            "CSV QUOTE AS '%'",
            config['os_version_schema_and_table'])

        logging.info('Copy: ' + sql)

        try:
            pg_hook.run(sql)
        except Exception as e:
            logging.exception('Redshift COPY CrashOsVersion failed')
            raise ValueError('Redshift COPY CrashOsVersion failed')

def redshiftCopyCrashOverview(config_ini, **kwargs):
    logging.info('Starting redshiftCopyCrashOverview')
    config = json.loads(config_ini)
    conf = {}
    conf['date'] = date.today().strftime('%Y-%m-%d')

    # For every endpoint, perform a manifest copy (and clear old data)
    for x in range(0, int(config['endpoint_count'])):

        conn_id=config['host'+str(x+1)]
        logging.info("HOST: " + conn_id)
        if conn_id == 'rambo_prod':
            continue
        pg_hook = PostgresHook(postgres_conn_id=conn_id)

        aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
        sql = """COPY {0} ({1}) FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ','
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        {5};
        ANALYZE {6} PREDICATE COLUMNS;
        """.format(config['overview_schema_and_table'],
            config['overview_columns'],
            's3://' + config['s3_bucket'] + config['s3_folder_root'] + 'crashes/manifest/overview/' + conf['date'] + '/manifest_overview_' + conf['date'] + '.txt',
            aws_cred['access_key_id'],
            aws_cred['secret_access_key'],
            "CSV QUOTE AS '%'",
            config['overview_schema_and_table'])

        logging.info('Copy: ' + sql)

        try:
            pg_hook.run(sql)
        except Exception as e:
            logging.exception('Redshift COPY CrashOverview failed')
            raise ValueError('Redshift COPY CrashOverview failed')

def redshiftCopySupportedDevices(config_ini, **kwargs):
    logging.info('Starting redshiftCopySupportedDevices')
    config = json.loads(config_ini)

    # For every endpoint, perform a manifest copy (and clear old data)
    for x in range(0, int(config['endpoint_count'])):

        conn_id=config['host'+str(x+1)]
        logging.info("HOST: " + conn_id)
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        
        if conn_id == 'rambo_prod':
            supported_devices_schema_and_table = config['supported_devices_schema_table_rambo']
        else:
            supported_devices_schema_and_table=config['supported_devices_schema_and_table']

        aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()
        sql = """COPY {0} ({1}) FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        IGNOREHEADER 1
        CSV QUOTE '"'
        DELIMITER ',';
        ANALYZE {5} PREDICATE COLUMNS;
        """.format(supported_devices_schema_and_table,
            config['supported_devices_columns'],
            's3://' + config['s3_bucket'] + config['s3_folder_root'] + 'crashes/supported_devices/supported_devices_manifest.txt',
            aws_cred['access_key_id'],
            aws_cred['secret_access_key'],
            supported_devices_schema_and_table)

        logging.info('Copy: ' + sql)

        try:
            pg_hook.run(sql)
        except Exception as e:
            logging.exception('Redshift COPY SupportedDevices failed')
            raise ValueError('Redshift COPY SupportedDevices failed')

def clean_up():
    """
    Cleans local drive

    Params
    ------
    folder
        string with the path
    Return
    ------
    True indicating successful
    """
    d =  Variable.get("google_sales_config", deserialize_json=True)['file_directory']
    if os.path.exists(d):
        shutil.rmtree(d)
        logging.info("Directory is clean")
    else:
        logging.info("Directory does not exist")

dag = DAG(
    'google_crashes',
    catchup=False,
    default_args=default_args,
    schedule_interval='0 15 * * *')

delete_app_version = PythonOperator(
        task_id='delete_app_version',
        python_callable=delete_app_version,
        provide_context=True,
        op_kwargs={ 'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
                   ,'date_file':(json.loads(Variable.get("google_crashes_config")))['app_version_date_file']
                   ,'table':(json.loads(Variable.get("google_crashes_config")))['app_version_schema_and_table']
                   ,'config_ini':Variable.get("google_crashes_config")
                  },
        dag=dag)

delete_device = PythonOperator(
        task_id='delete_device',
        python_callable=delete_device,
        provide_context=True,
        op_kwargs={ 'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
                   ,'date_file':(json.loads(Variable.get("google_crashes_config")))['device_date_file']
                   ,'table':(json.loads(Variable.get("google_crashes_config")))['device_schema_and_table']
                   ,'config_ini':Variable.get("google_crashes_config")
                  },
        dag=dag)

delete_os_version = PythonOperator(
        task_id='delete_os_version',
        python_callable=delete_os_version,
        provide_context=True,
        op_kwargs={ 'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
                   ,'date_file':(json.loads(Variable.get("google_crashes_config")))['os_version_date_file']
                   ,'table':(json.loads(Variable.get("google_crashes_config")))['os_version_schema_and_table']
                   ,'config_ini':Variable.get("google_crashes_config")
                  },
        dag=dag)

delete_overview = PythonOperator(
        task_id='delete_overview',
        python_callable=delete_overview,
        provide_context=True,
        op_kwargs={ 'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
                   ,'date_file':(json.loads(Variable.get("google_crashes_config")))['overview_date_file']
                   ,'table':(json.loads(Variable.get("google_crashes_config")))['overview_schema_and_table']
                   ,'config_ini':Variable.get("google_crashes_config")
                  },
        dag=dag)

delete_supported_devices = PythonOperator(
        task_id='delete_supported_devices',
        python_callable=delete_supported_devices,
        provide_context=True,
        op_kwargs={ 'table':(json.loads(Variable.get("google_crashes_config")))['supported_devices_schema_and_table']
                   ,'config_ini':Variable.get("google_crashes_config")
                  },
        dag=dag)

run_google_crashes = BashOperator(
    task_id='run_google_crashes',
    bash_command='python3 {{params.local_path}}etl_google_crashes.py --config_ini \'{{params.config_ini}}\' ',
    params={'local_path': (json.loads(Variable.get("google_crashes_config")))['local_path']
            ,'config_ini': Variable.get("google_crashes_config")
    },
    dag=dag)

CopyCrashAppVersion = PythonOperator(
        task_id='redshiftCopyCrashAppVersion',
        python_callable=redshiftCopyCrashAppVersion,
        provide_context=True,
        op_kwargs={'config_ini':Variable.get("google_crashes_config")},
        dag=dag)

CopyCrashDevice = PythonOperator(
        task_id='redshiftCopyCrashDevice',
        python_callable=redshiftCopyCrashDevice,
        provide_context=True,
        op_kwargs={'config_ini':Variable.get("google_crashes_config")},
        dag=dag)

CopyCrashOsVersion = PythonOperator(
        task_id='redshiftCopyCrashOsVersion',
        python_callable=redshiftCopyCrashOsVersion,
        provide_context=True,
        op_kwargs={'config_ini':Variable.get("google_crashes_config")},
        dag=dag)

CopyCrashOverview = PythonOperator(
        task_id='redshiftCopyCrashOverview',
        python_callable=redshiftCopyCrashOverview,
        provide_context=True,
        op_kwargs={'config_ini':Variable.get("google_crashes_config")},
        dag=dag)

CopySupportedDevices = PythonOperator(
        task_id='redshiftCopySupportedDevices',
        python_callable=redshiftCopySupportedDevices,
        provide_context=True,
        op_kwargs={'config_ini':Variable.get("google_crashes_config")},
        dag=dag)


check_for_app_version_crashes = BranchPythonOperator(
    task_id='check_for_app_version_crashes',
    python_callable=check_for_app_version_crashes,
    op_kwargs={'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
              ,'filename':(json.loads(Variable.get("google_crashes_config")))['check_file']
			  ,'crashes_type':'app_version'},
    dag=dag)
check_for_device_crashes = BranchPythonOperator(
    task_id='check_for_device_crashes',
    python_callable=check_for_device_crashes,
    op_kwargs={'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
              ,'filename':(json.loads(Variable.get("google_crashes_config")))['check_file']
			  ,'crashes_type':'device'},
    dag=dag)
check_for_os_version_crashes = BranchPythonOperator(
    task_id='check_for_os_version_crashes',
    python_callable=check_for_os_version_crashes,
    op_kwargs={'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
              ,'filename':(json.loads(Variable.get("google_crashes_config")))['check_file']
			  ,'crashes_type':'os_version'},
    dag=dag)
check_for_overview_crashes = BranchPythonOperator(
    task_id='check_for_overview_crashes',
    python_callable=check_for_overview_crashes,
    op_kwargs={'bucket': (json.loads(Variable.get("google_crashes_config")))['s3_bucket']
              ,'filename':(json.loads(Variable.get("google_crashes_config")))['check_file']
			  ,'crashes_type':'overview'},
    dag=dag)

no_new_app_version_crashes = DummyOperator (
        task_id='no_new_app_version_crashes',
        dag = dag)

new_app_version_crashes = DummyOperator (
        task_id='new_app_version_crashes',
        dag = dag)

no_new_device_crashes = DummyOperator (
        task_id='no_new_device_crashes',
        dag = dag)

new_device_crashes = DummyOperator (
        task_id='new_device_crashes',
        dag = dag)

no_new_os_version_crashes = DummyOperator (
        task_id='no_new_os_version_crashes',
        dag = dag)

new_os_version_crashes = DummyOperator (
        task_id='new_os_version_crashes',
        dag = dag)

no_new_overview_crashes = DummyOperator (
        task_id='no_new_overview_crashes',
        dag = dag)

new_overview_crashes = DummyOperator (
        task_id='new_overview_crashes',
        dag = dag)

error = DummyOperator (
        task_id='error',
        dag = dag)

clean_up = PythonOperator(task_id='clean_up', python_callable=clean_up, dag=dag)

run_google_crashes.set_upstream(clean_up)

delete_supported_devices.set_upstream(run_google_crashes)
delete_supported_devices.set_downstream(CopySupportedDevices)

check_for_app_version_crashes.set_upstream(run_google_crashes)
check_for_app_version_crashes.set_downstream(no_new_app_version_crashes)
check_for_app_version_crashes.set_downstream(new_app_version_crashes)
check_for_app_version_crashes.set_downstream(error)
new_app_version_crashes.set_downstream(delete_app_version)
delete_app_version.set_downstream(CopyCrashAppVersion)

check_for_device_crashes.set_upstream(run_google_crashes)
check_for_device_crashes.set_downstream(no_new_device_crashes)
check_for_device_crashes.set_downstream(new_device_crashes)
check_for_device_crashes.set_downstream(error)
new_device_crashes.set_downstream(delete_device)
delete_device.set_downstream(CopyCrashDevice)

check_for_os_version_crashes.set_upstream(run_google_crashes)
check_for_os_version_crashes.set_downstream(no_new_os_version_crashes)
check_for_os_version_crashes.set_downstream(new_os_version_crashes)
check_for_os_version_crashes.set_downstream(error)
new_os_version_crashes.set_downstream(delete_os_version)
delete_os_version.set_downstream(CopyCrashOsVersion)

check_for_overview_crashes.set_upstream(run_google_crashes)
check_for_overview_crashes.set_downstream(no_new_overview_crashes)
check_for_overview_crashes.set_downstream(new_overview_crashes)
check_for_overview_crashes.set_downstream(error)
new_overview_crashes.set_downstream(delete_overview)
delete_overview.set_downstream(CopyCrashOverview)
