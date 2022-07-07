"""
previous commit done on 2021-Feb-24 commit 197393b8700c7ed4f3db6b00f1e2de7b85a57f4e
"""
import airflow
import logging
import os
import fnmatch 
import json
from datetime import datetime, timedelta, date
from dateutil import relativedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from hooks.secret_manager_hook import SecretManagerHook
from airflow.hooks.S3_hook import S3Hook


default_args = {
    'owner': 'Exostatic',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': Variable.get("email_alert_list"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_for_file(local_path, filename, **kwargs):
    wb_found=False
    playdemic_found=False
#     wb_file = local_path + '/wb/earnings/' + filename
#     playdemic_file = local_path + '/playdemic/earnings/' + filename
#     logging.info("Checking if files are present, {0} and {1}", wb_file, playdemic_file)
#     wb_found=os.path.isfile(wb_file)
#     playdemic_found= playdemic_file
    wb_files = fnmatch.filter(os.listdir(local_path + '/wb/earnings/'), filename)
    playdemic_files = fnmatch.filter(os.listdir(local_path + '/playdemic/earnings/'), filename)
    
    for f in wb_files:
        if os.path.isfile(os.path.join(local_path+ '/wb/earnings/',f)):
            wb_found = True
    
    for f in playdemic_files:
        if os.path.isfile(os.path.join(local_path+ '/playdemic/earnings/',f)):
            playdemic_found = True

    if wb_found and playdemic_found:
        return "file_found"
    else:
        return "file_not_found"


def build_manifest(local_path, s3_bucket, s3_prefix, bucket, yearmonth, bucket_subfolder, manifest_file, file_filter, **kwargs):
    manifest_file = local_path + "/" + manifest_file
    manifest_entries = []
    with open(manifest_file, "w") as manifest:
        manifest.write("{\n")
        manifest.write("  \"entries\": [\n")

        for i,l in enumerate(["    {\"url\":\"s3://"+s3_bucket+"/"+s3_prefix+"/earnings/"+bucket+"/"+yearmonth+"/"+bucket_subfolder+"/"+file+"\"}" for file in fnmatch.filter(os.listdir(local_path), '*' + file_filter+'*.csv')]):
            if i>0:
                manifest.write(",")
            manifest.write("\n"+str(l))
        manifest.write("  ]\n")
        manifest.write("}\n")


def s3_upload(local_path, s3_bucket, s3_prefix, bucket, yearmonth, bucket_subfolder, manifest, **kwargs):

    logging.info("Running dag ************************")
    dest_s3 = S3Hook('aws_conn_s3')
    bucket_name = s3_bucket
    logging.info("Checking connection and bucket S3")
    if not dest_s3.check_for_bucket(bucket_name):
      raise AirflowException("The bucket key {0} does not exist".format(bucket_name))
    else:
        logging.info("Found S3 bucket {0}".format(bucket_name))
    logging.info("Uploading files now")
    dirs = os.listdir(local_path)

    for file in dirs:
        if file.endswith('.csv'):
            dest_s3.load_file(
                filename=local_path + "/" + file,
                bucket_name=bucket_name,
                key=s3_prefix + '/earnings/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+file,
                replace=True,
                encrypt=True
            )
    logging.info("Uploading manifest files")

    for i in range(0,len(manifest.split(','))):
        dest=s3_prefix + '/earnings/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+manifest.split(',')[i]
        logging.info("Uploading manifest file: {0}".format(manifest.split(',')[i]))
        logging.info("destination: {0}".format(dest))
        dest_s3.load_file(
            filename=local_path + "/" + manifest.split(',')[i],
            bucket_name=bucket_name,
            key=s3_prefix + '/earnings/'+bucket+'/'+yearmonth+'/'+bucket_subfolder+'/'+manifest.split(',')[i],
            replace=True,
            encrypt=True
        )
    logging.info("S3 upload completed")


dag = DAG(
    'google_earnings',
    default_args=default_args,
    schedule_interval='0 8 * * *')


# check if it is the 1st, 2nd or 3rd, so we know to reload the previous month
def is_end_of_month():
    if datetime.today().day in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20):
        print("end of the month")
        return True
    else:
        print("Not the end of the month")
        return False


check_end_of_month = ShortCircuitOperator(
    task_id='check_end_of_month',
    python_callable=is_end_of_month,
    dag=dag)


branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_for_file,
    op_kwargs={'local_path': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['data_folder']
      ,'filename':'PlayApps_' + (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m') + '_*.csv'},
    dag=dag)


file_found = DummyOperator (
        task_id='file_found',
        dag = dag)


file_not_found = DummyOperator (
        task_id='file_not_found',
    trigger_rule='one_success',
        dag = dag)


end = DummyOperator(
        task_id='END',
        trigger_rule='all_done',
        dag=dag)


clear_folders = BashOperator(
        task_id='clean_up',
        bash_command='rm -f /{{params.local_path_wb}}/*|rm -f /{{params.local_path_playdemic}}/*',
        params = {'local_path_wb': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['local_path']
                , 'local_path_playdemic': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['local_path']},
        dag=dag)


end_clear_folders = BashOperator(
        task_id='clean_up_end',
        bash_command='rm -f /{{params.local_path_wb}}/*|rm -f /{{params.local_path_playdemic}}/*',
        params = {'local_path_wb': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['local_path']
                , 'local_path_playdemic': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['local_path']},
        dag=dag)


playdemic_download_lastmonth = BashOperator(
    task_id='google_pos_s3_playdemic_lastmonth',
    bash_command='python3 /{{params.script_folder}}/etl_google_earnings.py --yearmonth {{params.yearmonth}} --month_back {{params.month_back}} --bucket {{params.bucket}} --refresh_token {{params.refresh_token}} --file_folder /{{params.file_folder}}',
    params={ 'yearmonth': 'lastmonth'
            ,'month_back':  (json.loads(Variable.get("google_earnings")))['month_back']
            ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['bucket']
            ,'refresh_token': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['refresh_token']
            ,'file_folder': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['file_folder']
            ,'script_folder': (json.loads(Variable.get("google_earnings")))['script_folder']
    },
    dag=dag)


wb_download_lastmonth = BashOperator(
    task_id='google_pos_s3_wb_lastmonth',
    bash_command='python3 /{{params.script_folder}}/etl_google_earnings.py --yearmonth {{params.yearmonth}} --month_back {{params.month_back}} --bucket {{params.bucket}} --refresh_token {{params.refresh_token}} --file_folder /{{params.file_folder}}',
    params={ 'yearmonth': 'lastmonth'
            ,'month_back':  (json.loads(Variable.get("google_earnings")))['month_back']
            ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['bucket']
            ,'refresh_token': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['refresh_token']
            ,'file_folder': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['file_folder']
            ,'script_folder': (json.loads(Variable.get("google_earnings")))['script_folder']
    },
    dag=dag)


s3_upload_wb_earnings = PythonOperator(
        task_id='s3_upload_wb_earnings',
        python_callable=s3_upload,
        provide_context=True,
        op_kwargs={ 'local_path': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['local_path']
            ,'s3_bucket':(json.loads(Variable.get("google_earnings")))['s3_bucket']
            ,'s3_prefix':(json.loads(Variable.get("google_earnings")))['s3_prefix']
            ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['bucket']
            ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
            ,'bucket_subfolder':(json.loads(Variable.get("google_earnings")))['google_earnings_wb']['bucket_subfolder']
            ,'manifest':(json.loads(Variable.get("google_earnings")))['google_earnings_wb']['manifest']
        },
        dag=dag)


s3_upload_playdemic_earnings = PythonOperator(
        task_id='s3_upload_playdemic_earnings',
        python_callable=s3_upload,
        provide_context=True,
op_kwargs={ 'local_path': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['local_path']
            ,'s3_bucket':(json.loads(Variable.get("google_earnings")))['s3_bucket']
            ,'s3_prefix':(json.loads(Variable.get("google_earnings")))['s3_prefix']
            ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['bucket']
            ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
            ,'bucket_subfolder':(json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['bucket_subfolder']
            ,'manifest':(json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['manifest']
          },
        dag=dag)


aws_cred = SecretManagerHook(secret_name='aws_creds_AnalyticsZM', region='us-east-1').get_secret()


rs_load_wb_earnings = PostgresOperator(
        task_id='load_wb_earning_to_redshift',
        postgres_conn_id='vertica_redshift_id',
        sql="""COPY wbie_external_sales.google_earnings (description,transaction_date,transaction_time,tax_type,transaction_type,refund_type,product_title,product_id,product_type,sku_id,hardware,buyer_country,buyer_state,buyer_postal_code,buyer_currency,amount_buyer_currency,currency_conversion_rate,merchant_currency,amount_merchant_currency)
        FROM 's3://{{ params.product.s3_bucket }}/{{ params.product.s3_prefix }}/earnings/{{ params.product.google_earnings_wb.bucket }}/{{ params.yearmonth }}/earnings/manifest_earnings.txt'
        CREDENTIALS 'aws_access_key_id={{ params.aws_cred.access_key_id }};aws_secret_access_key={{ params.aws_cred.secret_access_key }}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ',' CSV QUOTE '"'
        IGNOREHEADER 1
        DATEFORMAT 'auto'""",
        params={'aws_cred': aws_cred,
               'product': Variable.get("google_earnings", deserialize_json=True),
               'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
        },
        retries=1, retry_delay=timedelta(0, 60),
        dag=dag)


rs_load_playdemic_earnings = PostgresOperator(
        task_id='load_playdemic_earning_to_redshift',
        postgres_conn_id='vertica_redshift_id',
        sql="""COPY wbie_external_sales.google_earnings (description,transaction_date,transaction_time,tax_type,transaction_type,refund_type,product_title,product_id,product_type,sku_id,hardware,buyer_country,buyer_state,buyer_postal_code,buyer_currency,amount_buyer_currency,currency_conversion_rate,merchant_currency,amount_merchant_currency)
        FROM 's3://{{ params.product.s3_bucket }}/{{ params.product.s3_prefix }}/earnings/{{ params.product.google_earnings_playdemic.bucket }}/{{ params.yearmonth }}/earnings/manifest_earnings.txt'
        CREDENTIALS 'aws_access_key_id={{ params.aws_cred.access_key_id }};aws_secret_access_key={{ params.aws_cred.secret_access_key }}'
        MANIFEST
        EMPTYASNULL
        BLANKSASNULL
        DELIMITER ',' CSV QUOTE '"'
        IGNOREHEADER 1
        DATEFORMAT 'auto'""",
        params={'aws_cred': aws_cred,
               'product': Variable.get("google_earnings", deserialize_json=True),
               'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
        },
        retries=1, retry_delay=timedelta(0, 60),
        dag=dag)


rs_delete_earnings = PostgresOperator(
        task_id='rs_delete_earnings',
        postgres_conn_id='vertica_redshift_id',
        sql="DELETE FROM wbie_external_sales.google_earnings WHERE transaction_date like '" + (datetime.today() + relativedelta.relativedelta(day=1, days=-1)).strftime('%b') + "%" + (datetime.today() + relativedelta.relativedelta(day=1, days=-1)).strftime('%Y') + "'",
        dag=dag)


wb_build_manifest_earnings = PythonOperator(
        task_id='wb_build_manifest_earnings',
        python_callable=build_manifest,
        provide_context=True,
        op_kwargs={ 'local_path': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['local_path']
                    ,'s3_bucket':(json.loads(Variable.get("google_earnings")))['s3_bucket']
                    ,'s3_prefix':(json.loads(Variable.get("google_earnings")))['s3_prefix']
                    ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['bucket']
                    ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
                    ,'bucket_subfolder':(json.loads(Variable.get("google_earnings")))['google_earnings_wb']['bucket_subfolder']
                    ,'manifest_file': (json.loads(Variable.get("google_earnings")))['google_earnings_wb']['manifest']
                    ,'file_filter': 'PlayApps_' + (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
                  },
        dag=dag)


playdemic_build_manifest_earnings = PythonOperator(
        task_id='playdemic_build_manifest_earnings',
        python_callable=build_manifest,
        provide_context=True,
        op_kwargs={ 'local_path': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['local_path']
                    ,'s3_bucket':(json.loads(Variable.get("google_earnings")))['s3_bucket']
                    ,'s3_prefix':(json.loads(Variable.get("google_earnings")))['s3_prefix']
                    ,'bucket': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['bucket']
                    ,'yearmonth': (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
                    ,'bucket_subfolder':(json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['bucket_subfolder']
                    ,'manifest_file': (json.loads(Variable.get("google_earnings")))['google_earnings_playdemic']['manifest']
                    ,'file_filter': 'PlayApps_' + (date.today() + relativedelta.relativedelta(months=-(json.loads(Variable.get("google_earnings")))['month_back'])).strftime('%Y%m')
                  },
        dag=dag)


end_clear_folders.set_upstream(rs_load_playdemic_earnings)
end_clear_folders.set_upstream(rs_load_wb_earnings)
end_clear_folders.set_upstream(end)

rs_load_playdemic_earnings.set_upstream(rs_delete_earnings)
rs_delete_earnings.set_upstream(s3_upload_playdemic_earnings)
s3_upload_playdemic_earnings.set_upstream(playdemic_build_manifest_earnings)
playdemic_build_manifest_earnings.set_upstream(file_found)

rs_load_wb_earnings.set_upstream(rs_delete_earnings)
rs_delete_earnings.set_upstream(s3_upload_wb_earnings)
s3_upload_wb_earnings.set_upstream(wb_build_manifest_earnings)
wb_build_manifest_earnings.set_upstream(file_found)

branch.set_downstream(file_found)
branch.set_downstream(file_not_found)
file_not_found.set_downstream(end)

branch.set_upstream(wb_download_lastmonth)
branch.set_upstream(playdemic_download_lastmonth)
wb_download_lastmonth.set_upstream(check_end_of_month)
playdemic_download_lastmonth.set_upstream(check_end_of_month)

check_end_of_month.set_upstream(clear_folders)