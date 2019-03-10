# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.



import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.hooks.mysql_hook import MySqlHook
import logging

# [END howto_operator_cloudsql_query_arguments]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'dlademo-1', default_args=default_args, schedule_interval=timedelta(1))
dag.doc_md = __doc__


t1 = BashOperator(
    task_id='print_date',
    bash_command='echo hello-airflow',
    dag=dag)

items = []
params = ''

def step2(ds, **kargs):
    mysql_hook = MySqlHook(mysql_conn_id = 'cloudsql-test')
    items = mysql_hook.get_records("SELECT policyID FROM sample_db_1.SAMPLE_TABLE_4  limit 20")
    # mysql_hook = MySqlHook(mysql_conn_id='local_mysql')
    # items = mysql_hook.get_records("SELECT samepleid FROM cloudtest.SAMPLE_TABLE_5 limit 20")
    mail_list = []

    for r in items:
        # print 'mail:%s ' % r
        mail_list.append('%d' % r)
    # print(mail_list)
    params = ','.join(mail_list)
    logging.info("params:{}".format(params))

t2 = PythonOperator(
    task_id='execute_dla_sql',
    provide_context=True,
    python_callable=step2,
    dag=dag)

def step3(ds, **kargs):
    logging.info("step3_params:".format(params))
    mysql_hook = MySqlHook(mysql_conn_id = 'cloudsql-test')
    mysql_hook.run("update sample_db_1.SAMPLE_TABLE_4 set statecode = 'FLLL' where policyID in ({})".format(params), True)

    # mysql_hook = MySqlHook(mysql_conn_id='local_mysql')
    # mysql_hook.run("update cloudtest.SAMPLE_TABLE_5 set statecode = 'FLLL' where policyID in ({})".format(params), True)

t3 = PythonOperator(
    task_id='update_dla_sql',
    provide_context=True,
    python_callable=step3,
    dag=dag)

t1 >> t2 >> t3

# [END howto_operator_cloudsql_query_operators]
