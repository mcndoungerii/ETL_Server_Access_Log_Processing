from datetime import timedelta

import time

from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago

#defining DAG arguements

default_args = {
    'owner': 'Ndunga JR',
    'start_date': days_ago(0),
    'email': ['allenmcndounger@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#DAG Definitions

dag=DAG(
    'dag_id'='ETL_Server_Access_Log_Processing',
    'default_args'=default_args,
    'description'='This is ETL DAG for Server Access Log Processing',
    'schedule_interval'=timedelta(days=1)
)

# Task Definitions

# define the task 'download'
# Download task from internet
download = BashOperator(
    task_id='download',
    bash_command='wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"'
    dag=dag
)

# define the task 'extract'
# must extract the fields timestamp and visitorid.
extract = BashOperator(
    task_id='extract',
    bash_command='cut -f1,4 -d"#" web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag=dag
)

# define the task 'transform'
# must capitalize the visitorid.
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt',
    dag=dag
)

# define the task 'load'
# must compress the extracted and transformed data
load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt'
    dag=dag
)

# Task Pipeline
download >> extract >> transform >> load