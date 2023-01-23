#1. Import Block 

from datetime import timedelta
from airflow import DAG # -> instantiate a DAG 
from airflow.operators.bash_operator import BashOperator #-> Writting Task 
from airflow.utils.dates import days_ago #-> make scheduling easy 

# 2. Defining DAG Arguments

## The format 
    # owner: the name of the workflow owner should be alphanumeric. It can have underscores but not spaces.
    # email: if a task fails for whatever reason, you’ll get an email.
    # start_date: start date of your workflow.
    # depends_on_past: if you run your workflow, the data depends upon the past run, then mark it as True otherwise, mark it as False.
    # retry_delay: time to wait to retry a task if any task fails.

defaults_args = {
    'owner' : 'shafiq',
    'start_date' : days_ago(0),
    'email' : 'muhdshafiqsafian@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}
#3. Python function Definition Block
# Now we’ll create a DAG object and pass the dag_id, which is the name of the DAG. Make sure you haven’t already established a DAG with this name.
# Add a description and schedule interval to the previously created input, and the DAG will execute after the specified time interval.
source  = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'

dag = DAG(
    'ETL_toll_data',
    default_args= defaults_args ,
    description='Scheduler to ingest toll data into db ',
    schedule_interval=timedelta(days=1)
)

#4. Task Definition Block 
##Formatting for bashoperator 
# # run_this = BashOperator(
#     task_id="run_after_loop",
#     bash_command="echo 1",

download = BashOperator(
    task_id="Download_Source",
     bash_command = "wget source",
     dag=dag,
)

# The extract task must extract the fields timestamp and visitorid.

extract = BashOperator(
    task_id = 'extract',
    bash_command='cut -d:" -f1,4 /web-server-access-log.txt >/home/project/airflow/dags/extracted.txt ',
    dag=dag,
)

# The transform task must capitalize the visitorid.

transform = BashOperator( 
    task_id = 'Capitalize',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt',
    dag=dag
)

# The load task must compress the extracted and transformed data.

load = BashOperator(
    task_id='zipping',
    bash_command = 'zip log.zip capitalized.txt',
    dag=dag
)

#Task Pipeline 

download >> extract >> transform >> load 
