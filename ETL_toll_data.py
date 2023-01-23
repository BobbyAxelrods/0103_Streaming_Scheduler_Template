# cd airflow/dags
# sudo mkdir finalassignment

# cd finalassignment

# sudo mkdir staging

# cd staging

#1. Definging Python Import Block 

from datetime import timedelta
from airflow import DAG # -> instantiate a DAG 
from airflow.operators.bash_operator import BashOperator #-> Writting Task 
from airflow.utils.dates import days_ago #-> make scheduling easy 

# 2. Defining DAG Arguments {dag_args.jpg}

## The format 
    # owner: the name of the workflow owner should be alphanumeric. It can have underscores but not spaces.
    # email: if a task fails for whatever reason, you’ll get an email.
    # start_date: start date of your workflow.
    # depends_on_past: if you run your workflow, the data 
    # depends upon the past run, then mark it as True otherwise, mark it as False.
    # retry_delay: time to wait to retry a task if any task fails.

defaults_args ={
    'owner' : 'bobby_axelrod',
    'start_date' : days_ago(0),
    'email' : 'bobbyaxelrod@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

#3. Python function Definition Block
# Now we’ll create a DAG object and pass the dag_id, which is the name of the DAG. Make sure you haven’t already established a DAG with this name.
# Add a description and schedule interval to the previously created input, and the DAG will execute after the specified time interval.

# Task 1.2 Creating DAG Definition Blocks {dag_definition.jpg.}
dag = DAG(
    'ETL_toll_data',
    default_args= defaults_args ,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1)
)
#Highlight data link source to download 

source='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'

path_save = '/home/project/airflow/dags/finalassignment'

#4. Task 1.3 Creating task to download data to specific folder 

download = BashOperator(
    task_id="download data",
    bash_command = "wget -0 path_save source",
    dag=dag
)

# Task 1.4 Task to extract data from csv files
# x for extract
# v for verbose
# z for gnuzip
# f for file, should come at last just before file name.

unzip = BashOperator(
    task_id = 'unzip_data',
    bash_command='tar -xvzf path_save tolldata.tgz',
    dag=dag
)

extract_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command='cut -d:'' -f1,2,4,6 < /path_save/vehicle-data.csv > path_save/csv_data',
    dag=dag
)

# Task 1.5 Task to exteact data from tsv files 
# This task should extract the fields 
# Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type
#  from the vehicle-data.csv file and save them into a file named csv_data.csv.

extract_tsv = BashOperator( 
    task_id ='extract_data_from_tsv',
    bash_command = 'cut -d:'' -f1,5,7 < /path_save/tollplaza-data.tsv > path_save/tsv_data.csv',
    dag=dag
)

# Task 1.6 Creating task to extract data from fixed width file 

extract_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -d:''-f7,9 < payment-data.txt > fixed_width_data.csv',
    dag=dag
)
# Task 1.7 Create a task to consolidate data extracted from previous task 

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d '' csv_data.csv, tsv_data.csv, fixed_width_data.csv > extracted_data.csv',
    dag=dag 
)

# Task 1.8 Transform and load data 

transform = BashOperator(
    task_id='transform_data',
    bash_command = 'tr "[a-z]" "[A-Z]" < path_save/extracted_data.csv > path_save/transformed_data.csv ',
    dag=dag
)

#Task 1.9 Defining Task Pipeline 

# Task	Functionality
# First task	unzip_data
# Second task	extract_data_from_csv
# Third task	extract_data_from_tsv
# Fourth task	extract_data_from_fixed_width
# Fifth task	consolidate_data
# Sixth task	transform_data

download >> unzip >> extract_csv >> extract_tsv >>  extract_fixed_width >> consolidate_data >> transform

# Submitting a DAG is as simple as copying the DAG python file into dags folder in the AIRFLOW_HOME directory.

# Open a terminal and run the command below to submit the DAG that was created in the previous exercise.

# Note: While submitting the dag that was created in the previous exercise, use sudo in the terminal before the command used to submit the dag.


#  cp ETL_toll_data.py $AIRFLOW_HOME/dags

# airflow dags list

# airflow dags list|grep "ETL_toll_data"

# airflow tasks list ETL_toll_data

# airflow dags list-import-errors
