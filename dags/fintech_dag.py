# To be able to import your function, you need to add the src/ directory to the Python path.
# import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import functions as fn
import fintech_dashboard as fdsh


# def extract_clean(filename):
#     df = pd.read_csv(filename)
#     df = clean_missing(df)
#     df.to_csv('/opt/airflow/data/titanic_clean.csv',index=False)
#     print('loaded after cleaning succesfully')

# def encode_load(filename):
#     df = pd.read_csv(filename)
#     df = encoding(df)
#     try:
#         df.to_csv('/opt/airflow/data/titanic_transformed.csv',index=False, mode='x')
#         print('loaded after cleaning succesfully')
#     except FileExistsError:
#         print('file already exists')

# def clean_missing(df):
#     df = impute_mean(df,'Age')
#     df = impute_arbitrary(df,'Cabin','Missing')
#     df = cca(df,'Embarked')
#     return df
# def impute_arbitrary(df,col,arbitrary_value):
#     df[col] = df[col].fillna(arbitrary_value)
#     return df
# def impute_mean(df,col):
#     df[col] = df[col].fillna(df[col].mean())
#     return df
# def impute_median(df,col):
#     df[col] = df[col].fillna(df[col].mean())
#     return df
# def cca(df,col):
#     return df.dropna(subset=[col])
# def encoding(df):
#     df = one_hot_encoding(df,'Embarked')
#     df = label_encoding(df,'Cabin')
#     return df
# def one_hot_encoding(df,col):
#     to_encode = df[[col]]
#     encoded = pd.get_dummies(to_encode)
#     df = pd.concat([df,encoded],axis=1)
#     return df
# def label_encoding(df,col):
#     df[col] = preprocessing.LabelEncoder().fit_transform(df[col])
#     return df
# def load_to_csv(df,filename):
#     df.to_csv(filename,index=False)
#     print('loaded succesfully')
    
# def load_to_postgres(filename): 
#     df = pd.read_csv(filename)
#     engine = create_engine('postgresql://root:root@pgdatabase:5432/titanic_etl')
#     if(engine.connect()):
#         print('connected succesfully')
#     else:
#         print('failed to connect')
#     df.to_sql(name = 'titanic_passengers',con = engine,if_exists='replace')



# Define the DAG
default_args = {
    "owner": "Mo_Tammaa_52_20136",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'fintech_etl_pipeline',
    default_args=default_args,
    description='fintech etl pipeline',
)

with DAG(
    dag_id = 'fintech_etl_pipeline',
    schedule_interval = '@once', # could be @daily, @hourly, etc or a cron expression '* * * * *'
    default_args = default_args,
    tags = ['fintech-pipeline'],
)as dag:
    # Define the tasks
    extract_clean_task = PythonOperator(
        task_id = 'extract_clean',
        python_callable = fn.extract_cleaned,
        # op_kwargs = {
        #     'filename': '/opt/airflow/data/fintech.csv'
        # }
    )

    transform_task = PythonOperator(
        task_id = 'transform',
        python_callable = fn.transform
    )

    load_to_postgres_task = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = fn.load_to_db
    )

    # show_dashboard_task = PythonOperator(
    #     task_id = 'show_dashboard',
    #     python_callable = fdsh.create_dashboard
    # )

    show_dashboard_task_bash = BashOperator(
        task_id = 'show_dashboard_bash',
        bash_command = 'python /opt/airflow/dags/fintech_dashboard.py'
    )
        

    # Define the task dependencies
    extract_clean_task >> transform_task >> load_to_postgres_task >> show_dashboard_task_bash