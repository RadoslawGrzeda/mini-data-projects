import functions_framework
import pandas as pd 
from google.cloud import storage,bigquery
from datetime import datetime
from google.cloud.bigquery import WriteDisposition
from dotenv import load_dotenv
import os

load_dotenv()

project=os.getenv('project')
bucketName=os.getenv('bucketName')
fileName=os.getenv('fileName')
dataset=os.getenv('dataset')
table=os.getenv('table')
bq_table=f'{project}.{dataset}.{table}'

@functions_framework.cloud_event
def load_data(cloud_event):
    data = cloud_event.data
    file_name=data['name']
    bucket=data['bucket']

    extracted_data=extract_data(bucket,file_name)
    transformed_data=transform_data(extracted_data)
    load_to_bigquery(transformed_data,bq_table,file_name)

def extract_data(bucketName:str,fileName:str) -> pd.DataFrame:
    """Read parquet file from GCS to pandas DataFrame""" 
    try:
        storageClient=storage.Client()
        bucket=storageClient.bucket(bucketName)
        blob=bucket.blob(fileName)
    
        with blob.open('rb') as f:
            df=pd.read_parquet(f)
    
        return df 

    except Exception as e:
        print(f"Something went wrong with extract step {e}")
        return pd.DataFrame()

def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    """Filter and transform the data"""
    try:

        if df.empty:
            print("DataFrame is empty in transfromation step ")
            return df

        df=df.drop(columns=['TrnType','TrnActivityTypeId','TrnTill','TrnOperator','POSName',])
        df=df[df['TrnLoyaltyAccountId'].notna()]
        df['source']='cloud_function'
        df['UpdateDate']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return df

    except Exception:
        print("Something went wrong with transformation step")
        return pd.DataFrame() 

def load_to_bigquery(df:pd.DataFrame,path:str,file_name:str) -> None:
    """Append DataFrame to Bigquery table"""
    try:
        if df.empty:
            print("DataFrame is empty nothing is appended to BQ")
            return
        bigqueryClient=bigquery.Client()

        safe_file=file_name.replace('/','_')
        job_id=f'load_{safe_file}'

        job_config = bigquery.LoadJobConfig(write_disposition=WriteDisposition.WRITE_APPEND)
    
        loadJob=bigqueryClient.load_table_from_dataframe(df,path,job_config=job_config,job_id=job_id)
        loadJob.result()
    except Exception:
        print("Something went wrong with load step")
