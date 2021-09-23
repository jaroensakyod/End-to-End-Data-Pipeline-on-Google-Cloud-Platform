from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

MYSQL_CONNECTION = "mysql_default" # ชื่อของ connection ใน Airflow ที่เซ็ตเอาไว้

# path ที่จะใช้
mysql_output_path = "/home/airflow/gcs/data/bank_term_deposit.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"

def get_data_from_mysql(transaction_path):
    # รับ transaction_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    bank_term_deposit = mysqlserver.get_pandas_df(sql="SELECT * FROM bank_term_deposit_old")
    # save ไฟล์ CSV
    bank_term_deposit.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def clear_db(transaction_path, output_path):
    # อ่านจากไฟล์ สังเกตว่าใช้ path จากที่รับ parameter มา
    table_df = pd.read_csv(transaction_path)

    table_df['age'].fillna('41.6', inplace = True)
    table_df['balance'].fillna('1136.75', inplace = True)
    table_df['pdays'] = table_df['pdays'].astype(str)
    table_df['pdays']=table_df.apply(lambda x: x["pdays"].replace("-1","0"), axis=1)
    table_df['pdays'] = table_df['pdays'].astype(int)
    table_df['age'] = table_df['age'].astype(float)
    table_df['balance'] = table_df['balance'].astype(float)

    # save ไฟล์ CSV
    table_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")


with DAG(
    "bank_term_deposit_to_bq_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["bank_term_deposit"]
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={"transaction_path": mysql_output_path},
    )

    t2 = PythonOperator(
        task_id="clear_db",
        python_callable=clear_db,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "output_path": final_output_path
        },
    )

    # TODO: สร้าง t3 ที่เป็น GCSToBigQueryOperator เพื่อใช้งานกับ BigQuery แบบ Airflow และใส่ dependencies

    t3 = BashOperator(
        task_id="load_to_bigqurey",
        bash_command="bq load \
            --source_format=CSV\
            --autodetect \
            bank_term_deposit.bank_term \
            gs://asia-east2-testproject-9f997ddb-bucket/data/output.csv"

    )
    t1 >> t2 >> t3
#(GCSToBigQueryOperator(
        #bucket='asia-east2-testproject-9f997ddb-bucket',
        #source_objects=['data/output.csv'],
        #destination_project_dataset_table='bank_term_deposit.bank_term_deposit2',
        #schema_fields =[
            #{'name':'age','type':'FLOAT','mode':'NULLABLE'},
            #{'name':'marital','type':'STRING','mode':'NULLABLE'},
            #{'name':'education','type':'STRING','mode':'NULLABLE'},
            #{'name':'default','type':'STRING','mode':'NULLABLE'},
            #{'name':'balance','type':'FLOAT','mode':'NULLABLE'},
            #{'name':'housing','type':'STRING','mode':'NULLABLE'},
            #{'name':'loan','type':'STRING','mode':'NULLABLE'},
            #{'name':'contact','type':'STRING','mode':'NULLABLE'},
            #{'name':'day','type':'INTEGER','mode':'NULLABLE'},
            #{'name':'month','type':'STRING','mode':'NULLABLE'},
            #{'name':'duration','type':'INTEGER','mode':'NULLABLE'},
            #{'name':'campaign','type':'INTEGER','mode':'NULLABLE'},
            #{'name':'pdays','type':'INTEGER','mode':'NULLABLE'},
            #{'name':'previous','type':'INTEGER','mode':'NULLABLE'},
            #{'name':'poutcome','type':'STRING','mode':'NULLABLE'},
            #{'name':'y','type':'STRING	','mode':'NULLABLE'},

        #],
        #write_disposition='WRITE_TRUNCATE',)