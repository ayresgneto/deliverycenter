from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import tarfile



#argumentos default
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2021,9,15,17),
    'email': 'ayresguimaraes@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG (
    "ETL-DeliveryCenter",
    description="ETL de arquivos .csv em um bucket s3 para um dw",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

#alguns caminhos para facilitar o codigo
path = "/home/ayres/airflow/data/"
arquivo = "/home/ayres/airflow/raw/datasets/"
ENDPOINT= "dw-deliverycenter.cl7yfxtcqdsk.us-east-2.rds.amazonaws.com" 
PORT="5432" 
USR="postgres" 
REGION="us-east-2" 
DBNAME="DW-deliverycenter"
PASSWORD="12345678"


#baixa os dados do bucket s3 utilizando AWS CLI
get_data = BashOperator(
    task_id='get-data',
    bash_command="aws s3 cp s3://dc-sourcefiles/datasetdeliverycenter.tar.gz /home/ayres/airflow/raw",
    trigger_rule="all_done",
    dag=dag
    

)

#entrai arquivos tar
def extrair_tar():

    tar = tarfile.open("/home/ayres/airflow/raw/datasetdeliverycenter.tar.gz")
    for member in tar.getmembers():
        print ("Extracting %s" % member.name)
        tar.extract(member, path='/home/ayres/airflow/raw/')

task_extrairtar = PythonOperator(
    task_id='extrair_tar',
    python_callable=extrair_tar,
    dag=dag
)

#Channels
def transforma_channels():

    df = pd.read_csv(arquivo + "channels.csv")

    #remove duplicatas
    df.drop_duplicates()
   
    #dicionario para tratar os dados NA (caso exista), 999 para o id para chaves primarias faltantes
    df.fillna(value={'channel_id': '999', 'channel_name': 'sem canal', 'channel_type': 'sem tipo'})

    #cria um novo arquivo csv tratado
    df.to_csv(path + 'channels_tratado.csv', index=False)


task_transforma_channels = PythonOperator (
    task_id="transforma_channels",
    python_callable=transforma_channels,
    dag=dag
)

#Stores
def transforma_stores():

    #Leitura de arquivo e tratamento de encoding
    df = pd.read_csv(arquivo + "stores.csv", encoding='ISO-8859-1')
   
    df_tratado = df.drop_duplicates()

    df_tratado = df.fillna(value={'store_id': '9999999', 'hub_id': '999', 'store_name': 'STORE SEM NOME',
    'store_segment': 'SEM SEGMENTO', 'store_plan_price': 0, 'store_latitude': 0, 'store_longitude': 0})

    df_tratado.to_csv(path + 'stores_tratado.csv', index=False)


task_transforma_stores = PythonOperator (
    task_id="transforma_stores",
    python_callable=transforma_stores,
    dag=dag
)

#Deliveries
def transforma_deliveries():


    df = pd.read_csv(arquivo + "deliveries.csv", encoding='ISO-8859-1')
   
    df_tratado = df.drop_duplicates()

    df_tratado = df.fillna(value={'delivery_id': '9999999', 'delivery_order_id': '9999999',
     'driver_id': '9999999', 'delivery_distance_meteres': '999999', 'delivery_status': 'SEM STATUS'})

    df_tratado.to_csv(path + 'deliveries_tratado.csv', index=False)


task_transforma_deliveries = PythonOperator (
    task_id="transforma_deliveries",
    python_callable=transforma_deliveries,
    dag=dag
)

#Hubs
def transforma_hubs():


    df = pd.read_csv(arquivo + "hubs.csv", encoding='ISO-8859-1')
   
    df_tratado = df.drop_duplicates()

    df_tratado = df.fillna(value={'hub_id': '9999999', 'hub_name': 'HUB SEM NOME',
     'hub_city': 'HUB SEM CIDADE', 'hub_state': 'HUB SEM ESTADO', 'hub_latitude': 0, 'hub_longitude': 0})

    df_tratado.to_csv(path + 'hubs_tratado.csv', index=False, encoding='ISO-8859-1')


task_transforma_hubs = PythonOperator (
    task_id="transforma_hubs",
    python_callable=transforma_hubs,
    dag=dag
)

#Drivers
def transforma_drivers():


    df = pd.read_csv(arquivo + "drivers.csv", encoding='ISO-8859-1')
   
    df_tratado = df.drop_duplicates()

    df_tratado = df.fillna(value={'driver_id': '9999999', 'driver_modal': 'SEM MODAL', 'driver_type': 'SEM TIPO'})

    df_tratado.to_csv(path + 'drivers_tratado.csv', index=False)

    
    


task_transforma_drivers = PythonOperator (
    task_id="transforma_drivers",
    python_callable=transforma_drivers,
    dag=dag
)

#Payments
def transforma_payments():


    df = pd.read_csv(arquivo + "payments.csv", encoding='ISO-8859-1')
   
    df_tratado = df.drop_duplicates()

    #valores de pagamento e taxa em branco tratados como zero (0) para não serem considerados caso seja feita alguma operação de análise
    df_tratado = df.fillna(value={'payment_id': '9999999', 'payment_order_id': '9999999', 'payment_amount': 000000, 'payment_fee': 000000,
    'payment_method': 'SEM FORMA PAGAMENTO', 'payment_status': 'SEM STATUS PAGAMENTO'})

    df_tratado.to_csv(path + 'payments_tratado.csv', index=False)


task_transforma_payments = PythonOperator (
    task_id="transforma_payments",
    python_callable=transforma_payments,
    dag=dag
)

#Orders
def transforma_orders():


    df = pd.read_csv(arquivo + "orders.csv", encoding='ISO-8859-1')

    #valores de pagamento e taxa em branco tratados como zero (0) para não serem considerados caso seja feita alguma operação de análise
    df_tratado = df.fillna(value={'order_id': '9999999', 'store_id': '9999999', 'channel_id': '9999999', 'payment_order_id': '9999999',
    'payment_order_id': '9999999', 'delivery_order_id': '9999999', 'order_status': 'ORDER SEM STATUS', 'order_amount': 0, 
    'order_livery_fee': 0, 'order_delivery_cost': 00000,'order_created_hour': 33, 'order_created_minute': 66, 'order_created_day': 33, 'order_created_month': 33, 
    'order_created_year': 9999, 'order_moment_created': '0/0/0000 0:00:00 AM', 'order_moment_accepted': '0/0/0000 0:00:00 AM', 'order_moment_ready': '0/0/0000 0:00:00 AM', 'order_moment_collected': '0/0/0000 0:00:00 AM',
    'order_moment_in_expedition': '0/0/0000 0:00:00 AM', 'order_moment_delivering': '0/0/0000 0:00:00 AM', 'order_moment_delivered': '0/0/0000 0:00:00 AM', 'order_moment_finished': '0/0/0000 0:00:00 AM',
    'order_metric_collected_time': 0, 'order_metric_paused_time': 0, 'order_metric_production_time': 0, 'order_metric_walking_time':0, 'order_metric_expedition_speed_time':0, 'order_metric_transit_time':0, 'order_metric_cycle_time': 0,})

    df_tratado.to_csv(path + 'orders_tratado.csv', index=False)


task_transforma_orders = PythonOperator (
    task_id="transforma_orders",
    python_callable=transforma_orders,
    dag=dag
)


#Insere dados no DW

conn_string = "host=dw-deliverycenter.cl7yfxtcqdsk.us-east-2.rds.amazonaws.com \
        dbname='DW-deliverycenter' \
        user='postgres' password='12345678'"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

def insert_dw2():

    drivers = open(path + 'drivers_tratado.csv')

    cursor.execute("CREATE TEMP TABLE drivers_tmp ON COMMIT DROP AS SELECT * FROM drivers \
    WITH NO DATA;")

    SQL = "COPY drivers_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL, file=drivers)

    hubs = open(path + 'hubs_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE hubs_tmp ON COMMIT DROP AS SELECT * FROM hubs \
    WITH NO DATA;")

    SQL2 = "COPY hubs_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL2, file=hubs)


    conn.commit()
    cursor.close()

task_insertdw2 = PythonOperator (
    task_id="insert-dw-2",
    python_callable=insert_dw2,
    dag=dag
)


def insert_dw():


    channels = open(path + 'channels_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE channels_tmp ON COMMIT DROP AS SELECT * FROM channels \
    WITH NO DATA;")

    SQL3 = "COPY channels_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL3, file=channels)

    stores = open(path + 'stores_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE stores_tmp ON COMMIT DROP AS SELECT * FROM stores \
    WITH NO DATA;")

    SQL4 = "COPY stores_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL4, file=stores)

    deliveries = open(path + 'deliveries_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE deliveries_tmp ON COMMIT DROP AS SELECT * FROM deliveries \
    WITH NO DATA;")

    SQL5 = "COPY deliveries_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL5, file=deliveries)

    payments = open(path + 'payments_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE payments_tmp ON COMMIT DROP AS SELECT * FROM payments \
    WITH NO DATA;")

    SQL6 = "COPY payments_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL6, file=payments)

    orders = open(path + 'orders_tratado.csv', encoding='ISO-8859-1')

    cursor.execute("CREATE TEMP TABLE orders_tmp ON COMMIT DROP AS SELECT * FROM orders \
    WITH NO DATA;")

    SQL7 = "COPY orders_tmp FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    cursor.copy_expert(sql=SQL7, file=orders)


    conn.commit()
    cursor.close()



task_insertdw = PythonOperator (
    task_id="insert-dw",
    python_callable=insert_dw,
    dag=dag
)



get_data >> task_extrairtar >> [task_transforma_channels, task_transforma_deliveries, task_transforma_payments, task_transforma_stores, task_transforma_orders] >> task_insertdw
task_extrairtar >> [task_transforma_hubs, task_transforma_drivers] >> task_insertdw2
 

                      