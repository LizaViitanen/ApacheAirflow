from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.decorators import dag, task
from util.settings import default_settings
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine


default_args = {
    'owner': 'airflow',
}


@dag(**default_settings())
def titanic_taskflow_api_etl():
    def connect_db(db_name, dataframe):
        alchemyEngine = create_engine('postgresql://admin:admin@localhost:5432/gb_airflow')
        postgreSQLConnection = alchemyEngine.connect()
        postgreSQLTable = db_name
        dataframe.to_sql(postgreSQLTable, postgreSQLConnection)
        postgreSQLConnection.close()

    @task()
    def download_titanic_dataset(url='https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'):
        df = pd.read_csv(url)
        titanic_df_json = df.to_json()
        return {'titanic_df_json'}

    @task()
    def pivot_dataset(titanic_df_json):
        dt = pd.read_json(titanic_df_json)
        df = dt.pivot_table(index=['Sex'],
                            columns=['Pclass'],
                            values='Name',
                            aggfunc='count').reset_index()
        connect_db('titanic_pivot', df)

    @task()
    def mean_fare_per_class(titanic_df_json):
        df = pd.read_json(titanic_df_json)
        dt = df.groupby('Pclass')['Fare'].mean()
        connect_db('titanic_mean_fare', dt)

    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is: {{ ds }}"',
    )

    create_titanic_dataset = download_titanic_dataset()
    pivot_titanic_dataset = pivot_dataset(create_titanic_dataset)
    mean_fares_titanic_dataset = mean_fare_per_class(create_titanic_dataset)

    first_task >> create_titanic_dataset >> mean_fares_titanic_dataset >> pivot_titanic_dataset >> last_task


my_titanic_dag = titanic_taskflow_api_etl()