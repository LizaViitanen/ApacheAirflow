import os
import pandas as pd


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    dt = df.to_json()
    context['task_instance'].xcom_push(key='download_titanic_dataset', value=dt)


def pivot_dataset(**context):
    titanic_df = context['task_instance'].xcom_pull(key='download_titanic_dataset', task_ids='download_titanic_dataset')
    dt = pd.read_json(titanic_df)
    df = dt.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    dt = dt.to_json()
    context['task_instance'].xcom_push(key='titanic_pivot', value=dt)

def mean_fare_per_class(**context):
    titanic_df = context['task_instance'].xcom_pull(key='download_titanic_dataset', task_ids='download_titanic_dataset')
    df = pd.read_json(titanic_df)
    dt = df.groupby('Pclass')['Fare'].mean()
    dt = dt.to_json()
    context['task_instance'].xcom_push(key='titanic_mean_fares', value=dt)

