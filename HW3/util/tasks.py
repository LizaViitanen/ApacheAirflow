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
    #titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    #df.to_csv(get_path('titanic_pivot.csv'))
    context['task_instance'].xcom_push(key='titanic_pivot', value=df)

def mean_fare_per_class(**context):
    titanic_df = context['task_instance'].xcom_pull(key='download_titanic_dataset', task_ids='download_titanic_dataset')
    #titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.groupby('Pclass')['Fare'].mean()
    #df.to_csv(get_path('titanic_mean_fares.csv'))
    context['task_instance'].xcom_push(key='titanic_mean_fares', value=df)
