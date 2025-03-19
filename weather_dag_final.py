from airflow import DAG
from datetime import timedelta, datetime, UTC
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 21),
    'email': ['zoltanjelovich@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

cities = ["porto", "lisbon", "sines", "albufeira"]

def fetch_weather_data(city, ti, **kwargs):
    http_hook = HttpHook(method="GET", http_conn_id="openweather_api")
    endpoint = f"/data/2.5/weather?q={city}&appid=8835872e0c227b9cfa9b463922684525"
    response = http_hook.run(endpoint)
    data = response.json()
    ti.xcom_push(key=f"{city}_raw_data", value=data)

def kelvin_to_celsius(temp_in_kelvin):
    return round((temp_in_kelvin - 273.15), 3)

def transform_data(city, ti, **kwargs):
    data = ti.xcom_pull(key=f"{city}_raw_data", task_ids=f"extract_weather_data_{city}")
    transformed_data = [{
        "city": data["name"],
        "description": data["weather"][0]['description'],
        "temperature_celsius": kelvin_to_celsius(data["main"]["temp"]),
        "feels_like_celsius": kelvin_to_celsius(data["main"]["feels_like"]),
        "minimum_temp_celsius": kelvin_to_celsius(data["main"]["temp_min"]),
        "maximum_temp_celsius": kelvin_to_celsius(data["main"]["temp_max"]),
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "time_of_record": datetime.fromtimestamp(data['dt'] + data['timezone'], UTC),
        "sunrise_local_time": datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'], UTC),
        "sunset_local_time": datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'], UTC)
    }]

    df = pd.DataFrame(transformed_data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_data = csv_buffer.getvalue()
    ti.xcom_push(key=f"{city}_csv_data", value=csv_data)

def load_to_rds(city, ti, **kwargs):
    table_name = f"weather_data_{city.replace(' ', '_')}"
    hook = PostgresHook(postgres_conn_id='postgres_conn')

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        city TEXT,
        description TEXT,
        temperature_celsius NUMERIC,
        feels_like_celsius NUMERIC,
        minimum_temp_celsius NUMERIC,
        maximum_temp_celsius NUMERIC,
        pressure NUMERIC,
        humidity NUMERIC,
        wind_speed NUMERIC,
        time_of_record TIMESTAMP,
        sunrise_local_time TIMESTAMP,
        sunset_local_time TIMESTAMP
    );
    """
    hook.run(create_table_sql)

    csv_data = ti.xcom_pull(key=f"{city}_csv_data", task_ids=f"transform_weather_data_{city}")

    conn = hook.get_conn()
    cursor = conn.cursor()

    csv_buffer = io.StringIO(csv_data)
    cursor.copy_expert(
        sql=f"COPY {table_name} FROM STDIN WITH CSV DELIMITER ','",
        file=csv_buffer
    )

    conn.commit()
    cursor.close()
    conn.close()

def load_to_s3(city, **kwargs):
    table_name = f"weather_data_{city.replace(' ', '_')}"
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    df = hook.get_pandas_df(f"SELECT * FROM {table_name};")
    file_name = f"{city.replace(' ', '_')}_weather_data.csv"
    df.to_csv(f"s3://airflowbucketbdta/{file_name}", index=False)

with DAG('weather_dag_final',
         default_args=default_args,
         schedule_interval='0 8,12,20 * * *',
         catchup=False) as dag:

    start_pipeline = DummyOperator(task_id='start_pipeline')
    end_pipeline = DummyOperator(task_id='end_pipeline')

    for city in cities:
        is_weather_api_ready = HttpSensor(
            task_id=f'is_weather_api_ready_{city}',
            http_conn_id='openweather_api',
            endpoint=f"/data/2.5/weather?q={city}&appid=8835872e0c227b9cfa9b463922684525"
        )

        extract_weather_data = PythonOperator(
            task_id=f"extract_weather_data_{city}",
            python_callable=fetch_weather_data,
            op_kwargs={"city": city}
        )

        transform_weather_data = PythonOperator(
            task_id=f'transform_weather_data_{city}',
            python_callable=transform_data,
            op_kwargs={"city": city}
        )

        load_weather_data_to_rds = PythonOperator(
            task_id=f'load_weather_data_to_rds_{city}',
            python_callable=load_to_rds,
            op_kwargs={"city": city}
        )

        load_weather_data_to_s3 = PythonOperator(
            task_id=f'load_weather_data_to_s3_{city}',
            python_callable=load_to_s3,
            op_kwargs={"city": city}
        )

        start_pipeline >> is_weather_api_ready >> extract_weather_data >> transform_weather_data >> load_weather_data_to_rds >> load_weather_data_to_s3 >> end_pipeline
