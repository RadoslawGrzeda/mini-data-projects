from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from datetime import date, datetime
import requests as req
import pandas as pd 
from sqlalchemy import create_engine,text
import os
from dotenv import load_dotenv

load_dotenv() 

key=os.getenv("API_KEY")
lat=52.22977
lon=21.01178



#extract data from api
def _extract_weather(url):
    try:
        params={
            'units': 'metric',
            'lang': 'en'
        }
        response=req.get(url,params=params)

        if (response.status_code!=200):
            return f'Error: {response.status_code}'
        data=response.json()
        return data
    except Exception as e:
        return f'Error: {e}'

#transform data to table
def _transform_weather():
    ti=get_current_context()['ti']
    data=ti.xcom_pull(task_ids='get_weather')
    try:
        country=data['sys']['country']
        city=data['name']
        temp=data['main']['temp']
        temp=round(temp, 2)
        main=data['weather'][0]['main']
        desc=data['weather'][0]['description']
        wind=data['wind']['speed']
    
        table={
            'date': date.today(),
            'Time': datetime.now(),
            "Country":  country,
            "City": city,
            "Temperature": temp,
            "Main": main,
            "Description": desc,
            "Wind_Speed": wind,
            }
    except Exception as e:
        return f'Error: {e}'
    return table

#load data to db if exists current date append to array if not create new row
def _load_weather():
    engine =create_engine(
    os.getenv("DATABASE_URL")
    )
    ti=get_current_context()['ti']
    data=ti.xcom_pull(task_ids='transform_weather')
    with engine.begin() as conn:
        maxDate=conn.execute(text("""
                        SELECT max(date) from weather
            """))

        if (maxDate.fetchone()[0] ==date.today()):
            conn.execute(text("""
                UPDATE weather SET temp= array_append(temp, ROW(:Time, :Description, :Temperature, :Wind_Speed)::weather_temp)
                WHERE date=:date AND country=:Country AND city=:City
            """), data)
        else:
            conn.execute(text("""
                INSERT INTO weather (date, country, city, temp) VALUES
                (:date, :Country, :City,
                ARRAY[ROW(:Time, :Description, :Temperature, :Wind_Speed)::weather_temp]
        )
            """), data)


with DAG("my_dag",start_date=datetime(2025, 1, 1),schedule="0 * * * *",catchup=False) as dag:
    url=f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}'
    getWeather=PythonOperator(
        task_id="get_weather",
        python_callable=_extract_weather,op_args=[url]
    )
    transformWeather=PythonOperator(
        task_id="transform_weather",
        python_callable=_transform_weather
    )
    loadWeather=PythonOperator(
        task_id="load_weather",
        python_callable=_load_weather
    )
    getWeather >> transformWeather >> loadWeather
