from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import psycopg2
import itertools
import requests
import json
import pytz
import os

WEATHER_KEY="******"
TRAFFIC_KEY="******"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_capital():

    url = "https://gist.githubusercontent.com/pimatco/968ef5ab81a516605fbbb1fa79ed89a0/raw/3c7e72609bbc50b17a5666a762bce05a7f554e82/estados-cidades-capitais.json"
    response = requests.get(url)
    states = json.loads(response.text)
    southeast = ['SP', 'MG', 'RJ', 'ES']
    states = {s['sigla']:s['capital'] for s in states['estados'] if s['sigla'] in southeast}
    perm_paths = list(itertools.permutations(southeast, 2))

    return states, perm_paths

def get_traffic_data(origin, destination):

    origin += ', Brasil'
    destination += ', Brasil'

    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&key={TRAFFIC_KEY}"

    response = requests.get(url)
    return json.loads(response.text)  

def get_weather_data(lat, lng):

    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lng}&appid={WEATHER_KEY}&units=metric"

    response = requests.get(url)
    return json.loads(response.text)    

def convert_to_gmt_minus3(date):

    dt_utc = datetime.strptime(date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

    tz_gmt_minus_3 = pytz.timezone('Etc/GMT+3')

    dt_gmt_minus_3 = dt_utc.astimezone(tz_gmt_minus_3)

    return dt_gmt_minus_3.strftime('%Y-%m-%d %H:%M:%S')

def find_next_weather_data(weather_data, dt):

    target_timestamp = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')

    for data in weather_data:

        date = convert_to_gmt_minus3(data['dt_txt'])
        
        time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        if time > target_timestamp:
            return data['weather'][0]['description'], data['main']['temp'], date
        
    return None, None, None

def connect_db():
    conn = psycopg2.connect(
        dbname="desafio", 
        user="desafio", 
        password="desafio", 
        host="postgres", 
        port="5432"
    )
    
    return conn, conn.cursor()

def store_data(path_info, weather_info):

    conn, cur = connect_db()

    for path, w_path in zip(path_info,weather_info):

        cur.execute(
            "INSERT INTO Routes (origin, destination, total_distance, arrival_time) VALUES (%s, %s, %s, %s) RETURNING route_id",
            (path['origin'], path['destination'], path['distance'], datetime.strptime(path['dt'], '%Y-%m-%d %H:%M:%S'))
        )

        route_id = cur.fetchone()[0]

        for step, w in zip(path['steps'],weather_info[w_path]):


            cur.execute(
                "INSERT INTO RouteSteps (route_id, step_order, coordinates, distance, estimated_arrival_time) VALUES (%s, %s, %s, %s, %s) RETURNING step_id",
                (route_id, step['order'], step['coord'], step['distance'], datetime.strptime(step['dt'], '%Y-%m-%d %H:%M:%S'))
            )

            step_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO WeatherConditions (step_id, weather_time, weather_description, temperature) VALUES (%s, %s, %s, %s)",
                (step_id, datetime.strptime(w['dt'], '%Y-%m-%d %H:%M:%S'), w['weather'], w['temp'])
            )

    conn.commit()

    cur.close()
    conn.close()


def etl_task():

    states, perm_paths = get_capital()

    path_info = []

    for path in perm_paths:

        origin = states[path[0]]
        destination = states[path[1]]

        data = get_traffic_data(origin,destination)

        legs = data['routes'][0]['legs'][0]

        current_time = datetime.now()

        new_time = current_time

        steps_list = []

        coordinates = set()

        order = 1

        for s in legs['steps']:

            coord = (s['start_location']['lat'], s['start_location']['lng'])

            if coord not in coordinates:

                coordinates.add(coord)

                duration_value = s['duration']['value']

                new_time += timedelta(seconds=duration_value)

                distance = float(s['distance']['value']/1000)

                steps_list.append({
                                    'coord':coord,
                                    'distance':distance,
                                    'dt':new_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'order':order
                                })
                
                order += 1
                
        distance = float(legs['distance']['value']/1000)

        duration_value = legs['duration']['value']

        new_time = current_time + timedelta(seconds=duration_value)

        path_info.append({
                            'origin':origin,
                            'destination':destination,
                            'distance':distance,
                            'dt':new_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'steps':steps_list
                        })
    
    weather_info = {}

    for p, traffic in zip(perm_paths,path_info):

        paths = traffic['steps']

        weather_list = []

        for path in paths:

            coord = path['coord']

            data = get_weather_data(coord[0], coord[1])

            target_time_dt = datetime.strptime(path['dt'], '%Y-%m-%d %H:%M:%S')
            
            weather, temp, date = find_next_weather_data(data['list'], path['dt'])

            weather_list.append({
                                'coord':coord,
                                'weather':weather,
                                'temp':temp,
                                'dt':date
                                })
            
        weather_info.update({
                            f'{p[0]}, {p[1]}':weather_list
                            })
        
    store_data(path_info, weather_info)

with DAG(
    'weather_data_collection',
    default_args=default_args,
    description='DAG to collect weather data from OpenWeatherMap API',
    schedule_interval=timedelta(hours=1),  # Executa a cada hora
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='etl',
        python_callable=etl_task,
    )

t1
