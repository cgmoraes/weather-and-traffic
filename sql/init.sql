CREATE DATABASE airflow;

CREATE TABLE Routes (
    route_id SERIAL PRIMARY KEY,
    origin VARCHAR(255),
    destination VARCHAR(255),
    total_distance FLOAT,
    arrival_time TIMESTAMP
);

CREATE TABLE RouteSteps (
    step_id SERIAL PRIMARY  KEY,
    route_id INT,
    step_order INT,
    coordinates VARCHAR(255),
    distance FLOAT,
    estimated_arrival_time TIMESTAMP,
    FOREIGN KEY (route_id) REFERENCES Routes(route_id)
);

CREATE TABLE WeatherConditions (
    weather_id SERIAL PRIMARY KEY,
    step_id INT,
    weather_time TIMESTAMP,
    weather_description VARCHAR(255),
    temperature FLOAT,
    FOREIGN KEY (step_id) REFERENCES RouteSteps(step_id)
);

