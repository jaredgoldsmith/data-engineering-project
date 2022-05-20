#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import time as t
import psycopg2
import csv
import numpy as np
import datetime

DBname = "postgres"
DBuser = "postgres"
DBpwd = "Data99!!"
TableName = "Trip"
full_path = '/home/jgolds/git/data-engineering-project/'

def dbconnect():
    connection = psycopg2.connect(
            host='localhost',
            database=DBname,
            user=DBuser,
            password=DBpwd,
    )
    connection.autocommit = True
    return connection

def break_down_time(total):
    total = int(total)
    hours = total // 3600
    total -= 3600 * hours
    minutes = total // 60
    total -= 60 * minutes
    return hours,minutes,total

def break_down_date(date):
    if 'JAN' in date:
        return date.replace('JAN','01')
    elif 'FEB' in date:
        return date.replace('FEB','02')
    elif 'MAR' in date:
        return date.replace('MAR','03')
    elif 'APR' in date:
        return date.replace('APR','04')
    elif 'MAY' in date:
        return date.replace('MAY','05')
    elif 'JUN' in date:
        return date.replace('JUN', '06')
    elif 'JUL' in date:
        return date.replace('JUL', '07')
    elif 'AUG' in date:
        return date.replace('AUG', '08')
    elif 'SEP' in date:
        return date.replace('SEP', '09')
    elif 'OCT' in date:
        return date.replace('OCT', '10')
    elif 'NOV' in date:
        return date.replace('NOV', '11')
    elif 'DEC' in date:
        return date.replace('DEC', '12')
    else:
        return date

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'data-engineering-class'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    unique_trips = set()
    start = t.time()
    total_count = 0
    maxtime = 0
    #Clear csv file
    f = open(f'{full_path}trips2.csv', 'w+')
    f.close()
    #connect to database
    conn = dbconnect()
    with conn.cursor() as cursor:
        cursor.execute(f'select count(*) from breadcrumb;')
        starting_number_breadcrumbs = cursor.fetchone()[0]
        print(starting_number_breadcrumbs)
        cursor.execute(f'select count(*) from trip;')
        x = cursor.fetchone()[0]
        print(x)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                elapsed = t.time() - start
                if elapsed > 1000:
                    break
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                record_value = record_value.decode("utf-8")

                # Check for unique trips, if unique add them to trips.csv
                json_data = json.loads(record_value)
                trip_num = json_data['trip_id']
                if trip_num not in unique_trips:
                    unique_trips.add(trip_num)
                    with conn.cursor() as cursor:
                        cursor.execute(f'select exists(select trip_id from trip where trip_id = {trip_num});')
                        existing_id = cursor.fetchone()[0]
                    trip_id = json_data['trip_id']
                    route_id = json_data['route_number']
                    vehicle_number = json_data['vehicle_number']
                    service_key = json_data['service_key']
                    if service_key == 'W':
                        service_key = 'Weekday'
                    elif service_key == 'U':
                        service_key = 'Sunday'
                    elif service_key == 'S':
                        service_key = 'Saturday'
                    else:
                        service_key = ''
                    direction = json_data['direction']
                    if direction == '0':
                        direction = 'Out'
                    elif direction == '1':
                        direction = 'Back'
                    if not existing_id:
                        trips = []
                        trips.append(trip_id)
                        trips.append(route_id)
                        trips.append(vehicle_number)
                        trips.append(service_key)
                        trips.append(direction)
                        with open(f'{full_path}trips2.csv', 'a+') as f:
                            writer = csv.writer(f)
                            writer.writerow(trips)
                    else:
                        with conn.cursor() as cursor:
                            if direction == '':
                                cursor.execute(f"update trip set route_id = {route_id}, service_key = '{service_key}' where trip_id = {trip_id};")
                            else:
                                cursor.execute(f"update trip set route_id = {route_id}, service_key = '{service_key}', direction = '{direction}' where trip_id = {trip_id};")


                total_count += 1
                # Copy entire record into json file for backup
                f = open(f'{full_path}data4.json', 'a')
                f.write(record_value)
                f.write("\n")
                f.close()

                # Every 300 seconds, copy data from csv into database 
                elapsed = t.time() - start
                if elapsed > 1000:
                    # Copy data from csv files into the database
                    cur = conn.cursor()
                    with open(f'{full_path}trips2.csv', 'r') as f:
                        cur.copy_from(f, 'trip', sep=',', null='')
                    conn.commit()
                    print(f'Total number of messages received is: {total_count}')
                    conn = dbconnect()
                    with conn.cursor() as cursor:
                        cursor.execute(f'select count(*) from breadcrumb;')
                        x = cursor.fetchone()[0]
                        print(x)
                        cursor.execute(f'select count(*) from trip;')
                        x = cursor.fetchone()[0]
                        print(x)
                    print(f'Total number of messages received is: {total_count}')
                    consumer.close()
                    break
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        # Copy data from csv files into the database
        cur = conn.cursor()
        with open(f'{full_path}trips2.csv', 'r') as f:
            cur.copy_from(f, 'trip', sep=',', null='')
        conn.commit()
        print(f'Total number of messages received is: {total_count}')
        conn = dbconnect()
        with conn.cursor() as cursor:
            cursor.execute(f'select count(*) from breadcrumb;')
            x = cursor.fetchone()[0]
            print(x)
            cursor.execute(f'select count(*) from trip;')
            x = cursor.fetchone()[0]
            print(x)
        print(f'Total number of messages received is: {total_count}')
        consumer.close()
