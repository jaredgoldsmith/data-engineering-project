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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import os
import requests
from bs4 import BeautifulSoup


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1

    # Gather breadcrumb data and add send it through Kafka Producer
    dir_path = os.path.dirname(os.path.realpath(__file__))
    count = 0


    s = requests.Session()
    url = 'http://www.psudataeng.com:8000/getStopEvents/'
    resp = s.get(url)

    soup = BeautifulSoup(resp.text, 'html.parser')
    tables = soup.find_all('table')
    trips = soup.find_all('h3')
    column_names = tables[0].find_all('th')
    cols = []
    for col in column_names:
        cols.append(col.text)
    records = []
    print(len(trips))
    z = 0
    for i in range(len(trips)):
        rows_values = tables[i].find_all('td')
        rows = []
        for row in rows_values:
            rows.append(row.text)
        j = 0
        data = dict()
        while j < len(rows):
            if j % 23 == 0:
                trip_id = trips[i].text.split(' ')[4]
                if data:
                    records.append(data)
                data = dict()
                data['trip_id'] = str(trip_id)
                z = 0
            data[cols[z]] = str(rows[j])
            j, z = j+1, z+1
            if j == len(rows):
                records.append(data)
                break

    producer.poll(0)
    for val in records:
        record_value = json.dumps(val)
        record_key = "daily reading"
        count += 1
        try:
            producer.poll(0)
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        except BufferError as bfer:
            producer.poll(0.1)

    print(f'total count of messages should be: {count}')
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
