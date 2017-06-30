# This script reads an 11-day wifi trace file, replicates it and
# streams to kafka line by line.

import os
import sys
import datetime
import csv
import time
import numpy as np

from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner

# Take the kafka topic as an input argument
if len(sys.argv) > 1:
    myTopic = sys.argv[1]
else:
    print('Please run again and provide a kafka topic as an argument.')
    exit()

print('The kafka topic is: {}').format(myTopic)

# TODO: read all hard coded values from a config file or pass in as arguments with defaults from config
# The data file's full path
datacsv = '/home/ubuntu/git/sb-project/data/data-noUC.csv'

# Hostname and port details
kafka_hostnames = '10.0.0.7:9092,10.0.0.4:9092,10.0.0.13:9092'
zookeeper_host = '10.0.0.10:2181'

client = KafkaClient(hosts=kafka_hostnames, zookeeper_hosts=zookeeper_host)
#client = KafkaClient('localhost:9092')
topic = client.topics[myTopic]

hash_partitioner = HashingPartitioner()
producer = topic.get_producer(partitioner=hash_partitioner, linger_ms=200)


def get_datetime(line, datetimeformat='%a %b %d %H:%M:%S UTC %Y'):
    # Returns date + time + time-zone of a line in the trace
    # Returns datetime
    return datetime.datetime.strptime(line, datetimeformat)

def get_time(line, timeformat='%d days %H hrs %M min %S sec'):
    # Returns time in datetime type
    if 'days' not in line and 'hrs' in line and 'min' in line and 'sec' in line:
        timeformat = '%H hrs %M min %S sec'
    if 'days' not in line and 'hrs' not in line and 'min' in line and 'sec' in line:
        timeformat = '%M min %S sec'
    if 'days' not in line and 'hrs' not in line and 'min' not in line and 'sec' in line:
        timeformat = '%S sec'
    return datetime.datetime.strptime(line, timeformat)

def date_to_string(dt, timeformat='%H:%M:%S'):
    # Converts datetime to string
    return datetime.datetime.strftime(dt, timeformat)

def datetime_to_string(dt, timeformat='%Y-%m-%d %H:%M:%S'):
    # Converts datetime to string
    return datetime.datetime.strftime(dt, timeformat)

def add_days(date, day=1):
    # Adds day(s) to the given date. By default adds 1 day
    # Returns datetime.
    return date + datetime.timedelta(days = day)

def ip_address(line):
    # Returns the IP address from a row in csv file
    return line[0]

def mac_address(line):
    # Returns the MAC address from a row in csv file
    return line[1]

def assocation_time(line):
    # Returns the Association Time in datetime type
    return get_datetime(line[2])

def vendor(line):
    # Returns the Vendor
    return line[3]

def access_point(line):
    # Returns the Access Point name
    return line[4]

def device_name(line):
    # Returns the Device name
    return line[5]

def map_location(line):
    # Returns the Map Location
    return line[6]

def ssid(line):
    # Returns the SSID
    return line[7]

def profile(line):
    # Returns the Profile
    return line[8]

def vlan_id(line):
    # Returns the VLAN ID
    return line[9]

def protocol(line):
    # Returns the Protocol
    return line[10]

def session_suration(line):
    # Returns the Session Duration in seconds
    dt = get_time(line[11])
    return dt.day * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second

def policy_type(line):
    # Returns the Policy Type
    return line[12]

def throughput(line):
    # Returns the Avg. Session Throughput (Kbps)
    return line[13]

def data_to_stream(line, days, recordID):
    # Creates messages for kafka
    output = str()

    try:
        output += ip_address(line)
        output += '\t' + mac_address(line)
        association_datetime = assocation_time(line)
        output += '\t' + datetime_to_string(add_days(association_datetime, days))
        output += '\t' + vendor(line)
        output += '\t' + access_point(line)
        output += '\t' + device_name(line)
        output += '\t' + map_location(line)
        output += '\t' + ssid(line)
        output += '\t' + profile(line)
        output += '\t' + vlan_id(line)
        output += '\t' + protocol(line)
        output += '\t' + str(session_suration(line))
        output += '\t' + policy_type(line)
        output += '\t' + throughput(line)
        output += '\t' + str(recordID)
    except:
        print('Oops, something went wrong. Check the following line:\n{}\n').format(line)
        return str() # return an empty string if something is wrong with the line

    return output


# Read the data (csv) file and convert each row/line to kafka format
# Each line in csv has the following format:
# 'Client IP Address', 'Client MAC Address', 'Association Time', 'Vendor',
# 'AP Name', 'Device Name', 'Map Location', 'SSID', 'Profile', 'VLAN ID',
# 'Protocol', 'Session Duration', 'Policy Type', 'Avg. Session Throughput (Kbps)'

days = 0 # required to replicate the 11 day data
recordID = 1 # required for enc/dec operations

while True:
    with open(datacsv, 'r') as csvFile:
        lines = csv.reader(csvFile)
        row = 0

        for line in lines:
            if row == 0:
                row += 1
                continue # skip the csv header

            new_date = add_days(assocation_time(line), days)
            stream_to_kafka = data_to_stream(line, days, recordID)
            try: # try to encode the kafka message
                encoded_message = stream_to_kafka.encode('UTF-8')
            except: # in case encoding is not successful
                print('Oops, cannot encode the message. Skipping the following message:\n{}\n').format(stream_to_kafka)
                continue # skip the message
            
            # send (produce) messages
            producer.produce(encoded_message, partition_key=mac_address(line))
            
            recordID += 1
            time.sleep(0.1)
    days += 11 # Once the csv file is processed, start over and add 11, 22, 33, ... days to the association date
