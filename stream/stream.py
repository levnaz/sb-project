# This script reads an 11-day wifi trace file, replicates it and
# streams to kafka line by line.

import os
import sys
import datetime
import csv
import time

from kafka import SimpleProducer, KafkaClient

# take the kafka topic as an input argument
if len(sys.argv) > 1:
    topic = sys.argv[1]
else:
    print('Please run again and provide a kafka topic as an argument.')
    exit()

print('The kafka topic is: {}').format(topic)

# the data file's full path
datacsv = '/home/ubuntu/git/sb-project/data/data.csv'

# hostname and port details
hostname = '10.0.0.14'
port = '9092'

# kafka hostname and port number
kafka = KafkaClient(hostname + ':' + port)
producer = SimpleProducer(kafka)

def get_datetime(line, DATETIMEFORMAT='%a %b %d %H:%M:%S UTC %Y'):
    # returns date + time + time-zone of a line in the trace
    # returns datetime
    return datetime.datetime.strptime(line, DATETIMEFORMAT)

def get_time(line, TIMEFORMAT='%d days %H hrs %M min %S sec'):
    # returns time in datetime type
    if 'days' not in line and 'hrs' in line and 'min' in line and 'sec' in line:
        TIMEFORMAT = '%H hrs %M min %S sec'
    if 'days' not in line and 'hrs' not in line and 'min' in line and 'sec' in line:
        TIMEFORMAT = '%M min %S sec'
    if 'days' not in line and 'hrs' not in line and 'min' not in line and 'sec' in line:
        TIMEFORMAT = '%S sec'
    return datetime.datetime.strptime(line, TIMEFORMAT)

def date_to_string(dt, TIMEFORMAT='%H:%M:%S'):
    # Converts datetime to string
    return datetime.datetime.strftime(dt, TIMEFORMAT)

def datetime_to_string(dt, TIMEFORMAT='%Y-%m-%d %H:%M:%S'):
    # Converts datetime to string
    return datetime.datetime.strftime(dt, TIMEFORMAT)

def add_days(date, day=1):
    # Adds day(s) to the given date. By default adds 1 day
    # Returns datetime.
    return date + datetime.timedelta(days = day)

def ip_address(line):
    # returns the IP address from a row in csv file
    return line[0]

def mac_address(line):
    # returns the MAC address from a row in csv file
    return line[1]

def assocation_time(line):
    # returns the Association Time in datetime type from a row in csv file
    return get_datetime(line[2])

def vendor(line):
    # returns the Vendor from a row in csv file
    return line[3]

def access_point(line):
    # returns the Access Point name from a row in csv file
    return line[4]

def device_name(line):
    # returns the Device name from a row in csv file
    return line[5]

def map_location(line):
    # returns the Map Location from a row in csv file
    return line[6]

def ssid(line):
    # returns the SSID from a row in csv file
    return line[7]

def profile(line):
    # returns the Profile from a row in csv file
    return line[8]

def vlan_id(line):
    # returns the VLAN ID from a row in csv file
    return line[9]

def protocol(line):
    # returns the Protocol from a row in csv file
    return line[10]

def session_suration(line):
    # returns the Session Duration in seconds from a row in csv file
    dt = get_time(line[11])
    return dt.day * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second

def policy_type(line):
    # returns the Policy Type from a row in csv file
    return line[12]

def throughput(line):
    # returns the Avg. Session Throughput (Kbps) from a row in csv file
    return line[13]

def data_to_stream(line, days):
    # creates messages for kafka
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
    except: # in case of exception return an empty string
        print('Oops, something went wrong. Check the following line:\n{}\n').format(line)
        return str() # return an empty string if something is wrong with the line

    return output


# Read the data (csv) file and convert each row/line to kafka format
# Each line in csv has the following format:
# 'Client IP Address', 'Client MAC Address', 'Association Time', 'Vendor',
# 'AP Name', 'Device Name', 'Map Location', 'SSID', 'Profile', 'VLAN ID',
# 'Protocol', 'Session Duration', 'Policy Type', 'Avg. Session Throughput (Kbps)'
days = 0 # required to replicate the 11 day data

while True:
    with open(datacsv, 'r') as csvFile:
        lines = csv.reader(csvFile)
        row = 0

        for line in lines:
            if row == 0:
                row += 1
                continue # skip the csv header

            new_date = add_days(assocation_time(line), days)
            stream_to_kafka = data_to_stream(line, days)
            try: # try to encode the kafka message
                encoded_message = stream_to_kafka.encode('UTF-8')
                #print encoded_message
            except: # in case encoding is not successful
                print('Oops, cannot encode the message. Skipping the following message:\n{}\n').format(stream_to_kafka)
                continue # skip the message
            producer.send_messages(topic, encoded_message) # send the message with the provided topic
            time.sleep(0.0001)
    days += 11 # Once the csv file is processed, start over and add 11, 22, 33, ... days to the association date
