import sys
import datetime
import time
import json
import MySQLdb
import math
import base64
import linecache
import authdata # imports my credentials

from Crypto.Cipher import AES
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
from operator import add

def streamrdd_to_df(srdd):
    sdf = sqlContext.createDataFrame(srdd)
    sdf.show(n=2, truncate=False)
    return sdf

def make_item(strItem):
    # Each srtItem has the following items:
    # 'Client IP Address', 'Client MAC Address', 'Association Time', 'Vendor',
    # 'AP Name', 'Device Name', 'Map Location', 'SSID', 'Profile', 'VLAN ID',
    # 'Protocol', 'Session Duration', 'Policy Type', 'Avg. Session Throughput (Kbps)'
    networkItem = {}
    parts = strItem.split("\t") # items are tab separated
    networkItem['ip_address'] = parts[0]
    networkItem['mac_address'] = parts[1]
    networkItem['association_time'] = parts[2]
    networkItem['vendor'] = parts[3]
    networkItem['access_point'] = parts[4]
    networkItem['device_name'] = parts[5]
    networkItem['map_location'] = parts[6]
    networkItem['ssid'] = parts[7]
    networkItem['profile'] = parts[8]
    networkItem['vlan_id'] = parts[9]
    networkItem['protocol'] = parts[10]
    networkItem['session_duration'] = parts[11]
    networkItem['policy_type'] = parts[12]
    networkItem['throughput'] = parts[13]
    networkItem['recordID'] = parts[14]
    return networkItem


################################################
## Start: Encryption related helper functions ##
################################################

def get_datetime(line, DATETIMEFORMAT='%a %b %d %H:%M:%S UTC %Y'):
    # Returns date + time + time-zone (by default) of a line
    # Returns datetime
    return datetime.datetime.strptime(line, DATETIMEFORMAT)

def add_seconds(given_time, second=60):
    # Adds seconds to the given time
    # Returns datetime
    return given_time + datetime.timedelta(seconds = second)

def time_delta(start_time, end_time):
    # Returns time difference in a minute resolution

    # Convert to Unix timestamp
    start_time_ts = time.mktime(start_time.timetuple())
    end_time_ts = time.mktime(end_time.timetuple())

    # They are now in seconds, subtract and then divide by 60 to get minutes
    return int((end_time_ts - start_time_ts) / 60)

def maxLength(maxNum):
    # Returns the max length for all binary strings
    maxBinary = '{0:b}'.format(maxNum)
    maxLen = len(maxBinary)
    return maxLen

def maxDepth(maxNum):
    # Returns the max depth for all binary strings
    maxDep = math.log(maxNum + 1, 2)
    maxDep = int(maxDep)
    return maxDep

def pad(plainText):
    # Pads string with '{'s
    BLOCK_SIZE = 16 #32
    PADDING = '{'
    paddedText = plainText + (BLOCK_SIZE - len(plainText) % BLOCK_SIZE) * PADDING
    return paddedText

def unpad(paddedText):
    # Removes the padded '{'s
    start = paddedText.find('{')
    unpadded = paddedText[0:start]
    return unpadded

def aesen(plainText, key):
    # Encryps plaintext with given key. Returns base64 encoded string
    cipher = AES.new(key, AES.MODE_ECB)
    padded = pad(plainText)
    encoded = padded.encode('utf_8') # string to byte
    encrypted = cipher.encrypt(encoded);
    cipherBytes = base64.b64encode(encrypted)
    cipherText = cipherBytes.decode('utf_8') # byte to string
    return cipherText

def stringtokey(string):
    # Converts string key to byte key
    bytekey = string.encode('utf_8')
    key = base64.b64decode(bytekey)
    return key

def binaryGen(node, maxLen):
    binary = '{0:b}'.format(node)
    binaryLen = len(binary)
    diffLen = maxLen - binaryLen
    for l in range(diffLen):
        binary = '0' + binary
    return binary

def leftRight(start, end, maxLen):
    startBinary =  binaryGen(start, maxLen)
    endBinary = binaryGen (end, maxLen)
    temp = ''
    depth = maxLen
    res = list()
    for r in range(len(startBinary)):
        if startBinary[r] == endBinary[r]:
            temp = temp + startBinary[r]
            depth = depth - 1
        else:
            res.append([temp + '0', depth - 1])
            res.append([temp + '1', depth - 1])
            break
    return res
    
def leftmost(root, maxLen):
    diff = maxLen - len(root[0])
    leftmostNode = root[0]
    for l in range(diff):
        leftmostNode = leftmostNode + '0'
    return leftmostNode

def rightmost(root, maxLen):
    diff = maxLen - len(root[0])
    rightmostNode = root[0]
    for l in range(diff):
        rightmostNode = rightmostNode + '1'
    return rightmostNode

def delegationPlainGen(start, end, maxLen):
    res = list()
    if start == end:
        res.append([binaryGen(start, maxLen), 0])
        return res
    root = leftRight(start, end, maxLen)
    leftChild = root[0]
    left = leftmost(leftChild, maxLen)
    startBinary =  binaryGen(start, maxLen)
    if left == startBinary:
        res.append(leftChild)
    else:
        for i in range(len(startBinary)):
            if startBinary[len(startBinary) - 1 - i] == '1':
                bitLoc = len(startBinary) - 1 - i
                break
        rootLoc = len(root[0][0])
        depth = root[0][1]
        for j in range(bitLoc-rootLoc):
            if startBinary[rootLoc+j] == '0':
                node = startBinary[0:rootLoc+j] + '1'
                depth = len(startBinary) - (rootLoc+j) - 1
                res.append([node,depth])
        res.append([startBinary[0:bitLoc+1], len(startBinary) - bitLoc - 1])
    rightChild = root[1]
    right = rightmost(rightChild, maxLen)
    endBinary =  binaryGen(end, maxLen)
    if right == endBinary:
        res.append(rightChild)
    else:
        for i in range(len(endBinary)):
            if endBinary[len(endBinary) - 1 - i] == '0':
                bitLoc = len(endBinary) - 1 - i
                break
        rootLoc = len(root[1][0])

        depth = root[1][1]
        for j in range(bitLoc-rootLoc):
            if endBinary[rootLoc+j] == '1':
                node = endBinary[0:rootLoc+j] + '0'
                depth = len(endBinary) - (rootLoc+j) - 1
                res.append([node,depth])
        res.append([endBinary[0:bitLoc+1], len(endBinary) - bitLoc - 1])
    return res

def md(start, end):
    res = math.ceil(math.log((end - start + 1), 2)) - 2 + 1
    return res

def level(maxNO):
    res = list()
    for i in range(int(maxNO)):
        res.append(i)
    res = set(res)
    return res

def getDepth(nodes):
    depth = list()
    res = list()
    for node in nodes:
        depth.append(node[1])
    res = set(depth)
    return res

def checkLevel(levelSet, nodes):
    miss = list()
    sig = False
    for node in levelSet:
        if not node in nodes:
            miss.append(node)
            sig = True
    node = sorted(miss, reverse = True)
    res = list()
    res.append(sig)
    res.append(node)
    return res

def pos(depth, nodes):
    poi = -1
    maxNO = 0
    for node in nodes:
        poi = poi + 1
        if node[1] == (depth + 1):
            maxNO = poi
    return maxNO

def decomp(poi, nodes):
    res = list()
    left = [nodes[poi][0] + '0', nodes[poi][1] - 1]
    right = [nodes[poi][0] + '1', nodes[poi][1] - 1]
    res.append(left)
    res.append(right)
    return res

def split(nodes):
    cur = 0
    for i in range(len(nodes)-1):
        if nodes[i][1] > nodes[i+1][1]:
            if i == len(nodes) - 1:
                cur = i
            continue
        else:
            cur = i
            break
    left = nodes[:cur+1]
    right = nodes[cur+1:]
    res = [left] + [right]
    return res

def insert(origin, decompNode, poi):
    left = decompNode[0]
    right = decompNode[1]
    leftList = split(origin)[0]
    rightList = split(origin)[1]
    if poi >= len(leftList):
        rightList.remove(origin[poi])
    else:
        leftList.remove(origin[poi])
    if not leftList:
        leftList = [left]
    else:
        for i in range(len(leftList)):
            if left[1] > leftList[i][1]:
                leftList = leftList[:i] + [left] + leftList[i:]
                break
            if i == len(leftList)-1:
                leftList = leftList + [left]
                break
    if not rightList:
        rightList = [right]
    else:
        for i in range(len(rightList)):
            if right[1] > rightList[i][1]:
                rightList = rightList[:i] + [right] + rightList[i:]
                break
            if i == len(rightList)-1:
                rightList = rightList + [right]
                break
    origin = leftList + rightList
    return origin

def urc(start, end, maxLen):
    temp = delegationPlainGen(start, end, maxLen)
    maxNO = md(start, end)
    allNO = level(maxNO)
    sig = True
    while sig:
        nodes = temp
        depth = getDepth(nodes)
        miss = checkLevel(allNO, depth)
        if miss[0] == True:
            no = miss[1]
            if no[0] == maxNO:
                break
            else:
                p = pos(no[0], nodes)
                node = decomp(p, nodes)
                temp = insert(nodes, node, p)
        else:
            break
    return temp

def divide_into_subintervals(start, end, interval_len, maxLen, minutesInAllDays):
    max_interval = minutesInAllDays / interval_len
    result = []
    start_interval_index = 0
    end_interval_index = 0
    for i in range(0, max_interval + 1, 1):
        if (i * interval_len > start):
            start_interval_index = i
            break
    for i in range(max_interval, -1, -1):
        if (i * interval_len < end):
            end_interval_index = i
            break
    # The sub-intervals corresponding to [start, end] are
    # [start, start_interval_index * interval_len ... end_interval_index * interval_len, end]
    if (start_interval_index > end_interval_index):
        result.extend(urc(start, end, maxLen)) # Appends the contents of urc() to results list
    else:
        result.extend(urc(start, start_interval_index * interval_len, maxLen))
        # This is for the intervals between the first and last intervals; it could be empty
        for i in range(start_interval_index, end_interval_index, 1):
            start_val = i * interval_len + 1
            end_val = (i + 1) * interval_len
            result.extend(urc(start_val, end_val, maxLen))
        if ((start == end_interval_index * interval_len) and (end == start_interval_index * interval_len)):
            pass
        else:
            result.extend(urc(end_interval_index * interval_len, end, maxLen))
    return result

##############################################
## End: Encryption related helper functions ##
##############################################

def save_to_db(data, insertSQL, emptyTable='No'):
    # Connects to DB and inserts the data
    conn = MySQLdb.connect(host=authdata.MYSQL_DATABASE_HOST,
                           user=authdata.MYSQL_DATABASE_USER,
                           passwd=authdata.MYSQL_DATABASE_PASSWORD,
                           db=authdata.MYSQL_DATABASE_DB)
    c = conn.cursor() # creating a Cursor object to execute queries

    # To empty the table if asked
    if emptyTable == 'Yes':
        c.execute('TRUNCATE TABLE t_range_m')

        # Make sure data is committed to the database
        conn.commit()

    try:
        c.execute(insertSQL, data)
        conn.commit()

    except MySQLdb.Error as err:
        conn.rollback() # rollback transaction here 
        print(err[1])
        print('rollback')

    finally:
        # Make sure data is committed to the database
        conn.commit()
        c.close()
        conn.close()

def encrypt_record(record):
    # Takes a record in and returns an encrypted message
   
    # Encryption key
    aesKeyStr = authdata.AESKEY
    
    # The closest power of 2 that is larger than 60 (minutes) is 2^6 = 64
    interval_len = 64

    # The full path of the map
    path = '/home/ubuntu/git/sb-project/data/map-data/rangem.txt'

    # Total number of days
    numDays = 100
    minutesInAllDays = 24 * 60 * numDays
    maxNum = int(math.pow(2, math.ceil(math.log(minutesInAllDays, 2)))) - 1
    first_day_of_the_log = get_datetime('2015-10-15', '%Y-%m-%d')

    # Tree related parameters
    maxLen = maxLength(maxNum)
    maxDep = maxDepth(maxNum)

    recordID = record['recordID']
    mac_address = record['mac_address']
    mac_address_enc = aesen(mac_address, stringtokey(aesKeyStr))
    access_point = record['access_point']
    access_point_enc = aesen(access_point, stringtokey(aesKeyStr))
    association_time = record['association_time']
    session_duration_sec = record['session_duration']

    # Get the disassociation time in datetime
    astime = get_datetime(association_time, '%Y-%m-%d %H:%M:%S')
    distime = add_seconds(astime, int(session_duration_sec))

    # Number of minutes passed from the first day for current association time
    start = time_delta(first_day_of_the_log, astime)
    end = time_delta(first_day_of_the_log, distime)

    internalNodes = divide_into_subintervals(start, end, interval_len, maxLen, minutesInAllDays)
    
    res = list()
    index = 0
    for i in range(0, len(internalNodes)):
        nodeValue = internalNodes[i][0]
        nodeDepth = internalNodes[i][1]
        index = int(math.pow(2, len(nodeValue))) - 2 + int(nodeValue, 2)
        res.append(linecache.getline(path, index).rstrip('\n') + ',' + str(nodeDepth))
    internalNodesStr = ';'.join(res)

    tempStay = str(astime) + " " + str(distime)
    stay_enc = aesen(tempStay, stringtokey(aesKeyStr))
    
    # Data to insert to the DB
    data = {'recordID': recordID,
            'c1': mac_address_enc,
            'c2': access_point_enc,
            'c3': internalNodesStr,
            'c4': stay_enc}
    insertSQL = "insert into t_range_m (recordID, c1, c2, c3, c4) values (%(recordID)s, %(c1)s, %(c2)s, %(c3)s, %(c4)s)"
    save_to_db(data, insertSQL)


# Set up the contexts
conf = SparkConf().setAppName("Smart Buildings")
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, 1) # 1 second window

# Returns a DStream (Discretized Stream) object
kafka_stream = KafkaUtils.createStream(stream,  # StreamingContext object
               'localhost:2181',                # Zookeeper quorum
               'my-test-group',                 # The group id for this consumer
               {'smartBuildings':1})            # Dict of (topic_name -> numPartitions) to consume
                                                # Each partition is consumed in its own thread
val_tup = kafka_stream.map(lambda x: x[1])
itemsStream = val_tup.map(lambda s: make_item(s))
encStream = itemsStream.map(lambda e: encrypt_record(e))

encStream.pprint()

stream.start() # start the streaming application
stream.awaitTermination()
