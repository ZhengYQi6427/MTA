# ********************************************************************************************* 
# Program to update dynamodb with latest data from mta feed. It also 
# cleans up stale entried from db Usage python dynamodata.py and
# record the state of the fastest downtown 1,2,3 train every 3 minutes
# *********************************************************************************************
import json,time,sys 
from collections import OrderedDict 
from threading import Thread 
import threading

import datetime 
import boto3 
from boto3.dynamodb.conditions import Key, Attr 
sys.path.append('../utils') 
import mtaUpdates,aws,get_y
import re
from pytz import timezone
import csv
### YOUR CODE HERE ####
DYNAMO_TABLE_NAME = 'mtadata'
VCS = {0:"INCOMING_AT", 1:"STOPPED_AT", 2:"IN_TRANSIT_TO"}
########################################################################
########################################################################
class dynamoMethods:
    def __init__(self, dbName):
        try:
            self.table = dynamodb.create_table(
                TableName=dbName,
                KeySchema=[
                    {
                        'AttributeName': 'tripId',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'tripId',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            self.table.meta.client.get_waiter('table_exists').wait(TableName=dbName)
            print("New Table Created!")
        except:
            self.table = dynamodb.Table(dbName)
            print("Table Already Exists")

    def Add_item(self, item):
        try:
            response = self.table.get_item(Key={'tripId': item['tripId']})
            if 'Item' in response.keys():
                response = self.table.delete_item(
                    Key={
                        'tripId': item['tripId']
                    },)
                response =self.table.put_item(Item=item)
            else:
                self.table.put_item(Item=item)
        except Exception as e:
            print('ERROR: add item fail. msg: ' + str(e))
            print(item)
            self.refresh()
            # exit(-1)

    def Delete_item(self, tripId):
        try:
            response = self.table.delete_item(
                Key={'tripId': tripId}
            )
        except Exception as e:
            print('ERROR: delete item fail. msg: ' + str(e))
            self.refresh()
            # exit(-1)

    def Delete_table(self):
        try:
            self.table.delete_table()
        except Exception as e:
            print('ERROR: delete table fail. msg: ' + str(e))
            self.refresh()
            # exit(-1)
        else:
            print('delete table succ')
    
    def clean_old(self):
        try:
            response = self.table.scan(
            TableName=DYNAMO_TABLE_NAME,
            AttributesToGet=['tripId','timestamp'],
            Limit=10000,)
            items = response['Items']
            currenttime = datetime.datetime.fromtimestamp(int(time.time()),timezone('America/New_York'))
            for item in items:
                t_item = datetime.datetime.fromtimestamp(int(item['timestamp']),timezone('America/New_York'))
                # print(int(item['timestamp']))
                if t_item<currenttime-datetime.timedelta(minutes=2):
                   self.table.delete_item(
                         Key={'tripId':item['tripId']})
            while 'LastEvaluatedKey' in response:
                  lastkey=response['LastEvaluatedKey']
                  response=self.table.scan(
                  TableName=DYNAMO_TABLE_NAME,
                  AttributesToGet=['tripId'],
                  Limit=10000,
                  ExclusiveStartKey=lastkey)
                  items = response['Items']
                  for item in items:
                      t_item = datetime.datetime.fromtimestamp(int(item['timestamp']),timezone('America/New_York'))
                      if t_item<currenttime-datetime.timedelta(minutes=2):
                         self.table.delete_item(Key={'tripId':item['tripId']})

        except Exception as e:
            self.refresh()
            print('clean error: '+str(e))
            # exit(-1)

    def refresh(self):
        global dynamodb
        dynamodb = aws.getResource('dynamodb', 'us-east-1')
        self.table = dynamodb.Table('mtadata')

def earliest96Train(items, stations, route):
    stops = get_y.make_stopIdList('96 St', 'S', stations)
    earliestTrain = {}
    earliestTime = {}
    for r in route:
        earliestTime[r] = float('+inf')
        for item in items:
            if item['routeId'] == r:
                for s in stops:
                    if s in item['futureStops'].keys():
                        if item['futureStops'][s][0]['arrivalTime']:
                            if earliestTime[r] > item['futureStops'][s][0]['arrivalTime'] \
                                    and item['futureStops'][s][0]['arrivalTime'] > time.time():
                                earliestTime[r] = item['futureStops'][s][0]['arrivalTime'] - int(time.time())
                                earliestTrain[r] = item
                        if 'currentStopId' in item.keys():
                            if item['currentStopId'] == s and item['currentStopStatus'] != 'IN_TRANSIT_TO':
                                earliestTime[r] = 0
                                earliestTrain[r] = item
    return earliestTime, earliestTrain

########################################################################
########################################################################


def add(threadname):
    try:
        while (1):
            # print('!!!!!!!!!!!!!!')
            time.sleep(30)
            data = Updates.getTripUpdates()
            if data == "ERROR":
                continue
            # print(data)
            items = data[0]
            timestamp = data[1]
            # print(len(items))
            for item in items:
                dynamoitem = {}
                dynamoitem['tripId'] = item.tripId
                dynamoitem['routeId'] = item.routeId
                dynamoitem['startDate'] = item.startDate
                dynamoitem['direction'] = item.direction
                dynamoitem['timestamp'] = timestamp
                if item.vehicleData:
                    dynamoitem['currentStopId'] = item.vehicleData.currentStopId
                    # print (item)
                    dynamoitem['vehicleTimeStamp'] = item.vehicleData.timestamp
                    dynamoitem['currentStopStatus'] = VCS[int(item.vehicleData.currentStopStatus)]
                if item.futureStops:
                    dynamoitem['futureStops'] = item.futureStops
                DB.Add_item(dynamoitem)
    except KeyboardInterrupt:
        exit(0)

def purge(threadname):
     try:
         while (1):
             time.sleep(60)
             DB.clean_old()
             print('clean')
     except KeyboardInterrupt:
         exit(0)

def get_info(threadname):
    try:
        while (1):
            time.sleep(180)
            table = dynamodb.Table('mtadata')
            stations = get_y.buildStationssDB()

            try:
                response = table.scan(FilterExpression=(Attr('routeId').eq('1') | Attr('routeId').eq('2') |
                                                    Attr('routeId').eq('3')) & Attr('direction').eq('S'))
            except:
                DB.refresh()
                continue

            items = response['Items']
            # print(items)

            activeNumber = {}
            estimatedTime = {'1': 0, '2': 0, '3': 0}
            count = {'1': 0, '2': 0, '3': 0}
            for item in items:

                if item['routeId'] not in activeNumber.keys():
                    activeNumber[item['routeId']] = 1
                else:
                    activeNumber[item['routeId']] += 1

                for s1 in get_y.make_stopIdList('96 St', 'S', stations):
                    if s1 in item['futureStops'].keys():
                        if item['futureStops'][s1][1]['departureTime']:
                            for s2 in get_y.make_stopIdList('42 St', 'S', stations):
                                if s2 in item['futureStops'].keys():
                                    if item['futureStops'][s2][0]['arrivalTime']:
                                        # print(s1, s2)
                                        estimatedTime[item['routeId']] += item['futureStops'][s2][0]['arrivalTime'] \
                                                                   - item['futureStops'][s1][1]['departureTime']
                                        count[item['routeId']] += 1

            for r in estimatedTime.keys():
                if count[r] == 0:
                    estimatedTime[r] = None
                    continue
                estimatedTime[r] = estimatedTime[r]/count[r]

            try:
                result, timetoDest_true = get_y.main(dynamodb)
            except:
                continue

            csvtable = []

            route = ['1', '2', '3']
            elap, items = earliest96Train(items, stations, route)

            for r in items:
                item = items[r]

                csvitem = {}
                tripId = item['tripId']
                csvitem['tripId'] = tripId
                csvitem['route'] = item['routeId']

                now = datetime.datetime.fromtimestamp(item['timestamp']).time()
                csvitem['currentTimestamp'] = now.hour * 60 + now.minute + (0 if now.second < 30 else 1)

                pattern = re.compile('(.*?)_')
                t = re.findall(pattern, tripId)[0]
                startTime = (int(t[0]) * 10 + int(t[1])) * 60 + (int(t[2]) * 10 + int(t[3])) \
                            + (1 if (int(t[4]) * 10 + int(t[5])) > 30 else 0)
                csvitem['trainStartTimes'] = startTime

                csvitem['numberofActiveTrains'] = activeNumber[item['routeId']]

                day = datetime.datetime.fromtimestamp(item['timestamp']).weekday()
                csvitem['DayofTheWeek'] = 0 if day < 5 else 1

                csvitem['elapsedTime'] = elap[item['routeId']]

                csvitem['estimatedTime'] = estimatedTime[item['routeId']]

                # aT = datetime.datetime.fromtimestamp(item['futureStops'][s2][0]['arrivalTime']).time()
                # csvitem['timetoDest'] = aT.hour * 60 + aT.minute + (0 if aT.second < 30 else 1)

                csvitem['trueTimetoDest'] = timetoDest_true[r]
                csvitem['result'] = result
                csvtable.append(list(csvitem.values()))


            # print(csvtable)
            with open("mtadata_new.csv", "a", newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(csvtable)
                csvfile.close()

    except KeyboardInterrupt:
        exit(0)


########################################################################
########################################################################
apikey = '38b00c0c7fe109238277ace1d6573388'
dynamodb = aws.getResource('dynamodb', 'us-east-1')
DB = dynamoMethods(DYNAMO_TABLE_NAME) 
Updates = mtaUpdates.mtaUpdates(apikey)
#_thread.start_new_thread(add, ('thread1',)) 
#_thread.start_new_thread(purge, ('thread2',)) 
datathread = Thread(target = add, args = ('datathread',),daemon = True)
cleanthread = Thread(target = purge, args = ('cleanthread',),daemon =True)
csvthread = Thread(target=get_info, args=('csvthread',), daemon=True)
# trueTimethread = Thread(target=get_trueArrivalTime, args=('trueTimethread'), daemon=True)

try:
    while 1:
        datathread.start()
        cleanthread.start()
        csvthread.start()
        while 1:
            pass
except KeyboardInterrupt:
    exit(0)

