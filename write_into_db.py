# *********************************************************************************************
# parameters need to write into csv file for further machine learning analysis:
# Current Timestamp (as time past midnight)
# Train Start Times
# Number of Active Trains
# Day of the week
# Elapsed times to express (96th St) station (averaged)
# Estimated time to reach destination station from 96th
# *********************************************************************************************

import csv
from threading import Thread
import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import aws, mtaUpdates, get_y
import time
import re

def buildStationssDB():
    stations = {}
    with open('stops.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[2] != 'stop_name' and row[0][-1] != 'N' and row[0][-1] != 'S':
                if row[2] in stations.keys():
                    stations[row[2]].append(row[0])
                else:
                    stations[row[2]] = [row[0]]

    return stations

def make_stopIdList(stop, direction, stations):
    L = []
    n = re.findall(r"\d+", stop)
    if n:
        for s in stations.keys():
            if n[0] in re.findall(r"\d+", s):
                for i in stations[s]:
                    L.append(i + direction)
    else:
        for s in stations.keys():
            if stop in s:
                for i in stations[s]:
                    L.append(i + direction)
    return L

def earliest96Train(items, stations, route):
    stops = make_stopIdList('96 St', 'S', stations)
    earliestTrain = {}
    earliestTime = {}
    for r in route:
        earliestTime[r] = float('+inf')
        for item in items:
            if item['routeId'] == r:
                for s in stops:
                    if item['currentStopId']:
                        if item['currentStopId']==s:
                            earliestTime[r] = 0
                            earliestTrain[r] = item
                            continue
                    if s in item['futureStops'].keys():
                        if item['futureStops'][s][0]['arrivalTime']:
                            if earliestTime[r] > item['futureStops'][s][0]['arrivalTime'] \
                                    and item['futureStops'][s][0]['arrivalTime'] > time.time():
                                earliestTime[r] = item['futureStops'][s][0]['arrivalTime'] - int(time.time())
                                earliestTrain[r] = item
    return earliestTime, earliestTrain


def get_info(threadname):
    try:
        while (1):
            time.sleep(30)
            table = dynamodb.Table('mtadata')
            stations = buildStationssDB()

            response = table.scan(FilterExpression=(Attr('routeId').eq('1') | Attr('routeId').eq('2') |
                                                    Attr('routeId').eq('3')) & Attr('direction').eq('S'))
            items = response['Items']
            # print(items)

            activeNumber = {}
            for item in items:
                if item['routeId'] not in activeNumber.keys():
                    activeNumber[item['routeId']] = 1
                else:
                    activeNumber[item['routeId']] += 1

            result = get_y.main(dynamodb)
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

                csvitem['estimatedTime'] = None
                for s1 in make_stopIdList('96 St', 'S', stations):
                    if s1 in item['futureStops'].keys():
                        if item['futureStops'][s1][1]['departureTime']:
                            for s2 in make_stopIdList('42 St', 'S', stations):
                                if s2 in item['futureStops'].keys():
                                    if item['futureStops'][s2][0]['arrivalTime']:
                                        csvitem['estimatedTime'] = item['futureStops'][s2][0]['arrivalTime'] \
                                                                   - item['futureStops'][s1][1]['departureTime']

                csvitem['result'] = result

                csvtable.append(list(csvitem.values()))
            print(csvtable)
            with open("mtadata.csv", "a", newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(csvtable)
                csvfile.close()
    except KeyboardInterrupt:
        exit(0)




dynamodb = aws.getResource('dynamodb', 'us-east-1')
csvthread = Thread(target=get_info, args=('csvthread',), daemon=True)

try:
    while 1:
        csvthread.start()
        while 1:
            pass
except KeyboardInterrupt:
    exit(0)
