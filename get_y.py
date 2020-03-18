# *********************************************************************************************
# Including functions to provide the true best switch choice as result y
# for further machine learning analysis
# *********************************************************************************************

import json, time, csv, sys

import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import re

sys.path.append('../utils')


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


##############################################
# YOU MAY USE THESE METHODS, OR CODE YOUR OWN
##############################################
# Method to get all local going in a given direction on a give routeId and haven't passed sourceStopId
def getLocalTrains(table, direction, routeId, sourceStopId):
    ###############################
    # YOUR CODE HERE #
    tripId = []
    response = []
    r = table.scan(
        FilterExpression=Attr('routeId').eq(routeId)
                         & Attr('direction').eq(direction)
    )
    items = r['Items']
    for s in sourceStopId:
        r = table.scan(
            FilterExpression=Attr('routeId').eq(routeId)
                             & Attr('direction').eq(direction)
        )
        items = r['Items']
        if items:
            for i in items:
                if 'currentStopId' in i.keys():
                    if i['currentStopId'] == s and i['currentStopStatus'] != 'IN_TRANSIT_TO':
                        response.append(i)
                        tripId.append(i['tripId'])
                        continue
                if s in i['futureStops'].keys():
                    if i['futureStops'][s][0]['arrivalTime'] != ' ':
                        if i['futureStops'][s][0]['arrivalTime'] > time.time():
                            response.append(i)
                            tripId.append(i['tripId'])

    ###############################
    return response


# Method to get all express going in a given direction on a give routeId and are at given list of stops
def getExpress(table, direction, routeId, stops):
    ###############################
    # YOUR CODE HERE #
    tripId = []
    response = []
    for s in stops:
        r = table.scan(
            FilterExpression=Attr('routeId').eq(routeId)
                             & Attr('direction').eq(direction)
        )
        items = r['Items']
        if items:
            for i in items:
                if s in i['futureStops'].keys():
                    response.append(i)
    ###############################
    return response


# Method to get the earliest train's data
def getEarliestTrain(response, destination):
    ###############################
    # YOUR CODE HERE #
    t = float('+inf')
    trainData = {}
    for item in response:
        for dest in destination:
            if dest in item['futureStops'].keys():
                if item['futureStops'][dest][0]['arrivalTime'] != None:
                    if t > int(item['futureStops'][dest][0]['arrivalTime']):
                        t = int(item['futureStops'][dest][0]['arrivalTime'])
                        trainData = item
    # return [earliest_tripId, t]
    ###############################
    return trainData, t


def getTimeToReachDestination(trainData, destination):
    ###############################
    # YOUR CODE HERE #
    for dest in destination:
        if dest in trainData['futureStops'].keys():
            arrivalTime = trainData['futureStops'][dest][0]['arrivalTime']
            return arrivalTime
    ###############################
    return 0

def t_to_datetime(t):
    return datetime.fromtimestamp(t)

def get_optimal(earliestTime):
    optimalRoute = min(earliestTime, key=lambda x: earliestTime[x])
    return optimalRoute

def planTrip(localRoute, expressRoute, direction, dynamoTable, sourceStop, destinationStop, transferStops, stations):

    localTrainData = {}
    expressTrainData = {}
    availableExpress = {}
    earliestTrain = {}

    sourceStopId = make_stopIdList(sourceStop, direction, stations)
    destination = make_stopIdList(destinationStop, direction, stations)

    stops = []

    if direction == 'N':
        firstTransfer = make_stopIdList(transferStops[-1], direction, stations)
        lastTransfer = make_stopIdList(transferStops[0], direction, stations)
    else:
        firstTransfer = make_stopIdList(transferStops[0], direction, stations)
        lastTransfer = make_stopIdList(transferStops[-1], direction, stations)

    for s in transferStops:
        ss = make_stopIdList(s, direction, stations)
        for stop in ss:
            stops.append(stop)

    for route in localRoute:
        # get all tripId of all local routes passing through the sourceStop
        localTrainData[route] = getLocalTrains(dynamoTable, direction, route, sourceStopId)
        # get the earliest train of each route to reach the 96 St
        if localTrainData[route]:
            earliestTrain[route], t = getEarliestTrain(localTrainData[route], make_stopIdList('96 St', direction, stations))
            # print('The earliest train of route ' + route + ' that reaches 96 St: ' + earliestTrain[route]['tripId'])
        else:
            continue

    earliestTime = {}
    for route in earliestTrain.keys():
        if earliestTrain[route]:
            earliestTime[route] = getTimeToReachDestination(earliestTrain[route], destination)

    if any(earliestTime[route] != 0 for route in earliestTime.keys()):
        optimalLocalRoute = min(earliestTime, key=lambda x: earliestTime[x])
    else:
        # print('Unable to plan your trip: lack of local train information')
        return 'null'

    # 4 kinds of trip:
    # 1. Start and end at a transfer stop
    # 2. Start at a transfer stop
    # 3. End at a transfer stop
    # 4. Neither start or end at a transfer stop

    # 3. End at a transfer stop (eg, >96 St to 42 St)

    if sourceStop not in transferStops and destinationStop in transferStops:
        flag = 0
        t_lTrain_Reach_Transfer = getTimeToReachDestination(earliestTrain[optimalLocalRoute], firstTransfer)

        for route in expressRoute:
            # get all tripId of all express routes passing through the sourceStop
            expressTrainData[route] = getExpress(dynamoTable, direction, route, stops)
            tripId = []
            for i in expressTrainData[route]:
                tripId.append(i['tripId'])

            # get express available for transfer
            availableExpress[route] = []
            for item in expressTrainData[route]:
                for stop in firstTransfer:
                    if stop in item['futureStops'].keys():
                        if t_lTrain_Reach_Transfer < item['futureStops'][stop][1]['departureTime']:
                            availableExpress[route].append(item)
                            break

            # get the earliest express train available for transfer
            earliestTrain[route], t = getEarliestTrain(availableExpress[route], destination)
            # print('The earliest train of route ' + route + ' that reaches 96 St: ' + earliestTrain[route]['tripId'])

            if earliestTrain[route]:
                earliestTime[route] = getTimeToReachDestination(earliestTrain[route], destination)
                if earliestTime[route] == 0:
                    earliestTime.pop(route)
            else:
                continue

    optimalRoute = get_optimal(earliestTime)
    timetoDest = {'1': None, '2': None, '3': None}
    for r in earliestTime.keys():
        t = datetime.fromtimestamp(earliestTime[r]).time()
        timetoDest[r] = t.hour * 60 + t.minute + (0 if t.second < 30 else 1)
    return optimalRoute, timetoDest

def main(dynamodb):

    dynamoTable = dynamodb.Table("mtadata")

    # Get list of all stopIds
    stations = buildStationssDB()
    # print(stations)

    localRoute = ['1']
    expressRoute = ['2', '3']

    transferStops = ["96 St", "42 St"]

    # if it is not available to transfer
    # for example, from 116 St to 103 St

    direction = 'S'
    sourceStop = '116 St'
    destinationStop = '42 St'

    route = planTrip(localRoute, expressRoute, direction, dynamoTable, sourceStop, destinationStop, transferStops, stations)
    return route

