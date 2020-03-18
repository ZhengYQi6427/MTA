import urllib,contextlib 
import urllib.request 
from datetime import datetime 
from collections import OrderedDict 
from google.protobuf.json_format import MessageToJson 
from pytz import timezone 
import gtfs_realtime_pb2
import google.protobuf 
import vehicle,alert,tripupdate 
import json

def get_str_btw(s, f, b):
    par = s.partition(f)
    return (par[2].partition(b))[0][:] 

class mtaUpdates(object):
    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')
    
    # feed url depends on the routes to which you want updates here we are 
    # using feed 1 , which has lines 1,2,3,4,5,6,S While initializing we can 
    # read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    
    VCS = {0:"INCOMING_AT", 1:"STOPPED_AT", 2:"IN_TRANSIT_TO"}
    tripUpdates = []
    alerts = []
    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey
    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib.request.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except:
            print('connection error')
            # return "ERROR"
        '''
        except (urllib.error.URLError, google.protobuf.message.DecodeError) as e:\
            print ("Error while connecting to mta server " +str(e))'''
        timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)

        self.tripUpdates = []
        for entity in feed.entity:
            # Trip update represents a change in timetable
            duplicate = 0
            if entity.trip_update and entity.trip_update.trip.trip_id:
                update = tripupdate.tripupdate()
                update.tripId = str(entity.trip_update.trip.trip_id)
                update.routeId = str(entity.trip_update.trip.route_id)
                update.startDate = str(entity.trip_update.trip.start_date)
                update.direction = 'N' if 'N' in update.tripId else 'S'
                if entity.trip_update.stop_time_update:
                                for stop_update in entity.trip_update.stop_time_update:
                                    arrival_time = stop_update.arrival.time if stop_update.HasField('arrival') else ' '
                                    departure_time = stop_update.departure.time if stop_update.HasField('departure') else ' '
                                    update.futureStops[str(stop_update.stop_id)] = [{"arrivalTime":arrival_time}, {"departureTime":departure_time}]
                for tripUpdate in self.tripUpdates:
                        if tripUpdate.tripId == update.tripId:
                                duplicate = 1
                                tripUpdate.futureStops = update.futureStops
                if duplicate == 0:
                        self.tripUpdates.append(update)

            if entity.vehicle and entity.vehicle.trip.trip_id:
                v = vehicle.vehicle()
                v.currentStopSequence = str(entity.vehicle.current_stop_sequence)
                v.currentStopId = str(entity.vehicle.stop_id)
                v.currentStopStatus = str(entity.vehicle.current_status)
                v.timestamp = entity.vehicle.timestamp
                for tripUpdate in self.tripUpdates:
                    if tripUpdate.tripId == str(entity.vehicle.trip.trip_id):
                       tripUpdate.vehicleData = v
                       duplicate = 1
                if duplicate == 0:
                        update = tripupdate.tripupdate()
                        update.tripId = str(entity.vehicle.trip.trip_id)
                        update.routeId = str(entity.vehicle.trip.route_id)
                        update.startDate = str(entity.vehicle.trip.start_date)
                        update.direction = 'N' if 'N' in update.tripId else 'S'
                        update.vehicleData = v
                        self.tripUpdates.append(update)		
            if entity.alert:
                a = alert.alert()
        return [self.tripUpdates,timestamp]
    # END OF getTripUpdates method
