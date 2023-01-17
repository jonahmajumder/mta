from google.transit import gtfs_realtime_pb2
import requests
from itertools import groupby
from mta_classes import SubwayTrip

# feed URLs available at https://api.mta.info/#/subwayRealTimeFeeds
URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs'
KEY = 'r9T8w9WnQUC00HNIRMQe2Uk5X5bPH0DCNYAxlp90'

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get(URL, headers={'x-api-key': KEY})
response.raise_for_status()

feed.ParseFromString(response.content)

if __name__ == '__main__':
    # each entity should have one of the following fields: 'trip_update', 'vehicle', 'alert', 'shape'
    trip_updates = [e.trip_update for e in feed.entity if e.HasField('trip_update')]

    updates_by_train = {k:list(g) for k,g in groupby(trip_updates, key=lambda t: t.trip.route_id)}

    for trip_update in updates_by_train['1']:
        tr = SubwayTrip(trip_update)

        print(tr)
        for stop in tr.stops:
            if hasattr(stop, 'departs'):
                print(stop, stop.departs.strftime('%I:%M %p').lstrip('0'))
            else:
                print(stop)

        print('')
