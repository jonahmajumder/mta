from google.transit import gtfs_realtime_pb2
import requests
from datetime import datetime
from itertools import groupby
from mta_classes import SubwayTrip, SubwayStop

# feed URLs available at https://api.mta.info/#/subwayRealTimeFeeds
URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs'
KEY = 'r9T8w9WnQUC00HNIRMQe2Uk5X5bPH0DCNYAxlp90'

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get(URL, headers={'x-api-key': KEY})
try:
    response.raise_for_status()
except requests.exceptions.HTTPError as exc:
    if response.status_code == 403:
        print('Forbidden! Visit https://api.mta.info/#/AccessKey to renew API key.')
    raise(exc)

feed.ParseFromString(response.content)

# each entity should have one of the following fields: 'trip_update', 'vehicle', 'alert', 'shape'
trip_updates = [e.trip_update for e in feed.entity if e.HasField('trip_update')]

updates_by_train = {k:list(g) for k,g in groupby(trip_updates, key=lambda t: t.trip.route_id)}

MY_STOP_ID = '118'
my_stop_info = SubwayStop.stop_id_to_station_dict(MY_STOP_ID)

NUM_TRAINS = 3
now = SubwayStop.utc_to_local_datetime(datetime.utcnow())

trips = [SubwayTrip(trip_update) for trip_update in updates_by_train['1']]
trips_here = [tr for tr in trips if tr.has_stop(MY_STOP_ID)]
southbound_trips_here = [tr for tr in trips_here if tr.direction_id == 'S']
northbound_trips_here = [tr for tr in trips_here if tr.direction_id == 'N']

southbound_stops_here = [tr.get_stop(MY_STOP_ID) for tr in southbound_trips_here]
northbound_stops_here = [tr.get_stop(MY_STOP_ID) for tr in northbound_trips_here]

assert(all([hasattr(st, 'arrives') for st in southbound_stops_here]))
assert(all([hasattr(st, 'arrives') for st in northbound_stops_here]))

southbound_stops_here.sort(key=lambda st: st.arrives)
northbound_stops_here.sort(key=lambda st: st.arrives)

print('')
print(f"{my_stop_info['stop_name']} ({my_stop_info['line_name']} Line)")
print('')

print(f'{southbound_stops_here[0].south_direction_label} (to {southbound_trips_here[0].last_stop.stop_name}):')
for st in southbound_stops_here[:NUM_TRAINS]:
    formatted_arrival_time = st.arrives.strftime('%I:%M %p')
    min_away = round((st.arrives - now).seconds/60)
    print(f"{formatted_arrival_time} ({min_away} min)")

print('')

print(f'{northbound_stops_here[0].north_direction_label} (to {northbound_trips_here[0].last_stop.stop_name}):')
for st in northbound_stops_here[:NUM_TRAINS]:
    formatted_arrival_time = st.arrives.strftime('%I:%M %p')
    min_away = round((st.arrives - now).seconds/60)
    print(f"{formatted_arrival_time} ({min_away} min)")
