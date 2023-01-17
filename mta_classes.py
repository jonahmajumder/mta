import re
import pandas as pd
from datetime import datetime
import pytz

STATION_INFO_CSV_URL = 'https://atisdata.s3.amazonaws.com/Station/Stations.csv'
STATION_INFO = pd.read_csv(STATION_INFO_CSV_URL)

class SubwayTrip(object):
    """
    SubwayTrip: represents TripUpdate object
    """
    TRIP_ID_RE = re.compile(r'(?P<id>\d+)_(?P<line>[A-Z\d]{1,2})\.{1,2}(?P<direction>[N|S])(?P<suffix>[A-Z\d]*)')

    def __init__(self, trip_update):
        super(SubwayTrip, self).__init__()

        self._parse_trip(trip_update.trip)

        self.stops = [SubwayStop(update) for update in trip_update.stop_time_update]

        self.last_stop = self.stops[-1]
        
    def _parse_trip(self, trip):
        parsed_trip_dict = self.TRIP_ID_RE.match(trip.trip_id).groupdict()

        self.id = parsed_trip_dict['id']
        self.direction_id = parsed_trip_dict['direction']
        self.line = trip.route_id
        self.start_date = datetime.date(datetime.strptime(trip.start_date, '%Y%m%d'))

    def has_stop(self, stop_id):
        return stop_id in [s.stop_id for s in self.stops]

    def get_stop(self, stop_id):
        return self.stops[[s.stop_id for s in self.stops].index(stop_id)]

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.line} train, {self.direction_id}-bound (#{self.id})>"

class SubwayStop(object):
    """
    SubwayStop: represents StopTimeUpdate object
    """
    STOP_ID_RE = re.compile(r'(?P<stop_number>\d{3})(?P<direction>[N|S])?')

    def __init__(self, stop_time_update):
        super(SubwayStop, self).__init__()

        self._parse_stop(stop_time_update)

    @staticmethod
    def utc_to_local_datetime(time):
        if isinstance(time, (float, int)):
            dt = datetime.utcfromtimestamp(time)
        elif isinstance(time, datetime):
            dt = time
        else:
            raise Exception(f"Input type '{type(time)}' not supported")

        return pytz.utc.localize(dt).astimezone(pytz.timezone('US/Eastern'))

    @staticmethod
    def stop_id_to_station_dict(stop_id):
        parsed_stop_id_dict = SubwayStop.STOP_ID_RE.match(stop_id).groupdict()

        station_dict = {}
        station_dict['direction_id'] = parsed_stop_id_dict['direction']

        matching_rows = STATION_INFO[STATION_INFO['GTFS Stop ID'] == parsed_stop_id_dict['stop_number']]
        assert(matching_rows.shape[0] == 1)
        row_dict = matching_rows.to_dict(orient='records')[0]
        row_dict = {k:v for k,v in row_dict.items() if not pd.isna(v)} # remove nan values

        station_dict['station_id'] = row_dict['Station ID']
        station_dict['complex_id'] = row_dict['Complex ID']
        station_dict['stop_id'] = row_dict['GTFS Stop ID']
        station_dict['division'] = row_dict['Division']
        station_dict['line_name'] = row_dict['Line']
        station_dict['stop_name'] = row_dict['Stop Name']
        station_dict['borough_id'] = row_dict['Borough']
        station_dict['station_routes'] = row_dict['Daytime Routes'].split()
        station_dict['structure'] = row_dict['Structure']
        station_dict['latitude'] = row_dict['GTFS Latitude']
        station_dict['longitude'] = row_dict['GTFS Longitude']

        if 'North Direction Label' in row_dict:
            station_dict['north_direction_label'] = row_dict['North Direction Label']
        if 'South Direction Label' in row_dict:
            station_dict['south_direction_label'] = row_dict['South Direction Label']

        return station_dict
        
    def _parse_stop(self, update):

        for k,v in self.stop_id_to_station_dict(update.stop_id).items():
            setattr(self, k, v)

        if update.HasField('arrival'):
            self.arrives = self.utc_to_local_datetime(update.arrival.time)

        if update.HasField('departure'):
            self.departs = self.utc_to_local_datetime(update.departure.time)

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self.stop_name}' ({'/'.join(self.station_routes)}), {self.direction_id}-bound>"
        