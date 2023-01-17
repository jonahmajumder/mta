from gtfs_realtime_pb2 import FeedMessage
import requests

# feed URLs available at https://api.mta.info/#/subwayRealTimeFeeds
URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs'
KEY = 'r9T8w9WnQUC00HNIRMQe2Uk5X5bPH0DCNYAxlp90'

response = requests.get(URL, headers={'x-api-key': KEY})
response.raise_for_status()

feed = FeedMessage()
feed.ParseFromString(response.content)

print(feed)
