import gzip
import json

with gzip.open('restaurants.ndjson.gz', 'rt', encoding='utf-8') as f:
    for line in f:
        restaurant = json.loads(line)
        print(restaurant)
