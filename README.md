# elastic-geo-poc

## Tools and utils

1. docker & docker compose
2. nodejs & npm
3. elasticvue (to browse Elastic) - https://chrome.google.com/webstore/detail/elasticvue/hkedbapjpblbodpgbajblpnlpenaebaa?hl=en

You have to adjust receiving data from Mongo and transform it to the index schema

QueryExample: 

```
  {
    "query": {
      "bool": {
        "must": {
          "match_all": {}
        },
        "filter": {
          "geo_distance": {
            "distance": "500m",
            "location": {
              "lat": 58.321185,
              "lon": 92.420225
            }
          }
        }
      }
    }
  }
```
