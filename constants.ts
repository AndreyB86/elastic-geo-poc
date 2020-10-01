export const indexConfig = {
  mappings: {
    properties: {
      location: {
        type: 'geo_point'
      },
      roadId: {
        type: 'integer'
      },
      roadName: {
        type: 'text'
      },
      pk: {
        type: 'integer'
      }
    }
  }
};

export const indexName = 'geoindex';

const queryExample = {
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
