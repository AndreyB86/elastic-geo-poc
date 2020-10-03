import moment from 'moment';
import * as mongodb from 'mongodb';
import { Client } from '@elastic/elasticsearch';
import { indexConfig } from './constants';

const client = new Client({ node: 'http://localhost:9200' })

const databaseName = 'indor_back';
const url = 'mongodb://mongo:mongo@192.168.100.6:27017';

async function connect(): Promise<any> {
  return new Promise((resolve, reject) => {
    mongodb.MongoClient.connect(url, {}, (err, client) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(client.db(databaseName));
    });
  });
}

async function getRawRoad(mongoDb, roadId): Promise<any> {
  return new Promise((resolve, reject) => {
    mongoDb.collection('Route').find({ _id: mongodb.ObjectId(roadId) }, {}).toArray((err, result) => {
      resolve(result[0]);
    });
  });
}

async function getTemplateRoad(mongoDb, roadId): Promise<any> {
  return new Promise((resolve, reject) => {
    mongoDb.collection('RouteOIS').find({ _id: mongodb.ObjectId(roadId) }, {}).toArray((err, result) => {
      resolve(result[0]);
    });
  });
}

async function doBulk(bulkData): Promise<boolean> {
  try {
    // @ts-ignore
    const result = await client.bulk({
      refresh: 'true',
      body: bulkData,
    });
    const { body } = result;
    if (body.errors) {
      console.log('doBulk()', 'errors during bulk operations execution');
      return false;
    }
    return true;
  } catch (e) {
    console.log('doBulk()', 'Exception during bulk operation execution');
  }
  return false;
}

async function processRoad(road, indexName: string) {
  let bulkDataToInsert = [];
  const points = road.points || [];
  let totalAdded = 0;
  console.log(`Road ${road.name} ${points.length} points`);
  for (let i = 0; i < points.length; i++) {
    const point = points[i];
    const docToInsert = {
      location: {
        lat: Number(point.latitude),
        lon: Number(point.longitude),
      },
      pk: point.picket,
      roadId: road.rawRouteId,
      roadName: road.name
    };
    bulkDataToInsert.push({ index: { _index: indexName } }, { ...docToInsert });
    if (bulkDataToInsert.length >= 10000) {
      totalAdded += bulkDataToInsert.length / 2;
      await doBulk(bulkDataToInsert);
      console.log(`${totalAdded} added`);
      bulkDataToInsert = [];
    }
  }
  if (bulkDataToInsert.length > 0) {
    await doBulk(bulkDataToInsert);
  }
  console.log(`Diagnostic proceed`);
}

async function initIndex(indexName: string) {
  try {
    await client.indices.delete({ index: indexName });
  } catch (e) {
  }
  await client.indices.create({
    index: indexName,
    body: indexConfig,
  });
}

async function findDocumentsByQuery(index: string, lat: number, lon: number): Promise<any[]> {
  const config = {
    index,
    body: {
      stored_fields: [
        '_source'
      ],
      query: {
        bool: {
          must: {
            match_all: {}
          },
          filter: {
            geo_distance: {
              distance: '50m',
              location: {
                lat,
                lon,
              }
            }
          }
        }
      },
      script_fields: {
        distance: {
          script: {
            params: {
              lat,
              lon,
            },
            inline: `Math.floor(doc['location'].planeDistance(params.lat,params.lon))`
          }
        }
      },
    },
  };
  const result = await client.search(config);
  const { hits } = result.body.hits;
  return hits.map((hit) => hit);
}

function getClosest(templateResult) {
  if (!templateResult || !templateResult.length) {
    return null;
  }
  const clearResults = templateResult.map((rawResult) => ({
    pk: rawResult._source.pk,
    location: rawResult._source.location,
    distance: rawResult.fields.distance[0],
  }));
  clearResults.sort((a, b) => a.distance - b.distance);
  const [finalResult] = clearResults;
  return finalResult;
}

async function syncRoads(indexName: string, rawRoad): Promise<void> {
  const points = rawRoad.clearPoints || [];
  console.info('Start syncing points', points.length);
  for (let i = 0; i < points.length; i++) {
    const templatePoints = await findDocumentsByQuery(indexName, Number(points[i].latitude), Number(points[i].longitude));
    if (templatePoints.length) {
      const closestPoint = getClosest(templatePoints);
      console.log(`Point ${i}: original pk ${Math.floor(points[i].picket / 100)} -> template picket ${closestPoint.pk} -> diff ${closestPoint.distance}m`);
      continue;
    }
    console.error(`Point ${i} found matches -> move out from track`);
  }
}

async function start() {
  console.clear();
  const mongoDb = await connect();
  const rawRoad = await getRawRoad(mongoDb, '5f489b43bd06504bcf3ed467');
  const templateRoad = await getTemplateRoad(mongoDb, '5f23d97ba2db9630cc40dc3f');
  const indexName = `${rawRoad._id.toString()}`;
  await initIndex(indexName);
  const timeStart = moment();
  console.info('Start processing', timeStart.toDate())
  await processRoad(templateRoad, indexName);
  await syncRoads(indexName, rawRoad);

  console.log('Template route points count', templateRoad.points.length);

  console.log('finished');

  const timeStop = moment();
  console.info('Stop processing', timeStop.toDate());
  console.info(`Processing time ${timeStop.diff(timeStart, 'second')}s`);

  // @ts-ignore
  process.exit(0)
}

start();
