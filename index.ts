import * as mongodb from 'mongodb';
import { Client } from '@elastic/elasticsearch';
import { indexConfig, indexName } from './constants';

const client = new Client({ node: 'http://localhost:9200' })

const databaseName = 'ois';
const url = 'mongodb://localhost';

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

async function getAllRoads(mongoDb): Promise<any> {
  return new Promise((resolve, reject) => {
    mongoDb.collection('roads').find({}, { order: { name: -1 } }).toArray((err, result) => {
      resolve(result);
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

async function processRoad(road) {
  let bulkDataToInsert = [];
  const { points: rawPoints } = road;
  const points = rawPoints || [];
  let totalAdded = 0;
  console.log(`Road ${road.name} in progress with ${points.length} points`);
  for (let i = 0; i < points.length; i++) {
    const point = points[i];
    const docToInsert = {
      location: {
        lat: Number(point.lat),
        lon: Number(point.lon),
      },
      pk: point.pk,
      roadId: road.roadId,
      roadName: road.name
    };
    bulkDataToInsert.push({ index: { _index: 'geoindex' } }, { ...docToInsert });
    if (bulkDataToInsert.length >= 10000) {
      totalAdded += bulkDataToInsert.length;
      await doBulk(bulkDataToInsert);
      console.log(`${totalAdded} added`);
      bulkDataToInsert = [];
    }
  }
  if (bulkDataToInsert.length > 0) {
    await doBulk(bulkDataToInsert);
  }
  console.log(`Road ${road.name} processed. ${points.length} points added`);
}

async function initIndex() {
  await client.indices.delete({ index: indexName });
  await client.indices.create({
    index: indexName,
    body: indexConfig,
  });
}

async function start() {
  const mongoDb = await connect();
  await initIndex();
  const roads = await getAllRoads(mongoDb);

  for (let i = 0; i < roads.length; i++) {
    await processRoad(roads[i]);
    console.log(`Road ${i+1} from ${roads.length} processed.`);
  }

  console.log('finished');
}

start();
