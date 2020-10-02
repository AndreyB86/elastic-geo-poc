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

async function processRoad(road, indexName) {
  let bulkDataToInsert = [];
  const points = road.clearPoints || [];
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
      totalAdded += bulkDataToInsert.length;
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

async function initIndex(indexName) {
  try {
    await client.indices.delete({ index: indexName });
  } catch (e) {
  }
  await client.indices.create({
    index: indexName,
    body: indexConfig,
  });
}

async function start() {
  const mongoDb = await connect();
  // await initIndex();
  const rawRoad = await getRawRoad(mongoDb, '5f489b43bd06504bcf3ed467');
  const indexName = `${rawRoad._id.toString()}`;
  await initIndex(indexName);
  await processRoad(rawRoad, indexName);
  const templateRoad = await getTemplateRoad(mongoDb, '5f23d97ba2db9630cc40dc3f');

  console.log('Template route points count', templateRoad.points.length);



  console.log('finished');

  // @ts-ignore
  process.exit(0)
}

start();
