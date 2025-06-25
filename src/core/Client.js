  import { MongoClient } from 'mongodb';
  import { getConnectionString } from './connections/Connections.js';

  export const getClient = async (isLocal = true) => {
    const connection_string = getConnectionString(isLocal);
    const client = await MongoClient.connect(connection_string, { useUnifiedTopology: true, useNewUrlParser: true })
      .catch((err) => { console.log('Error while connecting to MongoDB', err); });
    if (!client) return;
    return client;
  };

  export const closeClient = async (client) => {
    if (!client) return;
    await client.close();
    console.log('MongoDB connection closed');
  };

  export const getDB = async (client, dbName) => {
    if (!client) return;
    const db = await client.db(dbName);
    console.log('Connected to MongoDB database', dbName);
    return db;
  };
