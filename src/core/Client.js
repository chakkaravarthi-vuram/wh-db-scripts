  import chalk from 'chalk';
  import { MongoClient } from 'mongodb';
  import { getConnectionString } from './connections/Connections.js';

  export const getClient = async () => {
    const connection_string = getConnectionString();
    const client = await MongoClient.connect(connection_string, { useUnifiedTopology: true, useNewUrlParser: true })
      .catch((err) => { console.log('❌ Error while connecting to MongoDB', err); });
    if (!client) return;
    return client;
  };

  export const closeClient = async (client) => {
    if (!client) return;
    await client.close();
    console.log(`${chalk.bgGreen('MongoDB connection closed')}`);
  };

  export const getDB = async (client, dbName) => {
    if (!client) return;
    const db = await client.db(dbName);
    console.log(`${chalk.hex('#77DD77')('✅ Connected to MongoDB database')} ${chalk.greenBright.bold(dbName)}`);
    return db;
  };
