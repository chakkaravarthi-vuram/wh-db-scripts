// import { config } from 'dotenv';
// config();

// // AWS PARAM STORE CONFIGURATION
// const awsParamStore = require('aws-param-store');
// const environment = process.env.ENV;
// const mongodb_protocol = `/microservices/${environment}/mongodb_protocol`;

// const db = `/microservices/${environment}/database_connection_string`;
// const service_credentials = `/microservices/${environment}/backend_database_credentials`;

// console.log('AWS IS GOING TO FETCH');
// let results = awsParamStore.getParametersSync([service_credentials, db, mongodb_protocol],
//     { region: 'ap-south-1' });

// console.log('RESULTS -1 =>', results);

// if (results.Parameters.length < 2) {
//     console.log('Invalid Params', results.InvalidParameters);
// }

// const params = {};
// console.log('RESULTS =>', results);
// results.Parameters.forEach(item => {
//     let { Name, Value } = item;
//     params[Name] = Value;
// });

// params[mongodb_protocol] = params[mongodb_protocol] ? params[mongodb_protocol] : 'mongodb';
// export const server_connection_string = `${params[mongodb_protocol]}://${params[service_credentials]}@${params[db]}`;
export const server_connection_string = `server`;

