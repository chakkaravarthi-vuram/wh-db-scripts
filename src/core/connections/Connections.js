import { server_connection_string } from './server.js';
import { local_connection_string } from './local.js';
import dotenv from 'dotenv';
dotenv.config();

const { ENV } = process.env;

const getConnectionString = () => {
    if (ENV === 'dev') {
        return local_connection_string;
    }
    return server_connection_string;
}

export { getConnectionString };