import { server_connection_string } from './server.js';
import { local_connection_string } from './local.js';

const getConnectionString = (isLocal = true) => {
    if (isLocal) {
        return local_connection_string;
    }
    return server_connection_string;
}

export { getConnectionString };