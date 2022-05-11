import { HashRing } from 'ketama';
import minimist from 'minimist';
import toml from 'toml';
import fs from 'fs';
import { config as parseEnv } from 'dotenv';
import { AceBase } from 'acebase';
import { AceBaseClient } from 'acebase-client';
import { AceBaseServer } from 'acebase-server';

parseEnv();

const argv = minimist(process.argv.slice(2));
const config = toml.parse(fs.readFileSync(`${__dirname}/../conf/servers.toml`, 'utf-8'));

console.log(`config loaded`, JSON.stringify(config, null, 2));

const dbname = config.shared.dbname;
const adminPassword = process.env.DB_ADMIN_PASSWORD;

if (adminPassword === 'undefined') {
    console.error('missing DB_ADMIN_PASSWORD environment variable');
    process.exit(1);
}

type ConfigEntry = {
    host: string,
    port: number,
    https: boolean,
};

const serverName = argv.name;

if (serverName === undefined) {
    console.error("missing --name argument");
    process.exit(1);
}

const serverConfig = config.server[serverName];

console.log({ servers: Object.keys(config.server) });

if (serverConfig === undefined) {
    console.error(`server ${serverName} not found in servers.toml`);
    process.exit(1);
}

const storageOptions = {
    path: `data/ring_test/${serverName}`,
};

const server = new AceBaseServer(`ring`, {
    authentication: {
        enabled: false,
        defaultAdminPassword: adminPassword,
    },
    ...storageOptions,
    ...serverConfig
});

const local = new AceBase(`${dbname}_local`, { storage: storageOptions });

const clientConfig = {
    dbname,
    cache: { db: local, },
    ...serverConfig,
};

const client = new AceBaseClient(clientConfig);

type CacheEntry = {
    key: string,
    client: AceBaseClient,
};

const ring = new HashRing<CacheEntry>();
const connections: Map<string, AceBaseClient> = new Map();
const removed: Set<string> = new Set();

let clientCache: {[K: string]: CacheEntry} = {};

const buildRing = async () => {
    const serverNames = Object.keys(config.server);

    for (let key of serverNames) {
        if (key == serverName) continue;
        if (connections.has(key)) continue;

        const conf = config.server[key];

        const client = new AceBaseClient({ dbname, ...conf });

        const cacheEntry = { key, client };

        client.on('disconnect', () => {
            console.log(`server '${cacheEntry.key}' connection lost; removing from ring`);
            ring.removeNode(cacheEntry);
            connections.delete(key);
            removed.add(key);
        });

        client.ready().then(() => {
            console.log(`connected to server '${cacheEntry.key}'`);
            ring.addNode(cacheEntry);
            connections.set(key, client);
        }).catch((err) => {
            console.log(`failed to connect to server '${key}'`, { err });
        });
    }
}

const checkRemoved = async () => {
    console.log(`checkRemoved:begin`, { removed });
    for (let key in removed) {
        try {
            const sc = config.server[key]!;
            const client = new AceBaseClient({ dbname, ...sc });
            await client.ready();

            ring.addNode({ key, client });
            removed.delete(key);
            console.info(`reconnected to server '${key}'`);
        } catch (err) {
            console.error(`failed to reconnect to server '${key}'`, { err });
        }
    }
    console.log(`checkRemoved:end`, { removed });
}

const RING_CHECK_INTERVAL = 5000;
setInterval(checkRemoved, RING_CHECK_INTERVAL);

const replicaFor = async (root: string) => {
    let cacheEntry = clientCache[root];
    let client: AceBaseClient | undefined = cacheEntry?.client;

    if (client === undefined) {
        console.log(`no cached connection for '${root}'; trying next host`);
        const upstream = ring.getNode(root);

        if (upstream === undefined) {
            console.error(`all peer nodes down!`);
        } else {
            console.debug(`connecting to '${upstream.key}'`);
            client = upstream!.client;
            clientCache[root] = upstream;
        }
    }

    return client;
}

const testRing = async () => {
    await buildRing();

    await server.ready();
    await client.ready();

    let success = 0;
    let failure = 0;

    for (let obj of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) {
        const msg = { msg: `testing for ref ${obj}`, ts: new Date() };
        const objPath = `/test/${obj}`;
        const ref = client.ref(objPath).push();
        await ref.set(msg);

        const upstream = await replicaFor(objPath);

        if (upstream === undefined) {
            console.error(`failed to get replica for '${obj}'`);
            failure += 1;
        } else {
            try {
                await upstream.ref(ref.path).set(msg);
                success += 1;
            } catch (err) {
                console.error(`failed to update '${obj}' on peer`);
                failure += 1;
            }
        }
    }

    console.info(`test finished; ${success} updates okay, ${failure} failed`);
    const count = await client.ref('/test').count();
    const localCount = await local.ref('/test').count();
    console.info(`${count} total objects in server db (${localCount} in cache)`);
}

const timer = setInterval(testRing, 5000);
timer.unref();
