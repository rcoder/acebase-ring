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

const serverConfig = {
    logLevel: 'warn',
    ...config.server[serverName]
};

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

const local = new AceBase(`${dbname}_local`, {
    logLevel: config.shared.logLevel,
    storage: storageOptions,
});

const clientConfig = {
    dbname,
    cache: { db: local, },
    ...serverConfig,
};

const client = new AceBaseClient(clientConfig);

const ring = new HashRing();
const connections: Map<string, AceBaseClient> = new Map();
const removed: Set<string> = new Set();

const connectTo = async (key: string) => {
    if (connections.has(key)) {
        return connections.get(key);
    }

    const conf = config.server[key];

    const client = new AceBaseClient({ dbname, ...conf });

    client.on('disconnect', () => {
        console.log(`server '${key}' connection lost; removing from ring`);
        ring.removeNode(key);
        connections.delete(key);
        removed.add(key);
    });

    client.ready().then(() => {
        console.log(`connected to server '${key}'`);
        ring.addNode(key);
        connections.set(key, client);
    }).catch((err) => {
        console.log(`failed to connect to server '${key}'`, { err });
    });

    return client;
}

const buildRing = async () => {
    const serverNames = Object.keys(config.server);

    for (let key of serverNames) {
        if (key == serverName) continue;
        connectTo(serverName);
    }
}

const checkRemoved = () => {
    for (let key in removed) {
        connectTo(key)
            .then(() => {
                removed.delete(key)
            })
            .catch(async (err: any) => {
                await logMsg(`connection failed`, { key, err })
            });
    }
}

const RING_CHECK_INTERVAL = 5000;
setInterval(checkRemoved, RING_CHECK_INTERVAL);

const replicaFor = async (root: string) => {
    const upstream = ring.getNode(root);
    let conn: AceBaseClient | undefined;

    if (upstream === undefined) {
        await logMsg(`all peer nodes marked down`, { ring });
    } else {
        conn = await connectTo(upstream);
    }

    return conn;
}

const SAMPLE_RATE = 0.1;
const SAMPLE_COUNT = 100;

type Sample = { path: string, inner: { msg: string, ts: Date } };
let sampledValues: Sample[] = [];

const populateRing = async () => {
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

        if (Math.random() < SAMPLE_RATE) {
            sampledValues.push({ path: objPath, inner: msg });
            sampledValues = [...sampledValues].slice(-SAMPLE_COUNT);
        }

        const upstream = await replicaFor(objPath);

        if (upstream === undefined) {
            await logMsg(`failed to get replica for '${obj}'`, {});
            failure += 1;
        } else {
            upstream.ref(ref.path)
                .set(msg)
                .then(() => { success += 1; })
                .catch(async (err: any) => {
                    await logMsg(`failed to update '${obj}' on peer`, {});
                    failure += 1;
                });
        }
    }

    const count = await client.ref('/test').count();
    const localCount = await local.ref('/ring/cache/test').count();

    await logMsg(`new records added`, {
        success,
        failure,
        count,
        localCount,
    });
}

const logMsg = async (msg: string, extra: any) => {
    await fs.appendFile(
        `data/ring_test/${serverName}/report.ndjson`,
        `${JSON.stringify({ msg, ...extra })}\n`,
        console.log,
    );
}

const testRing = async () => {
    for (let sample of sampledValues) {
        const { path, inner } = sample;
        const proxy = await client.ref(path).proxy();
        const val = await proxy.value;
        const vmsg = val.msg;
        const imsg = inner.msg;

        if (imsg != vmsg) {
            await logMsg(`mismatched sample value for '${sample.path}`, { path, imsg, vmsg });
        }
    }
}

setInterval(populateRing, (1+Math.random()/10)*5000);
setInterval(testRing, (1+Math.random()/10)*5000);
