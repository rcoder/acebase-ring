import { AceBaseClient } from 'acebase-client';
import toml from 'toml';
import fs from 'fs';

const config = toml.parse(
    fs.readFileSync(
        `${__dirname}/../conf/servers.toml`,
        'utf-8'
    )
);

const client = new AceBaseClient({
    dbname: `ring`,
    ...config.server.a
});

const dumpObjects = async () => {
    const ref = client.ref('/test/a');
    const proxy = await ref.proxy();
    const val = await proxy.value;
    console.log({ val: [...val.entries()] });
    const count = await ref.count();
    console.log(`${count} total entries found`);
    client.disconnect();
}

dumpObjects();
