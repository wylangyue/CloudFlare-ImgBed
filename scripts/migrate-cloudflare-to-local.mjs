import { mkdir, readFile, rm, stat } from 'node:fs/promises';
import { dirname, join, resolve, sep } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { SqliteD1 } from '../deploy/server/sqliteD1.js';
import { D1Database } from '../functions/utils/d1Database.js';

const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
const apiToken = process.env.CLOUDFLARE_API_TOKEN;
const namespaceId = process.env.KV_NAMESPACE_ID;
const bucketName = process.env.R2_BUCKET_NAME;

if (!accountId || !apiToken || !namespaceId || !bucketName) {
    throw new Error('CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, KV_NAMESPACE_ID and R2_BUCKET_NAME are required');
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = join(root, 'data');
const dbPath = join(dataDir, 'database.sqlite');
const r2Dir = join(dataDir, 'r2');
const wranglerPath = join(root, 'node_modules', '.bin', 'wrangler');
const apiBase = `https://api.cloudflare.com/client/v4/accounts/${accountId}/storage/kv/namespaces/${namespaceId}`;

async function cloudflare(path) {
    const response = await fetch(`${apiBase}${path}`, {
        headers: { Authorization: `Bearer ${apiToken}` },
    });
    if (!response.ok) {
        throw new Error(`Cloudflare API ${response.status} for ${path}`);
    }
    return response;
}

async function listKeys() {
    const keys = [];
    let cursor = '';
    do {
        const query = new URLSearchParams({ limit: '1000' });
        if (cursor) query.set('cursor', cursor);
        const payload = await cloudflare(`/keys?${query}`).then(response => response.json());
        if (!payload.success) throw new Error(JSON.stringify(payload.errors));
        keys.push(...payload.result);
        cursor = payload.result_info?.cursor || '';
    } while (cursor);
    return keys;
}

async function mapConcurrent(items, concurrency, task) {
    let cursor = 0;
    const workers = Array.from({ length: concurrency }, async () => {
        while (cursor < items.length) {
            const item = items[cursor++];
            await task(item);
        }
    });
    await Promise.all(workers);
}

function safeObjectPath(key) {
    const destination = resolve(r2Dir, key);
    if (!destination.startsWith(`${resolve(r2Dir)}${sep}`)) {
        throw new Error(`Unsafe R2 object key: ${key}`);
    }
    return destination;
}

async function downloadR2Object(key) {
    const destination = safeObjectPath(key);
    await mkdir(dirname(destination), { recursive: true });
    const childEnv = { ...process.env };
    delete childEnv.CLOUDFLARE_API_TOKEN;

    await new Promise((resolvePromise, reject) => {
        const child = spawn(wranglerPath, [
            'r2', 'object', 'get', `${bucketName}/${key}`,
            '--file', destination, '--remote',
        ], { cwd: root, env: childEnv, stdio: ['ignore', 'ignore', 'pipe'] });
        let stderr = '';
        child.stderr.on('data', chunk => { stderr += chunk; });
        child.on('error', reject);
        child.on('exit', code => {
            if (code === 0) resolvePromise();
            else reject(new Error(`R2 download failed for ${key}: ${stderr.trim()}`));
        });
    });
}

await mkdir(dataDir, { recursive: true });
try {
    await stat(dbPath);
    throw new Error(`Refusing to overwrite existing database: ${dbPath}`);
} catch (error) {
    if (error.code !== 'ENOENT') throw error;
}

const keys = await listKeys();
const sqlite = new SqliteD1(dbPath);
const initSql = await readFile(join(root, 'database', 'init.sql'), 'utf8');
sqlite.exec(initSql);
const db = new D1Database(sqlite);

try {
    await mapConcurrent(keys, 12, async item => {
        const value = await cloudflare(`/values/${encodeURIComponent(item.name)}`).then(response => response.text());
        await db.put(item.name, value, { metadata: item.metadata || {} });
    });

    const r2Keys = keys.filter(item => item.metadata?.Channel === 'CloudflareR2');
    await mkdir(r2Dir, { recursive: true });
    await mapConcurrent(r2Keys, 6, item => downloadR2Object(item.name));

    console.log(JSON.stringify({
        kvKeysMigrated: keys.length,
        r2ObjectsMigrated: r2Keys.length,
    }));
} catch (error) {
    await rm(dbPath, { force: true });
    throw error;
}
