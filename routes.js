import express from 'express';
import { CID } from 'multiformats/cid';
import { createHelia } from 'helia';
import { unixfs } from '@helia/unixfs';
import { Readable } from 'stream';
import { FsDatastore } from 'datastore-fs';
import { KeyTransformDatastore } from 'datastore-core';
import { Key } from 'interface-datastore';
import path from 'path';
import { tcp } from '@libp2p/tcp';
import { webSockets } from '@libp2p/websockets';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { bootstrap } from '@libp2p/bootstrap';
import { mdns } from '@libp2p/mdns';
import { identify } from '@libp2p/identify';
import { kadDHT } from '@libp2p/kad-dht';
import { ping } from '@libp2p/ping';
import log4js from 'log4js';
import { createProxyMiddleware } from 'http-proxy-middleware';

const ipfsGateway = createProxyMiddleware({
  target: 'http://fso2.lan:8088',
  changeOrigin: true,
  ws: true
});

const MAX_DIR_ENTRIES = 1000;

const logDate = new Date()
const logDateString = logDate.toISOString().split('T')[0]

// ---- Logging ----
log4js.configure({
  appenders: { file: { type: "file", filename: "logs/log-"+logDateString+".log" }, console: { type: 'console' } },
  categories: { default: { appenders: ["file", "console"], level: "debug" } },
});
const logger = log4js.getLogger();

// ---- FS Store ----
const rawFsStore = new FsDatastore('/srv/helia');
const fsStore = new KeyTransformDatastore(rawFsStore, {
  convert: key => new Key(path.join('/', key.toString())),
  invert: key => new Key(key.toString())
});

// ---- Helia ----
// Custom bootstrap nodes - replace with your preferred IPFS nodes
const bootstrapNodes = [
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb',
  '/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt',
  // Add your custom nodes here, for example:
  '/ip4/100.106.187.83/tcp/21158/p2p/12D3KooWAKJvVXX77TmzMUKFiHpGqACUMErrmuzH3VyQ3kb8ucUe'
]

const helia = await createHelia({
  datastore: fsStore,
  blockstore: fsStore,
  libp2p: {
    addresses: {
      listen: [
        '/ip4/0.0.0.0/tcp/0',
        '/ip6/::/tcp/0'
      ]
    },
    transports: [
      tcp(),
      webSockets()
    ],
    connectionEncryption: [
      noise()
    ],
    streamMuxers: [
      yamux()
    ],
    peerDiscovery: [
      bootstrap({
        list: bootstrapNodes,
        timeout: 1000, // 1 second timeout
        tagName: 'bootstrap'
      }),
      mdns() // Optional: enable local network discovery
    ],
    services: {
      identify: identify(),
      dht: kadDHT(), // Enable DHT for peer discovery and content routing
      ping: ping()
    }
  }
})
const fs = unixfs(helia);

// ---- Express Router ----
const router = express.Router();

// ---- Helpers ----
function wantsHtml(req) {
  return (req.headers.accept || '').includes('text/html');
}

function isDirectoryRequest(req) {
  return (
    wantsHtml(req) &&
    !req.headers.range &&
    !req.query.download
  );
}

// --- list directory metadata without downloading big files ---
async function* listDirectoryStream(cidObj) {
  for await (const e of fs.ls(cidObj)) {
    if (e.cid.toString() == cidObj.cid.toString())
      continue;
    yield {
      name: e.name,
      type: e.type,
      cid: e.cid.toString(),
      size: e.type === 'file' && e.size < 10 * 1024 * 1024 ? Number(e.size) : undefined
    };
  }
}

// --- retrieve content ---
async function retrieveContent(cidStr, subPath) {
  let cidObj = CID.parse(cidStr);

  // walk subPath safely
  if (subPath) {
    for (const part of subPath.split('/').filter(Boolean)) {
      let found = false;
      for await (const entry of fs.ls(cidObj)) {
        if (entry.name === part) {
          cidObj = entry.cid;
          found = true;
          break;
        }
      }
      if (!found) {
        throw new Error(`Path not found: ${subPath}`);
      }
    }
  }

  const stat = await fs.stat(cidObj);

  // ---------- DIRECTORY ----------
  if (stat.type === 'directory') {
    const entries = [];
    let count = 0;

    for await (const e of fs.ls(cidObj)) {
      entries.push({
        name: e.name,
        type: e.type,
        cid: e.cid.toString(),
        size:
          e.type === 'file' && e.size < 10 * 1024 * 1024
            ? Number(e.size)
            : undefined
      });

      if (++count >= MAX_DIR_ENTRIES) break;
    }

    return {
      type: 'directory',
      entries
    };
  }

  // ---------- FILE ----------
  return {
    type: 'file',
    stream: Readable.from(fs.cat(cidObj)),
    size: Number(stat.size),
    mime: subPath
      ? getMimeTypeFromName(subPath)
      : 'application/octet-stream'
  };
}

function renderDirectoryHtml(cid, entries) {
  const rows = entries.map(e => {
    const name = e.name + (e.type === 'directory' ? '/' : '');
    return `
      <tr>
        <td><a href="${name}">${name}</a></td>
        <td>${e.type}</td>
        <td>${e.size ?? '-'}</td>
      </tr>`;
  }).join('');

  return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Index of /ipfs/${cid}</title>
<style>
body { font-family: sans-serif; padding: 1em }
table { border-collapse: collapse }
td { padding: 4px 12px }
tr:hover { background: #f0f0f0 }
</style>
</head>
<body>
<h1>Index of /ipfs/${cid}</h1>
<table>
<tr><th>Name</th><th>Type</th><th>Size</th></tr>
${rows}
</table>
</body>
</html>`;
}


// --- handle IPFS requests ---
async function handleIpfs(cid, subPath, req, res) {
  try {
    const result = await retrieveContent(cid, subPath);

    if (result.type === 'directory') {
      if (!wantsHtml(req)) {
        res.json({
          type: 'directory',
          cid,
          entries: result.entries
        });
        return;
      }

      const html = renderDirectoryHtml(cid, result.entries);
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.send(html);
      return;
    }

    handleResponse(req, res, result.mime, result.stream, result.size);
  } catch (err) {
    console.error('IPFS retrieval error:', err);
    res.status(500).send(err.message);
  }
}

// --- handle response with range support ---
function handleResponse(req, res, mime, stream, size) {
  res.setHeader('Content-Disposition', 'inline');
  res.setHeader('Content-Type', mime);
  res.type(mime);

  if (size != null) {
    res.setHeader('Content-Length', size);
    res.setHeader('Accept-Ranges', 'bytes');
  }

  const range = req.headers.range;

  if (range && size != null && mime.startsWith('video/')) {
    const [startStr, endStr] = range.replace(/bytes=/, '').split('-');
    const start = Number(startStr);
    const end = endStr ? Number(endStr) : size - 1;

    res.status(206);
    res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`);
    res.setHeader('Content-Length', end - start + 1);

    let offset = 0;

    stream.on('data', chunk => {
      const chunkStart = offset;
      const chunkEnd = offset + chunk.length - 1;
      offset += chunk.length;

      if (chunkEnd < start || chunkStart > end) return;

      const sliced = chunk.subarray(
        Math.max(0, start - chunkStart),
        Math.min(chunk.length, end - chunkStart + 1)
      );

      res.write(sliced);
    });

    stream.on('end', () => res.end());
    stream.on('error', () => res.destroy());
    return;
  }

  stream.pipe(res);
}
// ---- Routes ----
router.get('/ipfs/:cid', (req, res, next) => {
  if (isDirectoryRequest(req)) {
    return ipfsGateway(req, res, next);
  }
  next();
});

router.get('/ipfs/:cid/*subPath', (req, res, next) => {
  if (isDirectoryRequest(req)) {
    return ipfsGateway(req, res, next);
  }
  next();
});

router.get('/ipfs/:cid', async (req, res) => {
  await handleIpfs(req.params.cid, undefined, req, res);
});


// catch-all for nested paths
router.get('/ipfs/:cid/*subPath', async (req, res) => {
  const { cid, subPath } = req.params;
  await handleIpfs(cid, subPath, req, res);
});

router.get('/ipns/:name', async (req, res) => {
  res.status(501).send('IPNS not implemented. Use IPFS CIDs.');
});

export default router;
