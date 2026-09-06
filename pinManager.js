import FileRequest from './mongoose/FileRequest.js';
import disk from 'diskusage';
import { CID } from 'multiformats/cid';

const DISK_THRESHOLD = 0.8;

async function getDiskUsage(path = '/') {
  const { available, free, total } = await disk.check(path);
  return 1 - free / total;
}

export async function pinCid(cid, helia) {
  await helia.pins.add(CID.parse(cid));
  const doc = await FileRequest.findOneAndUpdate(
    { cid },
    { $inc: { accessCount: 1 }, lastAccessed: new Date() },
    { upsert: true, new: true }
  );
}

export async function unpinOldestIfNeeded(helia) {
  const usage = await getDiskUsage();
  if (usage < DISK_THRESHOLD) return;

  const oldest = await FileRequest.find().sort({ lastAccessed: 1 }).limit(5);
  for (const file of oldest) {
    helia.pins.rm(CID.parse(file.cid));
    await FileRequest.deleteOne({ cid: file.cid });
    const newUsage = await getDiskUsage();
    if (newUsage < DISK_THRESHOLD) break;
  }
}