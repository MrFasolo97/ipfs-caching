import { Schema, model } from 'mongoose';

const FileRequestSchema = new Schema({
  cid: { type: String, required: true, unique: true },
  lastAccessed: { type: Date, default: Date.now },
  accessCount: { type: Number, default: 1 },
});

export default model('FileRequest', FileRequestSchema);