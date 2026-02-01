import express from 'express';
import mongoose from 'mongoose';
import routes from './routes.js';
import dotenv from 'dotenv';

dotenv.config();
const app = express();
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || "127.0.0.1";
const MONGO_URI = process.env.MONGO_URI || "mongodb://127.0.0.1:27017/ipfs_cache"

mongoose.connect(MONGO_URI).then(() => {
  console.log('MongoDB connected');
  app.use(express.json());
  app.use('/', routes);
  app.listen(PORT, HOST, () => console.log(`Server running on port ${PORT}`));
});
