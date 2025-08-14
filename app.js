import 'dotenv/config';
import { MongoClient } from 'mongodb';
import cron from 'node-cron';
import fetch from 'node-fetch';
import { pipeline } from '@xenova/transformers';
import crypto from 'crypto';
import cliProgress from 'cli-progress';
import colors from 'ansi-colors';

const mongoUri = process.env.MONGO_URI;
const dbName = process.env.DB_NAME;
const client = new MongoClient(mongoUri);

let embedder = null;

// Load embedding model once
async function loadEmbedder() {
    if (!embedder) {
        console.log(colors.cyan('⏳ Loading local embedding model...'));
        embedder = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
        console.log(colors.green('✅ Embedding model loaded'));
    }
}

// Convert tensor → flat array
function flattenEmbedding(result) {
    if (!result) return [];
    if (result.data) return Array.from(result.data);
    if (Array.isArray(result[0]?.data)) return Array.from(result[0].data);
    if (Array.isArray(result[0])) return result[0].flat();
    if (Array.isArray(result)) return result;
    return [];
}

// Generate embedding vector
async function generateEmbedding(text) {
    if (!embedder) await loadEmbedder();
    try {
        const output = await embedder(text, { pooling: 'mean', normalize: true });
        return flattenEmbedding(output);
    } catch (err) {
        console.error(colors.red('❌ Embedding error:'), err);
        return [];
    }
}

// Generate MD5 hash for doc (ignore vector & hash fields)
function generateHash(doc) {
    const clone = { ...doc };
    delete clone.vector;
    delete clone.hash;
    return crypto.createHash('md5').update(JSON.stringify(clone)).digest('hex');
}

// Import API → MongoDB
async function importData(apiUrl, collectionName, type) {
    try {
        console.log(colors.yellow(`[${new Date().toLocaleString()}] 📡 Fetching data from ${apiUrl}`));
        const res = await fetch(apiUrl);
        const data = await res.json();
        console.log(colors.magenta(`📦 Fetched ${data.length} records from ${collectionName}`));

        const db = client.db(dbName);
        const bar = new cliProgress.SingleBar({
            format: `${collectionName} |${colors.cyan('{bar}')}| {percentage}% || {value}/{total} Docs || {doc}`,
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        let processed = 0;
        let upsertedCount = 0;
        let modifiedCount = 0;
        let skippedCount = 0;

        bar.start(data.length, 0, { doc: '' });

        for (let doc of data) {
            let textToEmbed = '';
            if (type === 'conference') {
                textToEmbed = `${doc.name || ''} ${doc.acronym || ''} ${doc.topics || ''}`.trim();
            } else if (type === 'journal') {
                textToEmbed = `${doc.title || ''} ${doc.categories || ''} ${doc.areas || ''}`.trim();
            }

            const hash = generateHash(doc);
            const id = doc.id_conference || doc.id_journal;
            const existing = await db.collection(collectionName).findOne({ _id: id });

            // Skip if unchanged
            if (existing && existing.hash === hash) {
                skippedCount++;
                processed++;
                bar.update(processed, { doc: doc.name || doc.title || '' });
                continue;
            }

            // Generate vector
            const vector = await generateEmbedding(textToEmbed);
            if (!vector.length) {
                console.warn(colors.yellow(`⚠️ Empty embedding for ${collectionName} doc:`), id);
            }

            // Upsert to DB
            doc._id = id;
            doc.vector = vector;
            doc.hash = hash;

            const result = await db.collection(collectionName).updateOne(
                { _id: id },
                { $set: doc },
                { upsert: true }
            );

            if (result.upsertedCount > 0) upsertedCount++;
            if (result.modifiedCount > 0) modifiedCount++;

            processed++;
            bar.update(processed, { doc: doc.name || doc.title || '' });
        }

        bar.stop();
        console.log(colors.green(`✅ Upserted: ${upsertedCount}, Modified: ${modifiedCount}, Skipped: ${skippedCount} in ${collectionName}`));
    } catch (err) {
        console.error(colors.red(`❌ Error importing ${collectionName}:`), err);
    }
}

// Run import job
async function runImport() {
    try {
        await client.connect();
        await importData(process.env.API_CONFERENCE, 'conference', 'conference');
        await importData(process.env.API_JOURNAL, 'journal', 'journal');
    } catch (err) {
        console.error(colors.red('❌ Error connecting to MongoDB:'), err);
    } finally {
        await client.close();
    }
}

// Run immediately
runImport();

// Cron job daily at 00:00
cron.schedule('0 0 * * *', async () => {
    console.log(colors.cyan('⏳ Running daily import...'));
    await runImport();
});
