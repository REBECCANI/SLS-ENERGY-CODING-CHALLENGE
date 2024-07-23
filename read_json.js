const fs = require('fs');
const readline = require('readline');
const { Worker } = require('worker_threads');
const { setupDatabase } = require('./dbsetup');

async function setupAndValidateDatabase() {
  try {
    await setupDatabase();
    console.log('Database setup completed successfully');
  } catch (error) {
    console.error('Error setting up database:', error);
    process.exit(1);
  }
}

function loadJSON(filePath) {
  return new Promise((resolve, reject) => {
    const tweets = [];
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath, {encoding: 'utf8'}),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      try {
        const tweet = JSON.parse(line);
        tweets.push(tweet);
      } catch (error) {
        console.error('Error parsing line:', error.message);
      }
    });

    rl.on('close', () => {
      console.log('JSON parsing completed');
      resolve(tweets);
    });

    rl.on('error', (error) => {
      reject(error);
    });
  });
}

function processChunk(chunk) {
  return new Promise((resolve, reject) => {
    const worker = new Worker('./worker.js', { workerData: chunk });
    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

const allowedLanguages = ['ar', 'en', 'fr', 'in', 'pt', 'es', 'tr', 'ja'];

function filterValidTweets(tweets) {
  return tweets.filter(tweet => 
    tweet.lang && allowedLanguages.includes(tweet.lang)
  );
}

async function main() {
  try {
    await setupAndValidateDatabase();
    
    const tweets = await loadJSON('C:/Users/robot/Downloads/coding_challenge/challenge/query2_ref.txt');
    console.log(`Loaded ${tweets.length} tweets`);

    const validTweets = filterValidTweets(tweets);
    console.log(`Filtered down to ${validTweets.length} valid tweets`);

    // Remove duplicates
    const uniqueTweets = Array.from(new Map(validTweets.map(tweet => [tweet.id, tweet])).values());
    console.log(`Removed duplicates, ${uniqueTweets.length} unique tweets`);

    const chunkSize = 1000;
    for (let i = 0; i < uniqueTweets.length; i += chunkSize) {
      const chunk = uniqueTweets.slice(i, i + chunkSize);
      console.log(`Processing chunk ${i/chunkSize + 1} with ${chunk.length} tweets`);
      try {
        const result = await processChunk(chunk);
        console.log(`Processed chunk ${i/chunkSize + 1}:`, result);
      } catch (error) {
        console.error(`Error processing chunk ${i/chunkSize + 1}:`, error);
      }
    }

    console.log('ETL process completed');
  } catch (error) {
    console.error('Error in ETL process:', error);
    process.exit(1);
  }
}

main();
