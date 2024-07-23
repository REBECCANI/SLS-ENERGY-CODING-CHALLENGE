const fs = require('fs');
const readline = require('readline');
const { Worker } = require('worker_threads');
const { setupDatabase } = require('./dbsetup');

// List of supported languages
const SUPPORTED_LANGUAGES = ['ar', 'en', 'fr', 'in', 'pt', 'es', 'tr', 'ja'];

// Helper functions for data processing
const isValidTweet = (tweet) => {
  return tweet &&
         (tweet.id || tweet.id_str) &&
         (tweet.user && (tweet.user.id || tweet.user.id_str)) &&
         tweet.created_at &&
         tweet.text && tweet.text.trim() !== '' &&
         tweet.entities && tweet.entities.hashtags && tweet.entities.hashtags.length > 0;
};

const isSupportedLanguage = (lang) => SUPPORTED_LANGUAGES.includes(lang);

const getContactTweets = (tweets) => tweets.filter(tweet => tweet.in_reply_to_user_id || tweet.retweeted_status);

const removeDuplicates = (tweets) => {
  const uniqueTweets = new Map();
  tweets.forEach(tweet => {
    if (!uniqueTweets.has(tweet.id_str || tweet.id)) {
      uniqueTweets.set(tweet.id_str || tweet.id, tweet);
    }
  });
  return Array.from(uniqueTweets.values());
};

const processTweets = (tweets) => {
  const validTweets = tweets.filter(isValidTweet);
  const uniqueTweets = removeDuplicates(validTweets);
  const languageFilteredTweets = uniqueTweets.filter(tweet => isSupportedLanguage(tweet.lang));
  const contactTweets = getContactTweets(languageFilteredTweets);
  return { filteredTweets: languageFilteredTweets, contactTweets };
};

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
      input: fs.createReadStream(filePath, { encoding: 'utf8' }),
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
      const { filteredTweets, contactTweets } = processTweets(tweets);
      resolve({ filteredTweets, contactTweets });
    });

    rl.on('error', (error) => reject(error));
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

async function main() {
  try {
    await setupAndValidateDatabase();
    
    const { filteredTweets, contactTweets } = await loadJSON('C:/Users/robot/Downloads/coding_challenge/challenge/query2_ref.txt');
    console.log(`Loaded ${filteredTweets.length} filtered tweets and ${contactTweets.length} contact tweets`);

    const chunkSize = 1000;
    for (let i = 0; i < filteredTweets.length; i += chunkSize) {
      const chunk = filteredTweets.slice(i, i + chunkSize);
      try {
        const result = await processChunk(chunk);
        console.log(`Processed chunk ${i / chunkSize + 1}:`, result);
      } catch (error) {
        console.error(`Error processing chunk ${i / chunkSize + 1}:`, error);
      }
    }

    console.log('ETL process completed');
  } catch (error) {
    console.error('Error in ETL process:', error);
    process.exit(1);
  }
}

main();
