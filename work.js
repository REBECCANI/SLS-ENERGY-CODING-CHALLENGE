const { parentPort, workerData, isMainThread } = require('worker_threads');
const db = require('./db');

// Validation and transformation functions
const validateUserData = (user) => user.id && user.created_at;
const validateTweetData = (tweet) => tweet.id && tweet.created_at && tweet.user && validateUserData(tweet.user);

const transformUserData = (user) => [
  user.id,
  user.name || '',
  user.screen_name || '',
  user.location || '',
  user.description || '',
  user.followers_count || 0,
  user.friends_count || 0,
  user.statuses_count || 0,
  user.profile_image_url || '',
  new Date(user.created_at)
];

const transformTweetData = (tweet) => [
  tweet.id,
  new Date(tweet.created_at),
  tweet.text || '',
  tweet.source || '',
  tweet.user.id,
  tweet.retweet_count || 0,
  tweet.favorite_count || 0,
  tweet.lang || '',
  tweet.possibly_sensitive || false
];

const insertUsers = async (users) => {
  const userInsertQuery = `INSERT INTO USERS(user_id, name, screen_name, location, description, followers_count, friends_count, statuses_count, profile_image_url, created_at) VALUES ? ON DUPLICATE KEY UPDATE name=VALUES(name), screen_name=VALUES(screen_name), location=VALUES(location), description=VALUES(description), followers_count=VALUES(followers_count), friends_count=VALUES(friends_count), statuses_count=VALUES(statuses_count), profile_image_url=VALUES(profile_image_url), created_at=VALUES(created_at)`;
  const userValues = users.map(transformUserData);
  try {
    const [result] = await db.query(userInsertQuery, [userValues]);
    return result.affectedRows;
  } catch (error) {
    console.error('Error inserting users:', error);
    throw error;
  }
};

const insertTweets = async (tweets) => {
  const tweetInsertQuery = `INSERT INTO TWEETS(tweet_id, created_at, text, source, user_id, retweet_count, favorite_count, lang, possibly_sensitive) VALUES ? ON DUPLICATE KEY UPDATE text=VALUES(text), source=VALUES(source), retweet_count=VALUES(retweet_count), favorite_count=VALUES(favorite_count), lang=VALUES(lang), possibly_sensitive=VALUES(possibly_sensitive)`;
  const tweetValues = tweets.map(transformTweetData);
  try {
    const [result] = await db.query(tweetInsertQuery, [tweetValues]);
    return result.affectedRows;
  } catch (error) {
    console.error('Error inserting tweets:', error);
    throw error;
  }
};

async function processChunk(chunk) {
  try {
    const validTweets = chunk.filter(validateTweetData);
    const users = validTweets.map(tweet => tweet.user);
    const usersInserted = await insertUsers(users);
    const tweetsInserted = await insertTweets(validTweets);
    return { usersInserted, tweetsInserted };
  } catch (error) {
    console.error('Error processing chunk:', error);
    throw error;
  }
}

if (isMainThread) {
  console.log('This script is designed to be run as a worker thread.');
  console.log('Please run the main script (read_json.js) instead.');
} else {
  processChunk(workerData)
    .then(result => parentPort.postMessage(result))
    .catch(error => parentPort.postMessage({ error: error.message }));
}
