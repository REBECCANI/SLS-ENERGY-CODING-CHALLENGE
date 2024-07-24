const { parentPort, workerData, isMainThread } = require('worker_threads');
const db = require('./db');
const {
  calculateInteractionScore,
  calculateHashtagScore,
  calculateKeywordScore, // Corrected function name
  calculateFinalScore // Corrected function name
} = require('./ranking');

const validateUserData = (user) => {
  return user && user.id && user.created_at;
};

const validateTweetData = (tweet) => {
  return tweet && tweet.id && tweet.created_at && tweet.user && validateUserData(tweet.user);
};

const transformUserData = (user, createdAt) => {
  return [
    user.id,
    user.name || '',
    user.screen_name || '',
    user.location || '',
    user.description || '',
    user.followers_count || 0,
    user.friends_count || 0,
    user.statuses_count || 0,
    user.profile_image_url || '',
    new Date(createdAt)
  ];
};

const transformTweetData = (tweet) => {
  return [
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
};

const insertUsers = async (users) => {
  const userInsertQuery = `INSERT INTO USERS(user_id, name, screen_name, location, description, followers_count, friends_count, statuses_count, profile_image_url, created_at) VALUES ? ON DUPLICATE KEY UPDATE name=VALUES(name), screen_name=VALUES(screen_name), location=VALUES(location), description=VALUES(description), followers_count=VALUES(followers_count), friends_count=VALUES(friends_count), statuses_count=VALUES(statuses_count), profile_image_url=VALUES(profile_image_url), created_at=VALUES(created_at)`;
  const userValues = users.map(({ user, createdAt }) => transformUserData(user, createdAt));
  try {
    console.log('Inserting users...');
    const [result] = await db.query(userInsertQuery, [userValues]);
    console.log('Users inserted:', result.affectedRows);
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
    console.log('Inserting tweets...');
    const [result] = await db.query(tweetInsertQuery, [tweetValues]);
    console.log('Tweets inserted:', result.affectedRows);
    return result.affectedRows;
  } catch (error) {
    console.error('Error inserting tweets:', error);
    throw error;
  }
};

const insertHashtags = async (hashtags) => {
  const hashtagInsertQuery = `INSERT INTO HASHTAGS(tweet_id, hashtag) VALUES ?`;
  const hashtagValues = hashtags.flatMap(({ tweet_id, hashtags }) => hashtags.map(hashtag => [tweet_id, hashtag]));
  try {
    console.log('Inserting hashtags...');
    const [result] = await db.query(hashtagInsertQuery, [hashtagValues]);
    console.log('Hashtags inserted:', result.affectedRows);
    return result.affectedRows;
  } catch (error) {
    console.error('Error inserting hashtags:', error);
    throw error;
  }
};

const insertContacts = async (contacts) => {
  const contactInsertQuery = `INSERT INTO CONTACTS(user_id, contact_tweet_id, contacted_user) VALUES ? ON DUPLICATE KEY UPDATE contact_tweet_id=VALUES(contact_tweet_id), contacted_user=VALUES(contacted_user)`;
  const contactValues = contacts.map(({ user_id, contact_tweet_id, contacted_user }) => [user_id, contact_tweet_id, contacted_user]);
  try {
    console.log('Inserting contacts...');
    const [result] = await db.query(contactInsertQuery, [contactValues]);
    console.log('Contacts inserted:', result.affectedRows);
    return result.affectedRows;
  } catch (error) {
    console.error('Error inserting contacts:', error);
    throw error;
  }
};

const calculateScores = (user, tweets) => {
  const interactionScore = calculateInteractionScore(user, tweets);
  const hashtagScore = calculateHashtagScore(user, tweets);
  const keywordScore = calculateKeywordScore(user, tweets); // Corrected function name
  const finalScore = calculateFinalScore(interactionScore, hashtagScore, keywordScore); // Corrected function name

  return {
    interactionScore,
    hashtagScore,
    keywordScore,
    finalScore
  };
};

async function processChunk(chunk) {
  try {
    console.log('Processing chunk with', chunk.length, 'tweets');
    const validTweets = chunk.filter(validateTweetData);
    console.log('Valid tweets:', validTweets.length);

    const userMap = new Map();
    validTweets.forEach(tweet => {
      const tweetCreatedAt = new Date(tweet.created_at);

      // Process tweet user
      const tweetUser = tweet.user;
      if (validateUserData(tweetUser)) {
        if (!userMap.has(tweetUser.id) || userMap.get(tweetUser.id).createdAt < tweetCreatedAt) {
          userMap.set(tweetUser.id, { user: tweetUser, createdAt: tweetCreatedAt });
        }
      }

      // Process retweet user
      const retweetUser = tweet.retweeted_status ? tweet.retweeted_status.user : null;
      if (retweetUser && validateUserData(retweetUser)) {
        const retweetCreatedAt = new Date(tweet.retweeted_status.created_at);
        if (!userMap.has(retweetUser.id) || userMap.get(retweetUser.id).createdAt < retweetCreatedAt) {
          userMap.set(retweetUser.id, { user: retweetUser, createdAt: retweetCreatedAt });
        }
      }

      // Add users for in_reply_to_user_id
      const replyToUserId = tweet.in_reply_to_user_id;
      if (replyToUserId && !userMap.has(replyToUserId)) {
        userMap.set(replyToUserId, { user: { id: replyToUserId, screen_name: '', description: '' }, createdAt: tweetCreatedAt });
      }
    });

    const users = Array.from(userMap.values());
    const usersInserted = await insertUsers(users);

    const tweetsWithHashtags = validTweets.map(tweet => ({
      tweet_id: tweet.id,
      hashtags: tweet.entities?.hashtags?.map(h => h.text) || []
    }));
    const tweetsInserted = await insertTweets(validTweets);
    const hashtagsInserted = await insertHashtags(tweetsWithHashtags);

    const contacts = validTweets.flatMap(tweet => {
      const userId = tweet.user.id;
      const replyToUserId = tweet.in_reply_to_user_id;
      const retweetUserId = tweet.retweeted_status ? tweet.retweeted_status.user.id : null;
      const tweetId = tweet.id;

      const contacts = [];
      if (replyToUserId) {
        contacts.push({ user_id: replyToUserId, contact_tweet_id: tweetId, contacted_user: userId });
      }
      if (retweetUserId && retweetUserId !== userId) {
        contacts.push({ user_id: retweetUserId, contact_tweet_id: tweetId, contacted_user: userId });
      }
      return contacts;
    });
    const contactsInserted = await insertContacts(contacts);

    // Calculate and log scores for each user
    users.forEach(({ user }) => {
      const scores = calculateScores(user, validTweets.filter(tweet => tweet.user.id === user.id));
      console.log(`Scores for user ${user.id}:`, scores);
    });

    return { usersInserted, tweetsInserted, hashtagsInserted, contactsInserted };
  } catch (error) {
    console.error('Error processing chunk:', error);
    throw error;
  }
}

if (isMainThread) {
  console.log('This script is designed to be run as a worker thread.');
  console.log('Please run the main script to start processing.');
} else {
  processChunk(workerData)
    .then(result => {
      parentPort.postMessage({ result });
    })
    .catch(error => {
      parentPort.postMessage({ error: error.message });
    });
}
