const db =require('./db');
const createUsersTable = `
    CREATE TABLE IF NOT EXISTS users (
      user_id BIGINT PRIMARY KEY,
      name VARCHAR(255),
      screen_name VARCHAR(255),
      location VARCHAR(255),
      description TEXT,
      followers_count INT,
      friends_count INT,
      statuses_count INT,
      profile_image_url TEXT,
      created_at DATETIME
    );
  `;

  const createTweetsTable = `
    CREATE TABLE IF NOT EXISTS tweets (
      tweet_id BIGINT PRIMARY KEY,
      created_at DATETIME,
      text TEXT,
      source TEXT,
      user_id BIGINT,
      retweet_count INT,
      favorite_count INT,
      lang VARCHAR(10),
      possibly_sensitive BOOLEAN,
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
  `;

  const createHashtagsTable = `
    CREATE TABLE IF NOT EXISTS hashtags (
      hashtag_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      tweet_id BIGINT,
      hashtag VARCHAR(255),
      FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id)
    );
  `;
  const setupDatabase = async () => {
    try {
      await db.query(createUsersTable);
      console.log('Users table created');
      
      await db.query(createTweetsTable);
      console.log('Tweets table created');
      
      await db.query(createHashtagsTable);
      console.log('Hashtags table created');
    } catch (error) {
      console.error('Error setting up database:', error);
      throw error;
    }
  };

  
module.exports = { setupDatabase };
