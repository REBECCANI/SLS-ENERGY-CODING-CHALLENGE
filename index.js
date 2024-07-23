
const express = require('express');
const dotenv = require('dotenv');
const db = require('./db');
const bodyParser = require('body-parser');
dotenv.config()

const app = express();
app.use(bodyParser.json());

const port = process.env.PORT || 3000

db.query('SELECT 1')
  .then(() => {
    console.log("Connected to the database");
  })
  .catch((err) => {
    console.error("Database connection error", err);
    process.exit(1);
  });

app.listen(port, () => {
  console.log(`The server is running on port ${port}`)
})