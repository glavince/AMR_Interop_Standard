const _ = require('underscore');
const bodyParser = require('body-parser');
const testMessage = require('../schema/test-message.json');
const schema = require('../schema/schema.json');
const Ajv = require('ajv');
const ajv = new Ajv({strictSchema: false});
const addFormats = require('ajv-formats');
addFormats(ajv);
const validate = ajv.compile(schema);

const redis = require('redis');
const subscriber = redis.createClient({url: 'redis://localhost:6379'});
const publisher = subscriber.duplicate();

const { MongoClient } = require('mongodb');
const MongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/';
const dbClient = new MongoClient(MongoUrl);
const WS_UI_CHANNEL = 'ws:UIChannel';

const UISocketsMap = new Map();
const AgentSocketsMap = new Map();

const DB_NAME = 'amr_interop';

module.exports = async (app) => {
  app.use(bodyParser.json());

  await subscriber.subscribe(WS_UI_CHANNEL);

  subscriber.on('message', (channel, message) => {

    if (channel === WS_UI_CHANNEL) {
      for (const [socketKey, socket] of UISocketsMap) {
        try {
          if (socket.readyState !== 1) {
            throw 'Not sending message because websocket is not open'
          }
          socket.send(message);
        } catch (e) {
          console.error(e);
        }
      };
    }

  });

  app.ws('/ui', async (ws, req) => {
    console.log('UI has connected via websocket');
    let socketId = new Date().getTime();
    console.log(`UI websocket has id ${socketId}, pid ${process.pid}`);
    UISocketsMap.set(socketId, ws);

    ws.on('message', async (msg) => {
      console.log('Recieved a test message from UI');
      const message = validateMessage(msg)
      await publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
    });

    ws.on('close', (code, reason) => {
      console.log('UI Websocket has closed.');
      UISocketsMap.delete(socketId);
    });
  });

  app.ws('/interop-socket', async (ws, req) => {
    let uuid = undefined;

    ws.on('message', async (msg) => {
      console.log('Recieved a message from the agent.');
      const message = validateMessage(msg);
      await publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
      if (message.isValid === false) {
        console.log('Not valid message.');
        ws.send(JSON.stringify(message));
      } else {
        uuid = message.message.uuid;
        AgentSocketsMap.set(uuid, ws);
        await processMessage(uuid, msg)
        .catch(e => console.log(e));
      }
    });

    ws.on('close', async (code, reason) => {
      console.log('Agent Websocket has closed.', uuid);
      if (uuid === undefined) {
        return;
      }
      AgentSocketsMap.delete(uuid);
      try {
        await dbClient.connect();
        const database = dbClient.db(DB_NAME);
        await database.collection('agent_identity').deleteOne({uuid: uuid});
        await database.collection('agent_status').deleteOne({uuid: uuid});
      } finally {
        await dbClient.close();
      }
    });
  });

  app.get('/test-message', (req, res) => {
    res.send(testMessage);
  });
};

const validateMessage = (msg) => {
  let hasWellFormedJSON = false;
  let message = {};
  let errors = {};
  try {
    message = JSON.parse(msg);
    hasWellFormedJSON = true;
  } catch(e) {
    console.log('Not valid json');
    errors = [{
      type: 'MalformedJSON',
      message: e.toString()
    }];
    message = msg;
  }

  let result = false;
  if (hasWellFormedJSON) {
    result = validate(message);
    if (!result) {
      errors = validate.errors;
    }
  }

  return {
    message: message,
    isValid: result,
    errors: errors
  }
};

const processMessage = async (uuid, msg) => {
  const message = JSON.parse(msg);

  if (message.type === 'AGENT_IDENTITY') {
    try {
      await dbClient.connect();
      const database = dbClient.db(DB_NAME);
      const query = { uuid: uuid };
      const update = { $set: message };
      const options = { upsert: true };
      await database.collection('agent_identity').updateOne(query, update, options);
    } finally {
      await dbClient.close();
    }
  }
  if (message.type === 'AGENT_STATUS') {
    try {
      await dbClient.connect();
      const database = dbClient.db(DB_NAME);
      const query = { uuid: uuid };
      const update = { $set: message };
      const options = { upsert: true };
      await database.collection('agent_status').updateOne(query, update, options);
    } finally {
      await dbClient.close();
    }
  }
};
