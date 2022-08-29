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

  // Subscribe process to Redis channel
  await subscriber.subscribe(WS_UI_CHANNEL);

  subscriber.on('message', (channel, message) => {

    // Process messages from the "WS_UI_CHANNEL" channel
    if (channel === WS_UI_CHANNEL) {
      // Iterate over UISocketsMap sockets
      for (const [socketKey, socket] of UISocketsMap) {
        try {
          if (socket.readyState !== 1) {
            throw 'Not sending message because websocket is not open'
          }
          // Send WS message to the socket
          socket.send(message);
        } catch (e) {
          console.error(e);
        }
      };
    }

  });

  // Connect WS client to the "/ui" url
  app.ws('/ui', async (ws, req) => {
    console.log('UI has connected via websocket');
    let socketId = new Date().getTime();
    console.log(`UI websocket has id ${socketId}, pid ${process.pid}`);
    // Add WS to UISocketsMap
    UISocketsMap.set(socketId, ws);

    // Receive a WS message
    ws.on('message', async (msg) => {
      console.log('Recieved a test message from UI');
      // Validate message
      const message = validateMessage(msg)
      // Publish message to "WS_UI_CHANNEL"
      await publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
    });

    // On "/ui" connection close
    ws.on('close', (code, reason) => {
      console.log('UI Websocket has closed.');
      // Delete WS from UISocketsMap
      UISocketsMap.delete(socketId);
    });
  });

  // Connect WS client to the "/interop-socket" url
  app.ws('/interop-socket', async (ws, req) => {
    let uuid = undefined;

    // Receive a WS message
    ws.on('message', async (msg) => {
      console.log('Recieved a message from the agent.');
      // Validate message
      const message = validateMessage(msg);
      // Publish message to "WS_UI_CHANNEL"
      await publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
      if (message.isValid === false) {
        console.log('Not valid message.');
        // If the message is not valid, send back a WS message with the error
        ws.send(JSON.stringify(message));
      } else {
        // Get the UUID from the message
        uuid = message.message.uuid;

        // Add WS to AgentSocketsMap using UUID as a key
        AgentSocketsMap.set(uuid, ws);
        await processMessage(uuid, msg)
        .catch(e => console.log(e));
      }
    });

    // On "/interop-socket" connection close
    ws.on('close', async (code, reason) => {
      console.log('Agent Websocket has closed.', uuid);
      if (uuid === undefined) {
        return;
      }
      // Delete WS from AgentSocketsMap by uuid
      AgentSocketsMap.delete(uuid);
      try {
        await dbClient.connect();
        const database = dbClient.db(DB_NAME);
        // Delete the agent identity record from database
        await database.collection('agent_identity').deleteOne({uuid: uuid});
        // Delete the agent status record from database
        await database.collection('agent_status').deleteOne({uuid: uuid});
      } finally {
        await dbClient.close();
      }
    });
  });

  app.get('/test-message', (req, res) => {
    // Send test message schema
    res.send(testMessage);
  });
};

const validateMessage = (msg) => {
  let hasWellFormedJSON = false;
  let message = {};
  let errors = {};
  try {
    // Check if the message has a valid Json format
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
    // Validate schema format using Ajv
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
      // Upsert (insert or update) the agent_identity table by uuid
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
      // Upsert (insert or update) the agent_status table by uuid
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
