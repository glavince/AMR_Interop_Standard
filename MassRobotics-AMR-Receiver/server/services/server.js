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
const subscriber = redis.createClient({url: "redis://localhost:6379"});
const publisher = subscriber.duplicate();

const MongoClient = require('mongodb').MongoClient;
const url = "mongodb://localhost:27017/";
const WS_UI_CHANNEL = "ws:UIChannel";

const UISocketsMap = new Map();
const RobotSocketsMap = new Map();


module.exports = function(app) {
  app.use(bodyParser.json());

  subscriber.on('message', (channel, message) => {
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
  });

  app.ws('/ui', function(ws, req) {
    console.log('UI has connected via websocket');
    let socketId = new Date().getTime();
    console.log(`UI websocket has id ${socketId}`);
    UISocketsMap.set(socketId, ws);
    subscriber.subscribe(WS_UI_CHANNEL);

    ws.on('message', (msg) => {
      console.log('Recieved a test message from UI');
      const message = validateMessage(msg)
      publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
    });

    ws.on('close', (code, reason) => {
      console.log('UI Websocket has closed.');
      UISocketsMap.delete(socketId);
    });
  });

  app.ws('/', function(ws, req) {
    let socketId = new Date().getTime();
    RobotSocketsMap.set(socketId, ws)
    subscriber.subscribe(WS_UI_CHANNEL);

    ws.on('message', function(msg) {
      console.log('Recieved a message from the robot');
      const message = validateMessage(msg)
      publisher.publish(WS_UI_CHANNEL, JSON.stringify(message));
      if (message.isValid === false) {
        return;
      }

      msg = JSON.parse(msg)
      const uuid = msg.uuid;
      RobotSocketsMap.get(socketId).uuid = uuid;

      if (msg.type === 'Robot Identity') {
        MongoClient.connect(url, function(err, db) {
          if (err) throw err;
          var dbo = db.db('vecna');
          const query = { uuid: uuid };
          const update = { $set: msg };
          const options = { upsert: true };
          dbo.collection('robot_identity').updateOne(query, update, options);
        });
      }
      if (msg.type === 'Robot Status') {
        MongoClient.connect(url, function(err, db) {
          if (err) throw err;
          var dbo = db.db('vecna');
          const query = { uuid: uuid };
          const update = { $set: msg };
          const options = { upsert: true };
          dbo.collection('robot_status').updateOne(query, update, options);
        });
      }
    });

    ws.on('close', (code, reason) => {
      const uuid = RobotSocketsMap.get(socketId).uuid;
      console.log('Robot Websocket has closed.', uuid);
      RobotSocketsMap.delete(socketId)
      MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        var dbo = db.db('vecna');
        dbo.collection('robot_status').deleteOne({uuid: uuid});
        dbo.collection('robot_identity').deleteOne({uuid: uuid});
      });
    });
  });

  app.get('/test-message', function(req, res) {
    res.send(testMessage);
  });
};

function validateMessage(msg) {
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
}
