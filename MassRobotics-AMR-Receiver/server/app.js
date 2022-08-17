const Express = require('express');
const BodyParser = require('body-parser');
const cluster = require('cluster');
const totalCPUs = require('os').cpus().length;
const port = require('./props').port;

const app = Express();
app.use(BodyParser.urlencoded({extended: true}));

app.use(Express.static('dist'));

app.expressWs = require('express-ws')(app);

require('./services/server')(app);


if (cluster.isMaster) {
  console.log(`Number of CPUs is ${totalCPUs}`);
  console.log(`Master ${process.pid} is running`);

  // Fork workers.
  for (let i = 0; i < totalCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died`);
    console.log('Let`s fork another worker!');
    cluster.fork();
  });
} else {
  app.listen(port, function() {
    console.log(`Starting MassRobotics Interoperability Working Group schema validator service. Listening on port ${port}`);
  });
}
