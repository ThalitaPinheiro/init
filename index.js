'use strict';

process.title = require('./package.json').name;

var config = require('./lib/commons/config');

config.file('conf/config.json');

var logger = require('./lib/commons/logger');
var app = require('./lib/application');

/*var database = require('./lib/commons/database');
database.connect(config.get('mongo:connection'), function(err) {
  if (err) {
    logger.error('Shutdown the application because an error ocurred ' +
    'when connecting to database');

    process.exit(1);
  }
});
*/
var shutdown = function() {
  logger.warn('Server receive signal to shutdown.');
  process.exit(0);
};

process.on('SIGTERM', shutdown)
  .on('SIGINT', shutdown)
  .on('SIGHUP', shutdown)
  .on('uncaughtException', function(er) {
    logger.error(er.message);
  })
  .on('exit', function(code) {
    logger.info('Node process exit with code:', code);
    //database.close();
  });

var server = app.listen(config.get('server:port'), function(err) {
  if (err) {
    logger.error('Error on listen port. ', err.message);
  }
  logger.info('Server starting at %s:%s.',
    server.address().address, server.address().port);

  server.on('close', function() {
    logger.info('Shutdown the application server');
  });
});

module.exports = server;
