'use strict';

var ROOT_PATH = process.cwd();

var MongoClient = require('mongodb').MongoClient;
var async = require('async');
var logger = require(ROOT_PATH + '/lib/commons/logger');

var db;
var collections = [];
var Database = {};
var self = Database;

Database.connect = function(uri, callback) {
  logger.debug('Database trying to connect at', uri);

  if (db) {
    callback(db);
  } else {
    MongoClient.connect(uri, function(err, _db) {
      if (err) {
        logger.error('Database failed to connect at %s - ', uri, err.message);
      } else {
        logger.info('Database connected at', uri);
        db = _db;
      }
      callback(err, db);
    });
  }
};

Database.getCollection = function(collectionName) {
  var collection = collections[collectionName];

  if (!collection) {
    collection = db.collection(collectionName);
    collections[collectionName] = collection;
  }

  return collection;
};

Database.dropCollections = function() {
  var lastIndex = arguments.length - 1;
  var collectionsToDrop = [];

  for (var i = 0; i < arguments.length; i++) {
    var arg = arguments[i];

    if (i === lastIndex && typeof arg === 'function') {
      var done = arg;

      async.each(collectionsToDrop, function(collection, callback) {
        collection.drop(function(err) {
          if (err) {
            logger.error('Error on drop collection %s:',
              collectionsToDrop.collectionName, err.message);
          }
          callback();
        });
      }, done);
    } else {
      var collection = self.getCollection(arg);
      if (collection) {
        collectionsToDrop.push(collection);
      }
    }
  }
};

Database.dropDatabase = function(callback) {
  db.dropDatabase(function(err) {
    if (err) {
      logger.error('Error on drop database %s:',
        db.databaseName, err.message);
    } else {
      logger.info('Database dropped');
    }
    callback();
  });
};

Database.close = function(callback) {
  logger.debug('Database trying to disconnect');

  if (db) {
    db.close(function(err) {
      if (err) {
        logger.error('Error on closing database');
      } else {
        logger.info('Database disconnected');
      }
      callback(err);
    });
  }
};

module.exports = Database;
