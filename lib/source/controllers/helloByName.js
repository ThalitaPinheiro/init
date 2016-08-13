'use strict';

var fmt = require('util').format;

var ROOT_PATH = process.cwd();

var logger = require(ROOT_PATH + '/lib/commons/logger');

module.exports = function(req, res) {
  var name = req.params.name;
  var helloByName = fmt('Olá, %s !!!', name);

  logger.info(helloByName);

  res.status(200).send(helloByName);
};
