'use strict';
var ROOT_PATH = process.cwd();
var nconf = require('nconf');

nconf.argv()
.env()
.file('config', {
  file: ROOT_PATH + '/conf/config.json'
});

module.exports = nconf;
