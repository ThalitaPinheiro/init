'use strict';

var ROOT_PATH = process.cwd();

var fmt = require('util').format;
var winston = require('winston');
var config = require(ROOT_PATH + '/lib/commons/config');

var formatter = function(options) {
  var msg = fmt('%s [%s] %s', new Date().toISOString(),
    options.level.toUpperCase(), (options.message || ''));

  if (options.meta && Object.keys(options.meta).length) {
    msg += fmt('\n\t%s', JSON.stringify(options.meta));
  }

  return winston.config.colorize(options.level, msg);
};

var transports = null;
var handlers = null;

var appender = config.get('logging');
if (appender) {
  var stdout = appender.console;
  stdout.formatter = formatter;

  var file = appender.file;
  file.formatter = formatter;

  var handler = appender.exception;
  handler.formatter = formatter;

  transports = [
    new winston.transports.Console(stdout),
    new (require('winston-daily-rotate-file'))(file)
  ];
  handlers = [
    new (require('winston-daily-rotate-file'))(handler)
  ];
} else {
  transports = [
    new winston.transports.Console({
      level: 'debug',
      silent: false,
      colorize: true,
      timestamp: false,
      json: false,
      prettyPrint: true,
      formatter: formatter
    })
  ];
  handlers = [];
}

var logger = new winston.Logger({
  transports: transports,
  exceptionHandlers: handlers,
  exitOnError: false
});

module.exports = logger;
