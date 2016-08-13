'use strict';
var express = require('express');
var router = new express.Router();

var helloByName = require('./controllers/helloByName')

router.get('/', function(req, res) {
  res.status(200).send('Hello World!!!');
});

router.get('/name/:name', helloByName);

module.exports = router;
