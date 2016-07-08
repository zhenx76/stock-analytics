var when = require('when');
var logger = require('./utility').logger;
var devices = require('./app/devices');
var server = require('./server');

devices.init()
    .then(function() { devices.start(); })
    .then(function() { server.init(); })
    .then(function() { server.start(); });

//process.stdin.resume();
//process.on('SIGINT', function() {
//    logger.error("Caught interrupt signal");
//    process.exit();
//});