var when = require('when');
var logger = require('./utility').logger;
var server = require('./server');

logger.info('Stock Analytics Server 0.1');
server.init()
    .then(function() {
        server.start();
    })
    .catch(function(error) {
        logger.error('Exit with error: ' + JSON.stringify(error, null, 2));
    });

//process.stdin.resume();
//process.on('SIGINT', function() {
//    logger.error("Caught interrupt signal");
//    process.exit();
//});