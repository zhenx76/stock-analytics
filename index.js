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

function cleanup(options, exitCode) {

    if (options.cleanup) {
        server.stop();
    }

    if (exitCode || exitCode === 0) logger.error(exitCode);
    if (options.exit) process.exit();
}

//do something when app is closing
process.on('exit', cleanup.bind(null, {cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', cleanup.bind(null, {exit:true}));

// catches "kill pid" (for example: nodemon restart)
process.on('SIGUSR1', cleanup.bind(null, {exit:true}));
process.on('SIGUSR2', cleanup.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', cleanup.bind(null, {exit:true}));
