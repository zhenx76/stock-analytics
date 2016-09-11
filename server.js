var logger = require('./utility').logger;
var express = require('express');
var bodyParser = require('body-parser');
var api = require('./api');

var app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

var port = 8080;

module.exports = {
    init: function() {
        logger.info('Initializing server');
        api.init(app);
    },
    getServer: function() {
        return app;
    },
    start: function() {
        logger.info('Starting server on port ' + port);
        app.listen(port);
    }
};