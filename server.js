var logger = require('./utility').logger;
var when = require('when');
var express = require('express');
var bodyParser = require('body-parser');
var api = require('./api');
var User = require('./user-mgmt').User;

var app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

var port = process.env.PORT || 8080;

function initWebClient(app) {
    logger.info('Initializing web client');

    //
    // http://stackoverflow.com/questions/20396900/angularjs-routing-in-expressjs
    //
    // In order to use AngularJS html5mode along with Express, you must serve "index.html" for all requests to
    // leave all routing up to AngularJS. I had this same problem a while back.
    // So first, you declare all API endpoint routes, any static file directories (CSS, JS, partials, etc),
    // and then serve index.html for all remaining requests.
    //
    app.use('/styles', express.static(__dirname + '/client/web/styles'));
    app.use('/scripts', express.static(__dirname + '/client/web/scripts'));
    app.use('/assets', express.static(__dirname + '/client/web/assets'));
    app.use('/app', express.static(__dirname + '/client/web/app'));
    app.use('/maps', express.static(__dirname + '/client/web/maps'));

    // serve index.html for all remaining routes, in order to leave routing up to angular
    app.all("/*", function(req, res, next) {
        res.sendfile("index.html", { root: __dirname + "/client/web" });
    });
}

module.exports = {
    init: function() {
        logger.info('Stock analytics v0.1');
        return when.promise(function(resolve, reject) {
            User.init()
                .then(function() {
                    api.init(app);
                    initWebClient(app);
                    resolve(null);
                })
                .catch(function(error) {
                    logger.error('Fail to init server' + + JSON.stringify(err, null, 2));
                    reject(error);
                });
        });
    },
    getServer: function() {
        return app;
    },
    start: function() {
        logger.info('Starting server on port ' + port);
        app.listen(port);
    }
};