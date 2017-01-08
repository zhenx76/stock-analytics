//
// Monitor stock prices for each stock in every user's portfolio
// Compare the current price with each user's buy/sell target.
// If a target is meet, notify the user through email.
//

var when = require('when');
var logger = require('./utility').logger;
var config = require('./config');
var portfolio = require('./portfolio');

var AWS = require('aws-sdk');
AWS.config.region = 'us-west-1';

//
// Get a handle of Dynamodb
//
var db, docClient;
if (config.local) {
    db = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:8000')});
    docClient = new AWS.DynamoDB.DocumentClient({endpoint: new AWS.Endpoint('http://localhost:8000')});
} else {
    db = new AWS.DynamoDB();
    docClient = new AWS.DynamoDB.DocumentClient();
}

//
// Go through each user stock position in database, for each stock
// - get the stock quote from Yahoo Finance
// - notify user if it triggers certain conditions
//
var delay = config.local ? 0 : 1000;

when.resolve()
    .then(function() { return portfolio.initPortfolioTable(db); })
    .then(function() {
        portfolio.forEachUserStockPosition(docClient, delay, function(record, isFinal) {
            return when.promise(function(resolve, reject) {
                try {
                    logger.info('Processing ' + record.symbol + ' for ' + record.username);
                    logger.info(JSON.stringify(record));
                } catch (exception) {
                    logger.warn(exception);
                    reject(null);
                }
            });
        });
    });
