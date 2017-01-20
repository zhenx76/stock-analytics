//
// Monitor stock prices for each stock in every user's portfolio
// Compare the current price with each user's buy/sell target.
// If a target is meet, notify the user through email.
//

var when = require('when');
var yahooFinance = require('yahoo-finance');
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

var stockPrices = {};

function getPriceSnapshot(symbol) {
    return when.promise(function(resolve, reject) {
        if (stockPrices.hasOwnProperty(symbol)) {
            resolve(stockPrices[symbol]);
        } else {
            // Get price snapshot from Yahoo Finance
            yahooFinance.snapshot({
                symbol: symbol,
                fields: ['s', 'p', 'l1']
            }, function (err, snapshot) {
                if (err) {
                    reject(err);
                } else {
                    if (snapshot.hasOwnProperty('previousClose') && snapshot.previousClose) {
                        stockPrices[symbol] = snapshot.previousClose;
                        resolve(snapshot.previousClose);
                    }
                }
            });
        }
    });
}

function checkPriceTargets(record, price) {
    var msg = 'Previous closing price $' + price.toFixed(2);
    var action;
    if (price >= record.profitPrice) {
        msg += ' meets profit target $' + record.profitPrice.toFixed(2);
        action = 'SELL';
    } else if (price >= record.nextPriceTarget) {
        msg += ' beats next price target $' + record.nextPriceTarget.toFixed(2);
        action = 'BUY';
    } else if (price < record.stopLossPrice) {
        msg += ' misses stop loss price $' + record.stopLossPrice.toFixed(2);
        action = 'SELL';
    }

    if (action) {
        msg += '\nConsider ' + action;
        return msg;
    } else {
        return null;
    }
}

function sendEmailToUser(username, msg) {
    logger.info(msg);
}

when.resolve()
    .then(function() { return portfolio.initPortfolioTable(db); })
    .then(function() {
        portfolio.forEachUserStockPosition(docClient, delay, function(record, isFinal) {
            return when.promise(function(resolve, reject) {
                try {
                    logger.info('Processing ' + record.symbol + ' for ' + record.username);

                    when.resolve()
                        .then(function() {
                            return getPriceSnapshot(record.symbol);
                        })
                        .then(function(price) {
                            var msg = checkPriceTargets(record, price);
                            if (msg) {
                                return sendEmailToUser(record.username, msg);
                            }
                        })
                        .then(function() {
                            resolve(false); // true means stop scanning the table
                        })
                        .catch(when.TimeoutError, function() {
                            logger.error('Promise timeout on processing !' + record.symbol + ':' + record.username);
                            resolve(false); // true means stop scanning the table
                        })
                        .catch(function(err) {
                            logger.error("Skipping: Unable to process record.", JSON.stringify(err, null, 2));
                            resolve(false);
                        });
                } catch (exception) {
                    logger.error(exception);
                    reject(null);
                }
            });
        });
    });
