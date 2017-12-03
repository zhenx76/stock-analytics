//
// Monitor stock prices for each stock in every user's portfolio
// Compare the current price with each user's buy/sell target.
// If a target is meet, notify the user through email.
//

var when = require('when');
var priceAgent = require('./price_agent');
var logger = require('./utility').logger;
var config = require('./config');
var portfolio = require('./portfolio');
var User = require('./user-mgmt').User;

var AWS = require('aws-sdk');

//
// Get a handle of Dynamodb
//
var db, docClient;
if (config.local) {
    db = new AWS.DynamoDB({
        endpoint: new AWS.Endpoint('http://localhost:8000'),
        region: 'us-west-1'
    });
    docClient = new AWS.DynamoDB.DocumentClient({
        endpoint: new AWS.Endpoint('http://localhost:8000'),
        region: 'us-west-1'
    });
} else {
    db = new AWS.DynamoDB();
    docClient = new AWS.DynamoDB.DocumentClient();
}

// Get a handle of SES
var ses = new AWS.SES({region:'us-west-2'});

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
            priceAgent.getPriceSnapshot(symbol).then(function(snapshot) {
                stockPrices[symbol] = snapshot.price;
                resolve(snapshot.price);
            }).catch(function(err) {
               reject(err);
            });
        }
    });
}

function checkPriceTargets(record, price) {
    var msg = record.symbol + ' price $' + price.toFixed(2);
    var action;
    if (!!record.profitPrice && price >= record.profitPrice) {
        msg += ' meets profit target $' + record.profitPrice.toFixed(2);
        action = 'SELL';
    } else if (!!record.nextPriceTarget && price >= record.nextPriceTarget) {
        msg += ' beats next price target $' + record.nextPriceTarget.toFixed(2);
        action = 'BUY';
    } else if (!!record.stopLossPrice && price < record.stopLossPrice) {
        msg += ' misses stop loss price $' + record.stopLossPrice.toFixed(2);
        action = 'SELL';
    }

    if (action) {
        msg += '. Consider ' + action + '.';
        return msg;
    } else {
        return null;
    }
}

function sendEmailToUser(username, msg) {
    return when.promise(function(resolve, reject) {
        var user = User.find(username);
        if (!user) {
            reject(new Error('Invalid user ' + username));
        } else {
            var from = 'zhenx76@me.com'; // Need to update to domain email
            var params = {
                Source: from,
                Destination: {
                    ToAddresses: [user.email]
                },
                Message: {
                    Subject: {
                        Data: 'STOCK ALERT'
                    },
                    Body: {
                        Text: {
                            Data: msg
                        }
                    }
                }
            };

            ses.sendEmail(params, function(err, data) {
                if (err || !data) {
                    logger.error('Failed to send email to ' + username);
                    reject(err);
                } else {
                    logger.info('MessageId ' + data.MessageId + ' sent to ' + username + ' ' + user.email);
                    resolve();
                }
            });
        }
    });
}

when.resolve()
    .then(function() { return User.init(); })
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
