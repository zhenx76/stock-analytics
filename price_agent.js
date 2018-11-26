//
// Module to get stock price
//

var when = require('when');
var config = require('./config');
var alpha = require('alphavantage')({key: config.alphaVantageKey});
var mqtt = require('mqtt');
var logger = require('./utility').logger;

/*
 * Use Alpha Vantage API to return the price snapshot
 */
function getPriceSnapshotSingle(symbol) {
    return when.promise(function (resolve, reject) {
        alpha.data.daily(symbol).then(function(result) {
            var prices = [];
            var key = 'Time Series (Daily)';
            var priceKey = '4. close';
            if (result.hasOwnProperty(key) && result[key]) {
                var record = result[key];
                for (var dateKey in record) {
                    if (record.hasOwnProperty(dateKey)) {
                        var date = new Date(dateKey);
                        if (date) {
                            prices.push({
                                date: date,
                                price: parseFloat(record[dateKey][priceKey])
                            })
                        }
                    }
                }
            }

            // sort prices array by date
            prices.sort(function (p1, p2) {
                return p2.date - p1.date;
            });

            resolve({
                price: prices[0].price,
                change: prices[0].price - prices[1].price,
                changeInPercent: (prices[0].price - prices[1].price)/prices[1].price
            });

        }).catch(function(err) {
            reject(err);
        });
    });
}

/*
 * Since the Alpha Vantage API is very slow, I use an asynchronous design to download the quotes:
 * 1. Other applications will tell price agent which symbols to track through mqtt (stock/symbols)
 * 2. Stock agent will publish the stock quotes through mqtt (stock/quotes)
 * 3. The price agent is always running in the background and download the quotes periodically (every 5 minutes)
 * 4. For each iteration, price agent will pace at 1 second interval
 */

var symbolsToTrack = [];
var updateInterval = 5 * 60 * 1000; // 5 minutes
var delay = 15 * 1000; // 15 seconds

function getPriceSnapshot(symbols) {
    return when.promise(function (resolve, reject) {
        try {
            if (symbols.length == 0) {
                resolve(null);
                return;
            }

            var snapshot = {};
            var index = 0;
            var retries = 0;

            (function getNextPriceSnapshot() {
                var symbol = symbols[index];
                getPriceSnapshotSingle(symbol).then(function(result) {
                    snapshot[symbol] = result;
                    publishQuote(snapshot);

                    if (++index == symbols.length) {
                        resolve(snapshot);
                    } else {
                        retries = 0;
                        setTimeout(getNextPriceSnapshot, delay);
                    }
                }).catch(function(err) {
                    if (retries++ < 2) {
                        setTimeout(getNextPriceSnapshot, retries * delay);
                    } else {
                        logger.error("Skip fetching symbol " + symbol + ' due to error: ', JSON.stringify(err));

                        if (++index == symbols.length) {
                            resolve(snapshot);
                        } else {
                            setTimeout(getNextPriceSnapshot, retries * delay);
                        }
                    }
                });
            })();
        } catch (error) {
            logger.error("Unable to download quote from Yahoo Finance.", JSON.stringify(error, null, 2));
            resolve(null);
        }
    });
}

var mqttClient;
var TOPIC_SYMBOL = config.mqttTopicSymbol;
var TOPIC_QUOTE = config.mqttTopicQutoes;

function downloadQuotes() {
    if (symbolsToTrack.length) {
        getPriceSnapshot(symbolsToTrack).then(function(snapshot) {
            //logger.info('Publish quotes on topic: ' + TOPIC_QUOTE + ' for symbols:' + symbolsToTrack.toString());
            //mqttClient.publish(TOPIC_QUOTE, JSON.stringify(snapshot));
            logger.info('Refresh quotes complete');
        });
    }
}

function publishQuote(snapshot) {
    logger.info('Publish quotes on topic: ' + TOPIC_QUOTE + ' for symbols:' + Object.keys(snapshot).toString());
    mqttClient.publish(TOPIC_QUOTE, JSON.stringify(snapshot));
}

mqttClient = mqtt.connect(config.mqttBrokerURL);

mqttClient.on('connect', function() {
    logger.info('Connected to ' + config.mqttBrokerURL);

    // subscribe to symbol topic as input
    logger.info('Subscribing to topic: ' + TOPIC_SYMBOL);
    mqttClient.subscribe(TOPIC_SYMBOL);

    // start a timer to download quotes periodically
    setInterval(downloadQuotes, updateInterval);
});

mqttClient.on('message', function(topic, message) {
    if (topic == TOPIC_SYMBOL) {
        try {
            var params, action, symbols;

            // Parse command parameters
            params = JSON.parse(message.toString());
            if (params.hasOwnProperty('action')) {
                action = params['action'].toUpperCase();
            }
            if (params.hasOwnProperty('symbols')) {
                symbols = params['symbols'];
            }

            // Validate command parameters
            if (action != 'ADD' && action != 'DELETE') {
                throw new Error('Invalid action parameter');
            }
            if (!Array.isArray(symbols) || !symbols.length) {
                throw new Error('Invalid symbols parameter');
            }

            logger.info(action + ' symbols: ' + symbols.toString());

            // Update symbolsToTrack
            for (var i = 0; i < symbols.length; i++) {
                var symbol = symbols[i];

                if (!/^[a-z]+$/i.test(symbol)) {
                    //
                    // Alpha Vantage API limitation: the symbol has to be all letters
                    //
                    logger.info('Ignore symbol that contains non alphabetic: ' + symbol);
                    continue;
                }

                if (action == 'ADD') {
                    if (symbolsToTrack.indexOf(symbol) == -1) {
                        symbolsToTrack.push(symbol);
                    }
                } else if (action == 'DELETE') {
                    var index = symbolsToTrack.indexOf(symbol);
                    if (index > -1) {
                        symbolsToTrack.splice(index, 1);
                    }
                }
            }

            // Download quotes immediately
            if (action == 'ADD') {
                downloadQuotes();
            }

        } catch (err) {
            logger.error(topic + ': ' + err.message +  ' in: ' + message.toString());
        }
    }
});

/*
 Yahoo Finance API is no longer available
 function getPriceSnapshot(symbols) {
 return when.promise(function (resolve, reject) {
 try {
 if (!Array.isArray(symbols)) {
 symbols = [symbols]; // make it an array
 }

 // Get price snapshot from Yahoo Finance
 yahooFinance.snapshot({
 symbols: symbols,
 fields: ['s', 'p', 'l1', 'c1', 'p2']
 }, function (err, results) {
 if (err) {
 reject(err);
 } else {
 var snapshot = {};
 for (var i = 0; i < results.length; i++) {
 var record = results[i];
 if (record.hasOwnProperty('symbol') &&
 record.hasOwnProperty('lastTradePriceOnly') &&
 record.lastTradePriceOnly) {
 snapshot[record.symbol] = {
 price: record.lastTradePriceOnly,
 change: record.change,
 changeInPercent: record.changeInPercent
 };
 }
 }
 resolve(snapshot);
 }
 });
 } catch (error) {
 logger.error("Unable to download quote from Yahoo Finance.", JSON.stringify(error, null, 2));
 resolve(null);
 }
 });
 }
 */