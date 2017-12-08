//
// Module to get stock price
//

var when = require('when');
var config = require('./config');
var alpha = require('alphavantage')({key: config.alphaVantageKey});
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
 * Since the Alpha Vantage API is very slow, I create a cache here
 * for repeated access. Also create a background thread to fetch the price
 * every 5 minutes from 9am to 4pm EST on weekdays.
 */
var priceCache = {};
var priceCacheUpdateInterval = 5 * 60 * 1000;
var stopUpdatePrice = false;

function isEmptyObject(obj) {
    return !Object.keys(obj).length;
}

function updatePriceCache() {
    if (stopUpdatePrice) {
        // Get a signal stop fetch prices
        return;
    }

    var days = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    var now = new Date();
    var day = days[now.getDay()];
    var hour = now.getHours();

    if ((day == 'Saturday' || day == 'Sunday' || hour < 6 || hour >= 14) || isEmptyObject(priceCache)) {
        // We only update the cache on weekday during work hours.
        setTimeout(updatePriceCache, priceCacheUpdateInterval);
        return;
    }

    // Go through each symbol in the cache
    var symbols = [];
    for (var symbol in priceCache) {
        if (priceCache.hasOwnProperty(symbol)) {
            symbols.push(symbol);
        }
    }

    var index = 0;
    (function updateNextCacheEntry() {
        symbol = symbols[index];
        getPriceSnapshotSingle(symbol).then(function(snapshot) {
            priceCache[symbol] = {
                timeStamp: Date.now(),
                quote: snapshot
            };

            if (++index == symbols.length) {
                // reach the end of array, schedule the next update.
                logger.info('Update price cache complete with ' + symbols.length + ' quotes at ' + Date.now());
                setTimeout(updatePriceCache, priceCacheUpdateInterval);
            } else {
                // download the next quote
                setTimeout(updateNextCacheEntry, 0);
            }
        }).catch(function (err) {
            logger.error("Unable to download quote. Try next time", JSON.stringify(err, null, 2));
            setTimeout(updatePriceCache, priceCacheUpdateInterval);
        });
    })();
}

function getPriceSnapshot(symbols) {
    return when.promise(function (resolve, reject) {
        try {
            if (!Array.isArray(symbols)) {
                symbols = [symbols]; // make it an array
            }

            var snapshot = {};
            var missingSymbols = [];

            // Fetch quotes from cache first
            for (var i = 0; i < symbols.length; i++) {
                var symbol = symbols[i];
                if (priceCache.hasOwnProperty(symbol) && priceCache[symbol]) {
                    snapshot[symbol] = priceCache[symbol].quote;
                } else {
                    missingSymbols.push(symbol);
                }
            }

            if (missingSymbols.length == 0) {
                resolve(snapshot);
                return;
            }

            // Download quotes for symbols not in cache
            var index = 0;
            (function getNextPriceSnapshot() {
                symbol = missingSymbols[index];
                getPriceSnapshotSingle(symbol).then(function(result) {
                    snapshot[symbol] = result;

                    // Update cache
                    priceCache[symbol] = {
                        timeStamp: Date.now(),
                        quote: result
                    };

                    if (++index == missingSymbols.length) {
                        resolve(snapshot);
                    } else {
                        setTimeout(getNextPriceSnapshot, 0);
                    }
                }).catch(function(err) {
                    reject(err);
                });
            })();
        } catch (error) {
            logger.error("Unable to download quote from Yahoo Finance.", JSON.stringify(error, null, 2));
            resolve(null);
        }
    });
}

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

exports.getPriceSnapshot = getPriceSnapshot;

exports.init = function() {
    logger.info('start price cache');
    updatePriceCache();
};

/*
// Unit Test
var SYMBOLS = [
    'AAPL',
    'GOOG',
    'MSFT',
    'IBM',
    'AMZN'
    //'ORCL',
    //'INTC',
    //'QCOM',
    //'FB',
    //'CSCO',
    //'SAP',
    //'TSM',
    //'BIDU',
    //'EMC',
    //'HPQ',
    //'TXN',
    //'ERIC',
    //'ASML',
    //'CAJ',
    //'YHOO'
];

getPriceSnapshot(SYMBOLS)
    .then(function (snapshot) {
        logger.info(JSON.stringify(snapshot, null, 2));
    })
    .catch(function () {
        logger.error('Error');
    });
*/
