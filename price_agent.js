//
// Module to get stock price
//

var when = require('when');
var config = require('./config');
var alpha = require('alphavantage')({key: config.alphaVantageKey});
var logger = require('./utility').logger;

function getPriceSnapshot(symbols) {
    return when.promise(function (resolve, reject) {
        try {
            if (!Array.isArray(symbols)) {
                symbols = [symbols]; // make it an array
            }

            // Query daily price using Alpha Vantage API
            var index = 0;
            var snapshot = {};

            (function getPriceSnapshotSingle() {
                var symbol = symbols[index];
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
                                        price: record[dateKey][priceKey]
                                    })
                                }
                            }
                        }
                    }

                    // sort prices array by date
                    prices.sort(function (p1, p2) {
                        return p2.date - p1.date;
                    });

                    snapshot[symbol] = {
                        price: prices[0].price,
                        change: prices[0].price - prices[1].price,
                        changeInPercent: (prices[0].price - prices[1].price)/prices[1].price
                    };

                    if (++index == symbols.length) {
                        resolve(snapshot);
                    } else {
                        setTimeout(getPriceSnapshotSingle, 0);
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
