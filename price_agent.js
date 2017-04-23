//
// Module to get stock price
//

var when = require('when');
var yahooFinance = require('yahoo-finance');
var logger = require('./utility').logger;

function getPriceSnapshot(symbols) {
    return when.promise(function (resolve, reject) {
        try {
            if (!Array.isArray(symbols)) {
                symbols = [symbols]; // make it an array
            }

            // Get price snapshot from Yahoo Finance
            yahooFinance.snapshot({
                symbols: symbols,
                fields: ['s', 'p', 'l1']
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
                            snapshot[record.symbol] = record.lastTradePriceOnly;
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

exports.getPriceSnapshot = getPriceSnapshot;

/*
// Unit Test
var SYMBOLS = [
    'AAPL',
    'GOOG',
    'MSFT',
    'IBM',
    'AMZN',
    'ORCL',
    'INTC',
    'QCOM',
    'FB',
    'CSCO',
    'SAP',
    'TSM',
    'BIDU',
    'EMC',
    'HPQ',
    'TXN',
    'ERIC',
    'ASML',
    'CAJ',
    'YHOO'
];

getPriceSnapshot(SYMBOLS)
    .then(function (snapshot) {
        logger.info(JSON.stringify(snapshot, null, 2));
    })
    .catch(function () {
        logger.error('Error');
    });
*/
