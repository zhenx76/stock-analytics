//
// Module to manage the stock symbol table
//

var logger = require('./utility').logger;
var when = require('when');
var assert = require('assert');

function getStockFromExchange(docClient, exchange, symbol, callback) {
    try {
        // Check whether the stock exists
        var symbolTableName = 'stocks-' + exchange;
        var msg = '';

        var params = {
            TableName: symbolTableName,
            Key:{"Symbol": symbol}
        };

        docClient.get(params, function(err, data) {
            if (err) {
                msg = 'Unable to get item from ' + symbolTableName + '. Error JSON:' + JSON.stringify(err, null, 2);
                callback(new Error(msg), null);
            } else {
                if (data.hasOwnProperty('Item')) {
                    callback(null, data.Item);
                } else {
                    msg = "Stock " + symbol + " doesn't exist in " + symbolTableName;
                    callback(new Error(msg), null);
                }
            }
        });
    } catch (exception) {
        callback(exception, null);
    }
}

exports.getStock = function(docClient, symbol) {
    return when.promise(function(resolve, reject) {
        getStockFromExchange(docClient, 'nasdaq', symbol, function(err, data) {
            if (!err && !!data) { // first check Nasdaq
                logger.info('Found + ' + symbol + ' in Nasdaq');
                data.exchange = 'nasdaq';
                resolve(data);
            } else {
                // trying NYSE
                getStockFromExchange(docClient, 'nyse', symbol, function(err, data) {
                    if (!err && !!data) {
                        logger.info('Found ' + symbol + ' in NYSE');
                        data.exchange = 'nyse';
                        resolve(data);
                    } else {
                        logger.error("Stock " + symbol + " doesn't exist!");
                        reject(data);
                    }
                });
            }
        });
    });
};

exports.updateStock = function(docClient, stockInfo, exchange) {
    exchange = (typeof exchange !== 'undefined') ? exchange : 'nasdaq';
    var symbolTableName = 'stocks-' + exchange;

    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: symbolTableName,
                Key:{'Symbol': stockInfo.Symbol},
                UpdateExpression: 'SET LastUpdatedYear = :year, LastUpdatedQuarter = :quarter',
                ExpressionAttributeValues: {
                    ':year': stockInfo.LastUpdatedYear,
                    ':quarter': stockInfo.LastUpdatedQuarter
                }
            };

            docClient.update(params, function(err, data) {
                if (err) {
                    logger.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(data);
                } else {
                    logger.info("Updated " + stockInfo.Symbol);
                    resolve(data);
                }
            });
        } catch (exception) {
            logger.warn(exception);
            reject(symbol);
        }
    });
};

function getNextStock(docClient, symbolTableName, startKey) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName : symbolTableName,
                Limit: 1 // Rate limiting scan operation
            };

            if (startKey != null) {
                params.ExclusiveStartKey = startKey;
            }

            docClient.scan(params, function(err, data) {
                if (err) {
                    logger.error("Unable to get item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(err);
                } else if (data.hasOwnProperty('Items')) {
                    var records = [];
                    for (var i = 0; i < data.Items.length; i++) {
                        records.push(data.Items[i]);
                    }

                    var result = {records: records};

                    if (data.hasOwnProperty('LastEvaluatedKey') && data.LastEvaluatedKey) {
                        result.nextStartKey = data.LastEvaluatedKey;
                        result.isFinal = false;
                    } else {
                        result.isFinal = true;
                    }

                    resolve(result);
                }
            });
        } catch (exception) {
            logger.warn(exception);
            reject(null);
        }
    });
}

exports.forEachStock = function(docClient, exchange, delay, callback) {
    var startKey = null;
    var isFinal = false;
    var counter = 0;
    var symbolTableName = 'stocks-' + exchange;

    (function scanNextRecord() {
        when.resolve(null)
            .then(function() {
                return getNextStock(docClient, symbolTableName, startKey);
            })
            .then(function(result) {
                isFinal = result.isFinal;
                if (result.hasOwnProperty('nextStartKey')) {
                    startKey = result.nextStartKey;
                }
                counter += result.records.length;
                if (result.records.length > 1) {
                    logger.error('Scan return more records than we asked! result.records.length == '
                        + result.records.length);
                }

                if (result.records.length == 0) {
                    return false; // return false to continue scan next item
                } else {
                    return callback(result.records[0], isFinal);
                }
            })
            .then(function(stopScan) {
                if (isFinal) {
                    logger.info('Scanned ' + counter + ' records from table ' + symbolTableName);
                } else if (stopScan) {
                    logger.info('Stop scan table ' + symbolTableName + ' per user request');
                } else {
                    // For real deployment, throttle dynamodb request
                    // to minimize required read/write units
                    setTimeout(scanNextRecord, delay);
                }
            });
    })();
};
