//
// Module to manage the stock symbol table
//

var logger = require('./utility').logger;
var when = require('when');
var assert = require('assert');

var symbolTableName = 'stocks-nasdaq';

exports.getStock = function(docClient, symbol) {
    return when.promise(function(resolve, reject) {
        try {
            // Check whether the stock exists
            var params = {
                TableName: symbolTableName,
                Key:{"Symbol": symbol}
            };

            docClient.get(params, function(err, data) {
                if (err) {
                    logger.error("Unable to get item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(data);
                } else {
                    if (data.hasOwnProperty('Item')) {
                        resolve(data.Item);
                    } else {
                        logger.warn("Stock " + symbol + "doesn't exist!");
                        reject(data);
                    }
                }
            });
        } catch (exception) {
            logger.warn(exception);
            reject(symbol);
        }
    });
};

exports.updateStock = function(docClient, stockInfo) {
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

function getNextStock(docClient, startKey) {
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

exports.forEachStock = function(docClient, delay, callback) {
    var startKey = null;
    var isFinal = false;
    var counter = 0;

    (function scanNextRecord() {
        when.resolve(null)
            .then(function() {
                return getNextStock(docClient, startKey);
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
