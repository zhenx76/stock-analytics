//
// Module to manage the portfolio table
//

var logger = require('./utility').logger;
var when = require('when');

var portfolioTableName = 'stocks-portfolio';
var portfolioIndexName = 'stocks-portfolio-symbol-index-1';

function initTable(db, tableName, schema, attributes, readCapacity, writeCapacity, indexName, indexSchema, projections) {
    return when.promise(function(resolve, reject) {
        db.listTables(function (err, data) {
            if (err) {
                logger.error('Fail to open dynamodb: ' + err.message);
                reject(err);
            } else {
                for (var i = 0; i < data.TableNames.length; i++) {
                    if (tableName == data.TableNames[i]) {
                        logger.info('Table ' + tableName + ' already exists.');
                        resolve(tableName);
                        return;
                    }
                }

                logger.info('Creating table ' + tableName);
                var params = {
                    TableName: tableName,
                    KeySchema: schema,
                    AttributeDefinitions: attributes,
                    ProvisionedThroughput: {
                        ReadCapacityUnits: readCapacity,
                        WriteCapacityUnits: writeCapacity
                    },
                    GlobalSecondaryIndexes: [
                        {
                            IndexName: indexName,
                            KeySchema: indexSchema,
                            Projection: {
                                ProjectionType: "INCLUDE",
                                NonKeyAttributes: projections
                            },
                            ProvisionedThroughput: {
                                ReadCapacityUnits: readCapacity,
                                WriteCapacityUnits: writeCapacity
                            }
                        }
                    ]
                };

                db.createTable(params, function(err, data) {
                    if (err) {
                        logger.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
                        reject(err);
                    } else {
                        logger.info("Created table. Table description JSON:", JSON.stringify(data, null, 2));
                        resolve(data);
                    }
                });
            }
        });
    });
}

exports.initPortfolioTable = function(db) {
    var schema = [
        {AttributeName: "User", KeyType: "HASH"},  //Partition key
        {AttributeName: "Symbol", KeyType: "RANGE"}  //Sort key
    ];
    var attributes = [
        {AttributeName: "User", AttributeType: "S"},
        {AttributeName: "Symbol", AttributeType: "S"}
    ];
    var indexName = portfolioIndexName;
    var indexSchema = [
        {AttributeName: "Symbol", KeyType: "HASH"} //Partition key
    ];
    var projections = [
        "CurrentPhase", "NextPriceTarget", "StopLossPrice", "ProfitPrice"
    ];
    return initTable(db, portfolioTableName, schema, attributes, 1, 1, indexName, indexSchema, projections);
};

function getRecordFromItem(Item) {
    return {
        username: Item.User,
        symbol: Item.Symbol,
        totalShares: Item.TotalShares,
        pyramidingPhases: Item.PyramidingPhases,
        holdings: Item.Holdings,
        transactions: Item.Transactions,
        currentPhase: Item.CurrentPhase,
        nextPriceTarget: Item.NextPriceTarget,
        stopLossPrice: Item.StopLossPrice,
        profitPrice: Item.ProfitPrice
    };
}

function getRecordFromIndexItem(Item) {
    return {
        username: Item.User,
        symbol: Item.Symbol,
        totalShares: Item.TotalShares,
        currentPhase: Item.CurrentPhase,
        nextPriceTarget: Item.NextPriceTarget,
        stopLossPrice: Item.StopLossPrice,
        profitPrice: Item.ProfitPrice
    };
}

exports.getStockPositions = function(docClient, symbol) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: portfolioTableName,
                IndexName: portfolioIndexName, // Query on Global Secondary Index
                KeyConditionExpression: "#s = :ssss",
                ExpressionAttributeNames:{"#s": "Symbol"},
                ExpressionAttributeValues: {":ssss": symbol}
            };

            docClient.query(params, function(err, data) {
                if (err) {
                    logger.error("Unable to query items. Error JSON:", JSON.stringify(err, null, 2));
                    reject(err);
                } else {
                    if (data.hasOwnProperty('Items')) {
                        var records = [];
                        data.Items.forEach(function(Item) {
                            var record = getRecordFromIndexItem(Item);

                            if (record.totalShares) {
                                records.push(record);
                            }
                        });

                        resolve(records);
                    } else {
                        logger.info("No stock holdings for " + symbol);
                        resolve(null);
                    }
                }
            });

        } catch (exception) {
            logger.warn(exception);
            reject(exception);
        }
    });
};

exports.getUserPositions = function(docClient, username) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: portfolioTableName,
                KeyConditionExpression: "#u = :nnnn",
                ExpressionAttributeNames:{"#u": "User"},
                ExpressionAttributeValues: {":nnnn": username}
            };

            docClient.query(params, function(err, data) {
                if (err) {
                    logger.error("Unable to query items. Error JSON:", JSON.stringify(err, null, 2));
                    reject(err);
                } else {
                    if (data.hasOwnProperty('Items')) {
                        var records = [];
                        data.Items.forEach(function(Item) {
                            var record = getRecordFromItem(Item);

                            if (record.totalShares && record.holdings.length > 0) {
                                records.push({
                                    symbol: record.symbol,
                                    totalShares: record.totalShares,
                                    phase: record.currentPhase,
                                    nextPriceTarget: record.nextPriceTarget,
                                    profitPrice: record.profitPrice,
                                    stopLossPrice: record.stopLossPrice
                                });
                            }
                        });

                        resolve(records);
                    } else {
                        logger.info("No position of " + symbol + " found for " + username);
                        resolve(null);
                    }
                }
            });

        } catch (exception) {
            logger.warn(exception);
            reject(exception);
        }
    });
};

var getUserStockPosition = exports.getUserStockPosition = function(docClient, username, symbol) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: portfolioTableName,
                Key:{
                    "User": username,
                    "Symbol": symbol
                }
            };

            docClient.get(params, function(err, data) {
                if (err) {
                    logger.error("Unable to get item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(err);
                } else {
                    if (data.hasOwnProperty('Item')) {
                        resolve(getRecordFromItem(data.Item));
                    } else {
                        logger.info("No position of " + symbol + " found for " + username);
                        resolve(null);
                    }
                }
            });

        } catch (exception) {
            logger.warn(exception);
            reject(symbol);
        }
    });
};

var pyramidingModels = [
    {
        phase: 'BASE',
        sharesIncrementalRatio: 3/5,
        priceIncrementalRatio: 1.02,
        cutLossRatio: 0.92
    },
    {
        phase: 'PYRAMIDING_1',
        sharesIncrementalRatio: 2/3,
        priceIncrementalRatio: 1.05/1.02,
        cutLossRatio: 0.99
    },
    {
        phase: 'PYRAMIDING_2',
        sharesIncrementalRatio: 0,
        priceIncrementalRatio: 0,
        cutLossRatio: 0.99
    }
];

var maxProfitLevel = 1.15;

function getPyramidingIndex(phase) {
    for (var i = 0; i < pyramidingModels.length; i++) {
        if (phase == pyramidingModels[i].phase) {
            return i;
        }
    }
    return -1;
}

function initPyramidingPhases(record, price, shares) {
    var pyramidingPhases = record.pyramidingPhases = {};

    for (var i = 0; i < pyramidingModels.length; i++) {
        pyramidingPhases[pyramidingModels[i].phase] = {};
        var currentPhase = pyramidingPhases[pyramidingModels[i].phase];

        if (i == 0) {
            currentPhase.price = price;
            currentPhase.shares = shares;
        } else {
            var lastPhase = pyramidingPhases[pyramidingModels[i - 1].phase];
            currentPhase.price = lastPhase.price * pyramidingModels[i - 1].priceIncrementalRatio;
            currentPhase.shares = lastPhase.shares * pyramidingModels[i - 1].sharesIncrementalRatio;
        }

        currentPhase.stopLossPrice = currentPhase.price * pyramidingModels[i].cutLossRatio;
    }

    return pyramidingPhases;
}

function findTargetPyramidingPhase(record, price) {
    var phase;
    for (var i = 0; i < pyramidingModels.length; i++) {
        phase = pyramidingModels[i].phase;
        if (price < (record.pyramidingPhases[phase].price * 0.99)) {
            if (i == 0) {
                return pyramidingModels[0].phase;
            } else {
                return pyramidingModels[i-1].phase;
            }
        }
    }

    return phase;
}

function initRecord(record, price, shares) {
    record.totalShares = shares;

    initPyramidingPhases(record, price, shares);

    record.holdings.push({
        phase: pyramidingModels[0].phase,
        shares: shares,
        price: price
    });
}

function createNewRecord(username, symbol, price, shares) {
    var record = {
        username: username,
        symbol: symbol,
        totalShares: 0,
        pyramidingPhases: {},
        holdings: [],
        transactions: []
    };

    initRecord(record, price, shares);
    return record;
}

function processBuyTransaction(record, price, shares) {
    var targetPhase = findTargetPyramidingPhase(record, price);

    record.holdings.push({
        phase: targetPhase,
        shares: shares,
        price: price
    });

    record.holdings.sort(function(h1, h2) {
        var idx1 = getPyramidingIndex(h1.phase);
        var idx2 = getPyramidingIndex(h2.phase);
        return idx1 - idx2;
    });

    record.totalShares += shares;
}

function processSellTransaction(record, shares) {
    var holding;

    record.totalShares -= shares;

    while (holding = record.holdings.pop()) {
        if (holding.shares > shares) {
            holding.shares -= shares;
            record.holdings.push(holding);
            break;
        }

        shares -= holding.shares;
    }

    if (record.totalShares == 0) {
        // Sold all the shares, delete pyramid
        record.pyramidingPhases = [];
    }
}

var getNextPriceTarget = exports.getNextPriceTarget = function(record) {
    if (record.totalShares == 0) {
        return {price: 0, shares: 0, stopLossPrice: 0, profitPrice: 0};
    }

    var holding = record.holdings[record.holdings.length-1];
    var index = getPyramidingIndex(holding.phase);
    if (index < pyramidingModels.length - 1) {
        return {
            price: record.pyramidingPhases[pyramidingModels[index+1].phase].price,
            shares: record.pyramidingPhases[pyramidingModels[index+1].phase].shares,
            stopLossPrice: record.pyramidingPhases[pyramidingModels[index].phase].stopLossPrice,
            profitPrice: record.pyramidingPhases[pyramidingModels[0].phase].price * maxProfitLevel
        };
    } else {
        // We are at the top of pyramid, do not buy anymore
        return {
            price: 0,
            shares: 0,
            stopLossPrice: record.pyramidingPhases[holding.phase].stopLossPrice,
            profitPrice: record.pyramidingPhases[pyramidingModels[0].phase].price * maxProfitLevel
        };
    }
};

exports.updateUserStockPosition = function(docClient, username, symbol, price, shares, datetime, action) {
    return when.promise(function(resolve, reject) {
        try {
            getUserStockPosition(docClient, username, symbol).then(function(record) {
                if (!record) {
                    if (action != 'BUY') {
                        logger.error('Invalid transaction');
                        reject(new Error('Invalid transaction'));
                        return;
                    }

                    logger.info('Create a new position of ' + symbol + ' for ' + username);
                    record = createNewRecord(username, symbol, price, shares);
                } else {
                    if (action == 'BUY') {
                        if (record.totalShares == 0) {
                            initRecord(record, price, shares);
                        } else {
                            processBuyTransaction(record, price, shares);
                        }
                    } else if (action == 'SELL') {
                        if (record.totalShares < shares) {
                            logger.error('Not enough shares to sell');
                            reject(new Error('Not enough shares to sell'));
                            return;
                        } else {
                            processSellTransaction(record, shares);
                        }
                    }
                }

                // Update next price targets
                var target = getNextPriceTarget(record);
                if (record.totalShares && record.holdings.length > 0) {
                    record.currentPhase = record.holdings[record.holdings.length - 1].phase;
                    record.nextPriceTarget = target.price;
                    record.stopLossPrice = target.stopLossPrice;
                    record.profitPrice = target.profitPrice;
                }

                // Dynamodb doesn't support Date type
                // Use JSON.parse(JSON.stringify(date)) to convert date to string
                // without extra quotation marks
                record.transactions.push({
                    action: action,
                    price: price,
                    shares: shares,
                    datetime: JSON.parse(JSON.stringify(datetime))
                });

                // Update database
                var expression = 'SET TotalShares = :ts, PyramidingPhases = :pp, Holdings = :hd, Transactions = :tr';
                var attributes = {
                    ':ts': record.totalShares,
                    ':pp': record.pyramidingPhases,
                    ':hd': record.holdings,
                    ':tr': record.transactions
                };


                if (record.totalShares && record.holdings.length > 0) {
                    expression += ', CurrentPhase = :cp, NextPriceTarget = :np, ProfitPrice = :rp, StopLossPrice = :sp';
                    attributes[':cp'] = record.currentPhase;
                    attributes[':np'] = record.nextPriceTarget;
                    attributes[':rp'] = record.profitPrice;
                    attributes[':sp'] = record.stopLossPrice;
                }

                var params = {
                    TableName: portfolioTableName,
                    Key:{
                        "User": username,
                        "Symbol": symbol
                    },
                    UpdateExpression: expression,
                    ExpressionAttributeValues: attributes
                };

                docClient.update(params, function(err, data) {
                    if (err || !data) {
                        logger.error("updateUserStockPosition: Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
                        reject(err);
                    } else {
                        logger.info(action + " " + symbol + " for " + username);

                        resolve({
                            holdings: record.holdings,
                            nextPriceTarget: target,
                            transactions: record.transactions.slice().reverse()
                        });
                    }
                });
            }).catch(function(err) {
                reject(err);
            })
        } catch (exception) {
            logger.warn(exception);
            reject(exception);
        }
    });
};

function scanNextUserStockPosition(docClient, startKey) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: portfolioTableName,
                IndexName: portfolioIndexName,
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
                        var record = getRecordFromIndexItem(data.Items[i]);
                        if (record.totalShares) {
                            records.push(record);
                        }
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

exports.forEachUserStockPosition = function(docClient, delay, callback) {
    var startKey = null;
    var isFinal = false;
    var counter = 0;

    (function scanNextRecord() {
        when.resolve(null)
            .then(function() {
                return scanNextUserStockPosition(docClient, startKey);
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
                    logger.info('Scanned ' + counter + ' records from index ' + portfolioIndexName);
                } else if (stopScan) {
                    logger.info('Stop scan index ' + portfolioIndexName + ' per user request');
                } else {
                    // For real deployment, throttle dynamodb request
                    // to minimize required read/write units
                    setTimeout(scanNextRecord, delay);
                }
            });
    })();
};

