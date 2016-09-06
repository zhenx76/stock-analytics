//
// Module to manage the financial tables
//

var logger = require('./utility').logger;
var when = require('when');

var annualFinancialTableName = 'financial-annual';
var quaterFinancialTableName = 'financial-quarter';
var epsTableName = 'financial-eps';

function initTable(db, tableName, schema, attributes, readCapacity, writeCapacity) {
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
                    }
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

exports.initFinancialTables = function(db) {
    return when.resolve(null)
        .then(function() {
            var schema = [
                {AttributeName: "Symbol", KeyType: "HASH"},  //Partition key
                {AttributeName: "Year", KeyType: "RANGE"}  //Sort key
            ];
            var attributes = [
                {AttributeName: "Symbol", AttributeType: "S"},
                {AttributeName: "Year", AttributeType: "N"}
            ];
            return initTable(db, annualFinancialTableName, schema, attributes, 5, 5);
        })
        .then(function() {
            var schema = [
                {AttributeName: "Symbol", KeyType: "HASH"},  //Partition key
                {AttributeName: "Quarter", KeyType: "RANGE"}  //Sort key
            ];
            var attributes = [
                {AttributeName: "Symbol", AttributeType: "S"},
                {AttributeName: "Quarter", AttributeType: "S"}
            ];
            return initTable(db, quaterFinancialTableName, schema, attributes, 5, 5);
        })
        .then(function() {
            var schema = [{AttributeName: "Symbol", KeyType: "HASH"}]; // Partition key
            var attributes = [{AttributeName: "Symbol", AttributeType: "S"}];
            return initTable(db, epsTableName, schema, attributes, 10, 2);
        });
};

exports.getFinancialRecords = function(docClient, symbol, isQuarter) {
    return when.promise(function(resolve, reject) {
        try {
            var tableName = isQuarter ? quaterFinancialTableName : annualFinancialTableName;
            var params = {
                TableName: tableName,
                KeyConditionExpression: '#ticker = :tttt',
                ExpressionAttributeNames: {'#ticker' : 'Symbol'},
                ExpressionAttributeValues: {':tttt' : symbol}
            };

            docClient.query(params, function(err, data) {
                if (err) {
                    logger.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(symbol);
                } else {
                    if (data.hasOwnProperty('Items')) {
                        resolve(data.Items);
                    } else {
                        logger.info("No records found for " + symbol);
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

exports.addSingleFinancialRecord = function(docClient, record) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {};
            if (record.hasOwnProperty('Quarter')) {
                params = {
                    TableName: quaterFinancialTableName,
                    Key:{
                        'Symbol': record.Symbol,
                        'Quarter': record.Quarter
                    }
                };
            } else {
                params = {
                    TableName: annualFinancialTableName,
                    Key: {
                        'Symbol': record.Symbol,
                        'Year': record.Year
                    }
                };
            }

            docClient.get(params, function(err, data) {
                if (err) {
                    logger.error("Unable to get item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(data);
                } else {
                    if (data.hasOwnProperty('Item')) {
                        //logger.info('Record exists, skip.');
                        resolve(data);
                    } else {
                        var tableName;
                        if (record.hasOwnProperty('Quarter')) {
                            tableName = quaterFinancialTableName;
                        } else {
                            tableName = annualFinancialTableName;
                        }

                        var params2 = {
                            TableName: tableName,
                            Item: record
                        };

                        docClient.put(params2, function(err, data) {
                            if (err) {
                                logger.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                                reject(record);
                            } else {
                                logger.info("Added item for " + symbol);
                                resolve(record);
                            }
                        });
                    }
                }
            });
        } catch (exception) {
            logger.warn(exception);
            reject(record);
        }
    });
};

function isEmpty(obj) {
    // null and undefined are "empty"
    if (obj == null) return true;

    // Assume if it has a length property with a non-zero value
    // that that property is correct.
    if (obj.length > 0)    return false;
    if (obj.length === 0)  return true;

    // Otherwise, does it have any properties of its own?
    // Note that this doesn't handle
    // toString and valueOf enumeration bugs in IE < 9
    for (var key in obj) {
        if (obj.hasOwnProperty(key)) return false;
    }

    return true;
}

exports.addFinancialRecords = function(docClient, records, isQuarter) {
    return when.promise(function(resolve, reject) {
        try {
            var tableName = isQuarter ? quaterFinancialTableName : annualFinancialTableName;
            var requestItems = {};
            requestItems[tableName] = [];

            for (var i = 0; i < records.length; i++) {
                requestItems[tableName].push({
                    PutRequest: { Item: records[i] }
                });
            }

            var backoff = 1000;

            (function batchWriteRecords() {
                docClient.batchWrite({RequestItems : requestItems}, function (err, data) {
                    if (err) {
                        logger.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                        reject(requestItems);
                    } else {
                        if (data.hasOwnProperty('UnprocessedItems') && !isEmpty(data.UnprocessedItems)) {
                            logger.info(tableName +
                                ': Partial batch write completed. Schedule write next batch after ' +
                                backoff + ' milliseconds');

                            requestItems = data.UnprocessedItems;

                            var timeout = backoff;
                            backoff *= 2;

                            setTimeout(batchWriteRecords, timeout);
                        } else {
                            logger.info('Added ' + records.length + ' records to table ' + tableName);
                            resolve(records);
                        }
                    }
                });
            })();
        } catch (exception) {
            logger.warn(exception);
            reject(records);
        }
    });
};

exports.getEPS = function(docClient, symbol) {
    return when.promise(function(resolve, reject) {
        try {
            var params = {
                TableName: epsTableName,
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
                        logger.warn("EPS data for " + symbol + " doesn't exist!");
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

//
// Essentially the EPS table is the JOIN->Transform results of stock table and finanical-record table.
// Will deprecate this table once we switch over to Amazon EMR (Hadoop + Hive) which has high performance,
// real-time join support.
//
exports.updateEPS = function(docClient, symbol, epsGrowth, roe, revenueGrowth) {
    return when.promise(function(resolve, reject) {
        try {
            if (isEmpty(epsGrowth.annual) || isEmpty(epsGrowth.quarterly)) {
                logger.warn(symbol + ': empty annual or quarterly EPS growth record!');
                reject(symbol);
            }

            if (isEmpty(roe.annual) || isEmpty(roe.quarterly)) {
                logger.warn(symbol + ': empty annual or quarterly ROE record!');
                reject(symbol);
            }

            if (isEmpty(revenueGrowth.annual) || isEmpty(revenueGrowth.quarterly)) {
                logger.warn(symbol + ': empty annual or quarterly Revenue growth record!');
                reject(symbol);
            }

            var expression = 'SET AnnulGrowth = :a, QuarterYearGrowth = :q, AnnualROE = :ar, QuarterROE = :qr, AnnualRevenueGrowth = :arv, QuarterRevenueGrowth = :qrv';
            var attributes = {
                ':a': epsGrowth.annual,
                ':q': epsGrowth.quarterly,
                ':ar': roe.annual,
                ':qr': roe.quarterly,
                ':arv': revenueGrowth.annual,
                ':qrv': revenueGrowth.quarterly
            };

            if (epsGrowth.hasOwnProperty('currentQuarterGrowth')) {
                expression += ', CurrentQuarterGrowth = :cq';
                attributes[':cq'] = epsGrowth.currentQuarterGrowth;
            }

            if (epsGrowth.hasOwnProperty('previousQuarterGrowth')) {
                expression += ', PreviousQuarterGrowth = :pq';
                attributes[':pq'] = epsGrowth.previousQuarterGrowth;
            }

            if (epsGrowth.hasOwnProperty('currentAnnualGrowth')) {
                expression += ', CurrentAnnualGrowth = :ca';
                attributes[':ca'] = epsGrowth.currentAnnualGrowth;
            }

            if (epsGrowth.hasOwnProperty('previousAnnualGrowth')) {
                expression += ', PreviousAnnualGrowth = :pa';
                attributes[':pa'] = epsGrowth.previousAnnualGrowth;
            }

            if (epsGrowth.hasOwnProperty('previousPreviousAnnualGrowthAnnualGrowth')) {
                expression += ', PreviousPreviousAnnualGrowthAnnualGrowth = :ppa';
                attributes[':ppa'] = epsGrowth.previousPreviousAnnualGrowthAnnualGrowth;
            }

            if (roe.hasOwnProperty('currentAnnualROE')) {
                expression += ', CurrentAnnualROE = :car';
                attributes[':car'] = roe.currentAnnualROE;
            }

            if (roe.hasOwnProperty('currentQuarterROE')) {
                expression += ', CurrentQuarterROE = :cqr';
                attributes[':cqr'] = roe.currentQuarterROE;
            }

            var params = {
                TableName: epsTableName,
                Key:{'Symbol': symbol},
                UpdateExpression: expression,
                ExpressionAttributeValues: attributes
            };

            docClient.update(params, function(err, data) {
                if (err) {
                    logger.error("updateEPS: Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(data);
                } else {
                    logger.info("Updated EPS for " + symbol);
                    resolve(data);
                }
            });

        } catch (exception) {
            logger.warn(exception);
            reject(symbol);
        }
    });
};
