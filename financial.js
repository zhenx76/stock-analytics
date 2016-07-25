//
// Module to manage the financial tables
//

var logger = require('./utility').logger;
var when = require('when');

var annualFinancialTableName = 'financial-annual';
var quaterFinancialTableName = 'financial-quarter';

function initTable(db, tableName, schema, attributes) {
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
                        ReadCapacityUnits: 5,
                        WriteCapacityUnits: 5
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
            return initTable(db, annualFinancialTableName, schema, attributes);
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
            return initTable(db, quaterFinancialTableName, schema, attributes);
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