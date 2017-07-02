// Generate ticker symbol table by parsing the CSV file
// - The CSV file is downloaded from nasdaq.com
// - Read the CSV from S3 bucket
// - Generate the "stock" table in dynamodb

var logger = require('./utility').logger;
var when = require('when');
var fs = require('fs');
var parseArgs = require('minimist');
var config = require('./config');
var local = config.local;

var AWS = require('aws-sdk');
AWS.config.region = 'us-west-1';

var parse = require('csv-parse');
var parser = parse({delimiter: ',', comment: '###-!'});
var output = [];

// Use the writable stream api
parser.on('readable', function() {
    var record;
    while (record = parser.read()) {
        output.push(record);
    }
});

// Catch any error
parser.on('error', function(err) {
    logger.error(err.message);
});

// When we are done, test that the parsed output matched what expected
parser.on('finish', function() {
    var index = 0;
    (function processRecord() {
        if (index < output.length) {
            var record = output[index];
            addStock(tableName, record)
                .then(function() {
                    var timeout = 0;
                    if (!local) {
                        // For real deployment, throttle dynamodb request
                        // to minimize required read/write units
                        timeout = 1000;
                    }
                    index++;
                    setTimeout(processRecord, timeout);
                });
        } else {
            logger.info(output.length + ' records processed');
        }
    })();
});

// Create stock table in dynamodb
var db, docClient;

if (local) {
    db = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:8000')});
    docClient = new AWS.DynamoDB.DocumentClient({endpoint: new AWS.Endpoint('http://localhost:8000')});
} else {
    db = new AWS.DynamoDB();
    docClient = new AWS.DynamoDB.DocumentClient();
}

function initTable(tableName) {
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
                    KeySchema: [
                        {AttributeName: "Symbol", KeyType: "HASH"}  //Partition key
                    ],
                    AttributeDefinitions: [
                        {AttributeName: "Symbol", AttributeType: "S"}
                    ],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1
                    }
                };

                db.createTable(params, function (err, data) {
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

function addStock(tableName, record) {
    return when.promise(function(resolve, reject) {
        try {
            var symbol = record[0];
            var name = record[1];
            var marketCap = parseFloat(record[3]);
            var adrTso = record[4];
            var ipoYear = record[5];
            var sector = record[6];
            var industry = record[7];
            var quote = record[8];

            // Check whether the stock is already in the table
            var params = {
                TableName: tableName,
                Key:{"Symbol": symbol}
            };

            docClient.get(params, function(err, data) {
                if (err) {
                    logger.error("Unable to get item. Error JSON:", JSON.stringify(err, null, 2));
                    reject(data);
                } else {
                    if (data.hasOwnProperty('Item')) {
                        //logger.info(symbol + ' exists, skip.');
                        resolve(data);
                    } else {
                        // Create a new item for the stock
                        var item = {
                            "Symbol": symbol,
                            "Name": name,
                            "MarketCap": marketCap,
                            "Sector": sector,
                            "Industry": industry,
                            "Quote": quote
                        };

                        if (adrTso != 'n/a') {
                            item["ADR-TSO"] = parseInt(adrTso);
                        }
                        if (ipoYear != 'n/a') {
                            item["IPOYear"] = parseInt(ipoYear);
                        }

                        var params2 = {
                            TableName:tableName,
                            Item: item
                        };

                        docClient.put(params2, function(err, data) {
                            if (err) {
                                logger.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                                reject(data);
                            } else {
                                logger.info("Added item for " + symbol);
                                resolve(data);
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
}

var argv = parseArgs(process.argv.slice(2));
var argvT = argv.t || 'nasdaq';
var csv = argvT.toLowerCase() + '.csv';
var tableName = 'stocks-' + argvT.toLowerCase();

console.log('Generate tickers table ' + tableName + ' from ' + csv);

when.resolve(null)
    .then(function() {return initTable(tableName);})
    .then(function() {
        var input;
        if (local) {
            // Read the file from local file system
            input = fs.createReadStream(__dirname + '/' + csv);
        } else {
            // Read the file from S3
            var s3 = new AWS.S3();
            var params = {Bucket: 'stock-analytics', Key: csv};
            input = s3.getObject(params).createReadStream();
        }

        // Start parsing CSV
        input.pipe(parser);
    });

console.log('Done');
