//
// Scrape the stock financial information from web
// Data is stored into dynamodb
//

// Before deploy to AWS, change local to false
var local = false;

var when = require('when');
var logger = require('./utility').logger;
var stocks = require('./stock');
var finanicals = require('./financial');
var scraper = require('./scraper');

var AWS = require('aws-sdk');
AWS.config.region = 'us-west-1';

//
// Get a handle of Dynamodb
//
var db, docClient;
if (local) {
    db = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:8000')});
    docClient = new AWS.DynamoDB.DocumentClient({endpoint: new AWS.Endpoint('http://localhost:8000')});
} else {
    db = new AWS.DynamoDB();
    docClient = new AWS.DynamoDB.DocumentClient();
}

//
// Go through each stock in database and scrape its financial data
//
var delay = local ? 0 : 1000;

when.resolve(null)
    .then(function() { return finanicals.initFinancialTables(db); })
    .then(function() {
        stocks.forEachStock(docClient, delay, function(stockInfo, isFinal) {
            return when.promise(function(resolve, reject) {
                try {
                    logger.info('Processing ' + stockInfo.Symbol);

                    when.resolve(null)
                        .then(function () {
                            return scraper.scrape(stockInfo.Symbol, false);
                        })
                        .then(function (financialRecords) {
                            if (!financialRecords || financialRecords.length == 0) {
                                // Empty records, skip the rest
                                return when.reject();
                            }

                            var startIndex = 0;
                            //if (stockInfo.hasOwnProperty('LastUpdatedYear')) {
                            //    for (startIndex = 0; startIndex < financialRecords.length; startIndex++) {
                            //        if (financialRecords[startIndex].Year == stockInfo.LastUpdatedYear) {
                            //            startIndex++;
                            //            break;
                            //        }
                            //    }
                            //}

                            stockInfo.LastUpdatedYear = financialRecords[financialRecords.length-1].Year;

                            if (startIndex < financialRecords.length) {
                                return finanicals.addFinancialRecords(docClient,
                                    financialRecords.slice(startIndex), false);
                            } else {
                                return true;
                            }
                        })
                        .then(function () {
                            return scraper.scrape(stockInfo.Symbol, true);
                        })
                        .then(function (financialRecords) {
                            if (!financialRecords || financialRecords.length == 0) {
                                // Empty records, skip
                                return when.reject();
                            }

                            var startIndex = 0;
                            //if (stockInfo.hasOwnProperty('LastUpdatedQuarter')) {
                            //    for (startIndex = financialRecords.length - 1; startIndex >= 0; startIndex--) {
                            //        if (financialRecords[startIndex].Quarter == stockInfo.LastUpdatedQuarter) {
                            //            startIndex++;
                            //            break;
                            //        }
                            //    }
                            //}

                            stockInfo.LastUpdatedQuarter = financialRecords[financialRecords.length-1].Quarter;

                            if (startIndex < financialRecords.length) {
                                return finanicals.addFinancialRecords(docClient,
                                    financialRecords.slice(startIndex), true);
                            } else {
                                return true;
                            }
                        })
                        .then(function() {
                            // Update stock timestamps
                            return stocks.updateStock(docClient, stockInfo);
                        })
                        .then(function() {
                            resolve(false); // true means stop scanning the table
                            return true;
                        })
                        .catch(when.TimeoutError, function() {
                            logger.error('Promise timeout on processing !' + stockInfo.Symbol);
                            resolve(false); // true means stop scanning the table
                            return false;
                        })
                        .catch(function() {
                            logger.info('Skipping ' + stockInfo.Symbol);
                            resolve(false); // true means stop scanning the table
                            return false;
                        });
                } catch (exception) {
                    logger.warn(exception);
                    reject(null);
                }
            });
        });
    });
