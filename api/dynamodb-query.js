//
// Query interface to Dynamodb tables
//

var when = require('when');
var logger = require('../utility').logger;
var stocks = require('../stock');
var finanicals = require('../financial');
var portfolio = require('../portfolio');
var config = require('../config');

var local = config.local;

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

function parseQuarter(q_str)
{
    try {
        var parts = q_str.split('-');
        var months = {'MON':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12};

        return {
            year: parseInt(parts[2]),
            month: months[parts[1].toUpperCase()],
            day: parseInt(parts[0])
        };
    } catch (exception) {
        logger.warn('Invalid quarter string: ' + q);
        return null;
    }
}

function compareQuarters(q_str1, q_str2)
{
    q1 = parseQuarter(q_str1);
    q2 = parseQuarter(q_str2);

    if (!q1 && !q2) return 0;
    if (!q1) return -1;
    if (!q2) return 1;

    if (q1.year != q2.year) {
        return q1.year - q2.year;
    }

    if (q1.month != q2.month) {
        return q1.month - q2.month;
    }

    return q1.day - q2.day;
}

module.exports = {
    run: function(filters) {
        return finanicals.scanEPS(docClient, filters);
    },
    runSymbol: function(symbol) {
        return finanicals.getEPS(docClient, symbol);
    },
    runSymbolList: function(symbols) {
        var index = 0;
        var records = [];

        return when.promise(function(resolve, reject) {
            (function runNextSymbol(symbol) {
                finanicals.getEPS(docClient, symbol)
                    .then(function(record) {
                        records.push({
                            PreviousQuarterGrowth: record.PreviousQuarterGrowth || 'N/A',
                            CurrentAnnualROE: record.CurrentAnnualROE || 'N/A',
                            CurrentAnnualGrowth: record.CurrentAnnualGrowth || 'N/A',
                            CurrentQuarterGrowth: record.CurrentQuarterGrowth || 'N/A',
                            Symbol: record.Symbol
                        });

                        index++;
                        if (index == symbols.length) {
                            resolve(records);
                        } else {
                            runNextSymbol(symbols[index]);
                        }
                    })
                    .catch(function(error) {
                        logger.error(error);
                        reject('Cannot get EPS records for ' + symbol);
                    })
            })(symbols[0]);
        });
    },
    getStockData: function(symbol) {
        return when.promise(function(resolve, reject) {
            when.all([
                // Promise 1: quarterly financial records
                finanicals.getFinancialRecords(docClient, symbol, true),

                // Promise 2: annual financial records
                finanicals.getFinancialRecords(docClient, symbol, false),

                // Promise 3: stock information
                stocks.getStock(docClient, symbol)

            ]).then(function(values) {
                var stockData = {
                    info: {Symbol: symbol, Name: values[2].Name},
                    quarterlyRecords: values[0].sort(function(r1, r2) {
                        return compareQuarters(r1.Quarter, r2.Quarter);
                    }),
                    annualRecords: values[1].sort(function(r1, r2) {
                        return r1.Year - r2.Year;
                    })
                };

                resolve(stockData);

            }).catch(function() {
                reject(symbol);
            });
        });
    },
    getUserStockData: function(username, symbol) {
        return when.promise(function(resolve, reject) {
            when.all([
                // Promise 1: quarterly financial records
                finanicals.getFinancialRecords(docClient, symbol, true),

                // Promise 2: annual financial records
                finanicals.getFinancialRecords(docClient, symbol, false),

                // Promise 3: stock information
                stocks.getStock(docClient, symbol),

                // Promise 4: user stock position information
                portfolio.getUserStockPosition(docClient, username, symbol)

            ]).then(function(values) {
                var stockData = {
                    info: {Symbol: symbol, Name: values[2].Name},
                    quarterlyRecords: values[0].sort(function(r1, r2) {
                        return compareQuarters(r1.Quarter, r2.Quarter);
                    }),
                    annualRecords: values[1].sort(function(r1, r2) {
                        return r1.Year - r2.Year;
                    })
                };

                var stockHoldingRecord = values[3];
                if (stockHoldingRecord) {
                    stockData.userData = {
                        holdings: stockHoldingRecord.holdings,
                        nextPriceTarget: portfolio.getNextPriceTarget(stockHoldingRecord),
                        transactions: stockHoldingRecord.transactions.slice().reverse()
                    }
                }

                resolve(stockData);

            }).catch(function() {
                reject(symbol);
            });
        });
    },
    getUserPositions: function(username) {
        return portfolio.getUserPositions(docClient, username);
    },
    updateUserStockPosition: function(username, symbol, price, shares, datetime, action) {
        return portfolio.updateUserStockPosition(docClient, username, symbol, price, shares, datetime, action);
    }
};

