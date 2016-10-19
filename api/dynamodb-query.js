//
// Query interface to Dynamodb tables
//

// Before deploy to AWS, change local to false
var local = false;

var when = require('when');
var logger = require('../utility').logger;
var stocks = require('../stock');
var finanicals = require('../financial');

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
    }
};

