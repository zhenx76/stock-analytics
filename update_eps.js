//
// Calculate EPS growth from financial data table and store results back to dynamodb
//

// Before deploy to AWS, change local to false
var local = false;

// Change dump to true to display the EPS data
var dump = false;

var when = require('when');
var logger = require('./utility').logger;
var stocks = require('./stock');
var finanicals = require('./financial');

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

function calculateEPSGrowth(annualRecords, quarterlyRecords) {
    var i, cur, eps_p, eps_c;
    var epsGrowth = {annual: {}, quarterly: {}};

    // Annual growth
    for (i = annualRecords.length-1; i >= 1 ; i--) {
        cur = annualRecords[i].Year.toString();
        eps_p = annualRecords[i-1]['EPS (Diluted)'];
        eps_c = annualRecords[i]['EPS (Diluted)'];

        if (!eps_p) {
            continue;
        }

        epsGrowth.annual[cur] = ((eps_c - eps_p) * 100)/Math.abs(eps_p);

        if (i == annualRecords.length-1) {
            epsGrowth.currentAnnualGrowth = epsGrowth.annual[cur];
        }

        if (i == annualRecords.length-2) {
            epsGrowth.previousAnnualGrowth = epsGrowth.annual[cur];
        }

        if (i == annualRecords.length-3) {
            epsGrowth.previousPreviousAnnualGrowthAnnualGrowth = epsGrowth.annual[cur];
        }
    }

    // Quarterly growth
    for (i = quarterlyRecords.length-1; i >= 4; i --) {
        cur = quarterlyRecords[i].Quarter;
        eps_p = quarterlyRecords[i-4]['EPS (Diluted)'];
        eps_c = quarterlyRecords[i]['EPS (Diluted)'];

        if (!eps_p) {
            continue;
        }

        epsGrowth.quarterly[cur] = ((eps_c - eps_p) * 100)/Math.abs(eps_p);

        if (i == quarterlyRecords.length-1) {
            epsGrowth.currentQuarterGrowth = epsGrowth.quarterly[cur];
        }

        if (i == quarterlyRecords.length-2) {
            epsGrowth.previousQuarterGrowth = epsGrowth.quarterly[cur];
        }
    }

    return epsGrowth;
}

function calculateROE(annualRecords, quarterlyRecords) {
    var i, cur, netIncome, equity;
    var roe = {annual: {}, quarterly: {}};

    // Annual ROE
    for (i = 0; i < annualRecords.length; i++) {
        cur = annualRecords[i].Year.toString();
        netIncome = annualRecords[i]['Net Income'];
        equity = annualRecords[i]['Total Equity'];

        if (!equity) {
            continue;
        }

        roe.annual[cur] = (netIncome * 100)/equity;

        if (i == annualRecords.length-1) {
            roe.currentAnnualROE = roe.annual[cur];
        }
    }

    // Quarterly ROE
    for (i = 0; i < quarterlyRecords.length; i++) {
        cur = quarterlyRecords[i].Quarter;
        netIncome = quarterlyRecords[i]['Net Income'];
        equity = quarterlyRecords[i]['Total Equity'];

        if (!equity) {
            continue;
        }

        roe.quarterly[cur] = (netIncome * 100)/equity;

        if (i == quarterlyRecords.length-1) {
            roe.currentQuarterROE = roe.quarterly[cur];
        }
    }

    return roe;
}

if (dump) {
    when.resolve(null)
        .then(function() {
            stocks.forEachStock(docClient, delay, function(stockInfo, isFinal) {
                return when.promise(function (resolve, reject) {
                    when.resolve(null)
                        .then(function() {
                            return finanicals.getEPS(docClient, stockInfo.Symbol);
                        })
                        .then(function(eps) {
                            logger.info(JSON.stringify(eps, null, 2));
                            resolve(false); // true means stop scanning the table
                            return true;
                        })
                        .catch(function() {
                            logger.info('Skipping ' + stockInfo.Symbol);
                            resolve(false); // true means stop scanning the table
                            return false;
                        });
                });
            });
        });
} else {
    when.resolve(null)
        .then(function() { return finanicals.initFinancialTables(db); })
        .then(function() {
            stocks.forEachStock(docClient, delay, function(stockInfo, isFinal) {
                return when.promise(function (resolve, reject) {
                    try {
                        logger.info('Processing ' + stockInfo.Symbol);

                        var annualRecords = [];
                        var quarterlyRecords = [];

                        when.resolve(null)
                            .then(function() {
                                // Query all the annual records for this stock
                                return finanicals.getFinancialRecords(docClient, stockInfo.Symbol, false);
                            })
                            .then(function(records) {
                                if (!records || records.length == 0) {
                                    // Empty records, skip
                                    return when.reject();
                                }
                                annualRecords = records;

                                // Query all the quarterly records for this stock
                                return finanicals.getFinancialRecords(docClient, stockInfo.Symbol, true);
                            })
                            .then(function(records) {
                                if (!records || records.length == 0) {
                                    // Empty records, skip
                                    return when.reject();
                                }
                                quarterlyRecords = records;

                                // Sort financial records
                                annualRecords = annualRecords.sort(function(r1, r2) {
                                    return r1.Year - r2.Year;
                                });

                                quarterlyRecords = quarterlyRecords.sort(function(r1, r2) {
                                    return compareQuarters(r1.Quarter, r2.Quarter);
                                });

                                // Calculate EPS growth data
                                var epsGrowth = calculateEPSGrowth(annualRecords, quarterlyRecords);

                                // Calculate ROE
                                var roe = calculateROE(annualRecords, quarterlyRecords);

                                // Update EPS table
                                return finanicals.updateEPS(docClient, stockInfo.Symbol, epsGrowth, roe);
                            })
                            .then(function() {
                                resolve(false); // true means stop scanning the table
                                return true;
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
}

