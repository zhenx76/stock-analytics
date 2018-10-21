//
// AngularJS datatables data source
// NOTE: here I decided NOT to use server side processing because:
// 1. Most of the time, user will apply stock filters which will only return a few stocks.
//    Server side processing is overkill for only a few stocks.
// 2. The total number of records will be ~3140 (# of stocks listed on Nasdaq). I read somewhere
//    that it's a rule of thumb to use server side processing when # of records is more than 10,000.
// 3. Currently I don't use Hive yet. Thus each request will result in dynamodb query. If I use
//    server side processing, every ordering event by user will result in a dynamodb query which is
//    too much for the provisioned throughput.
//

//
// Directly query dynamodb for now. In the future, will move to Amazon EMR (Hadoop + Hive)
// when it starts to make sense cost wise.
//
var query = require('./dynamodb-query');
var logger = require('../utility').logger;

function getStockFilters(params) {
    var filters = {};

    if (params.hasOwnProperty('currentQuarterEPSGrowth')) {
        filters.currentQuarterEPSGrowth = parseFloat(params.currentQuarterEPSGrowth);
    } else {
        filters.currentQuarterEPSGrowth = 25.00;
    }

    if (params.hasOwnProperty('lastQuarterEPSGrowth')) {
        filters.lastQuarterEPSGrowth = parseFloat(params.lastQuarterEPSGrowth);
    } else {
        filters.lastQuarterEPSGrowth = 20.00;
    }

    if (params.hasOwnProperty('currentAnnualEPSGrowth')) {
        filters.currentAnnualEPSGrowth = parseFloat(params.currentAnnualEPSGrowth);
    } else {
        filters.currentAnnualEPSGrowth = 20.00;
    }

    if (params.hasOwnProperty('lastAnnualEPSGrowth')) {
        filters.lastAnnualEPSGrowth = parseFloat(params.lastAnnualEPSGrowth);
    } else {
        filters.lastAnnualEPSGrowth = 15.00;
    }

    if (params.hasOwnProperty('previousAnnualEPSGrowth')) {
        filters.previousAnnualEPSGrowth = parseFloat(params.previousAnnualEPSGrowth);
    } else {
        filters.previousAnnualEPSGrowth = 0;
    }

    if (params.hasOwnProperty('currentQuarterROE')) {
        filters.currentQuarterROE = parseFloat(params.currentQuarterROE);
    } else {
        filters.currentQuarterROE = 5.00;
    }

    if (params.hasOwnProperty('currentAnnualROE')) {
        filters.currentAnnualROE = parseFloat(params.currentAnnualROE);
    } else {
        filters.currentAnnualROE = 17.00;
    }

    return filters;
}

exports.query = function(req, res) {
    try {
        if (req.body.hasOwnProperty('symbol') && !!(req.body.symbol)) {
            var symbol = req.body.symbol;
            query.runSymbol(symbol)
                .then(function (record) {
                    res.json([{
                        PreviousQuarterGrowth: record.PreviousQuarterGrowth || 'N/A',
                        CurrentAnnualROE: record.CurrentAnnualROE || 'N/A',
                        CurrentAnnualGrowth: record.CurrentAnnualGrowth || 'N/A',
                        CurrentQuarterGrowth: record.CurrentQuarterGrowth || 'N/A',
                        Symbol: record.Symbol
                    }]);
                })
                .catch(function () {
                    res.status(404).send('Cannot find records for symbol ' + symbol);
                });
        } else {
            var filters = getStockFilters(req.body);

            query.run(filters)
                .then(function(data) {
                    if (!Array.isArray(data)) {
                        return when.reject();
                    }
                    res.json(data);
                })
                .catch(function () {
                    res.status(404).send('Error when query stocks');
                });
        }

    } catch (exception) {
        logger.warn(exception);
        res.status(500).send('Failed to process stock financial data! Error = ' + exception.message);
    }
};

exports.getStock = function(req, res) {
    try {
        var symbol = req.params.symbol.toUpperCase();

        query.getStockData(symbol).then(function(data) {
            res.json(data);
        }).catch(function() {
           res.status(500).send("Symbol " + symbol + " doesn't exist or error retrieving");
        });

    } catch (exception) {
        logger.warn(exception);
        res.status(500).send('Failed to process stock financial data! Error = ' + exception.message);
    }
};