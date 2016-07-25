//
// Scrape the stock financial information from web
//
// The stock data is from marketwatch.com
// We scrape both yearly and quaterly data
// Data is stored into dynamodb

var logger = require('./utility').logger;
var when = require('when');
var request = require("request");
var cheerio = require("cheerio");

function parseNumber(str) {
    try {
        if (str == '-') {
            return 0;
        }

        var multiplier = 1;

        // Check if number is negative
        var result = str.match(/\(([^)]+)?\)/);
        if (result != null) {
            // Found a matching '()', which means it's negative
            multiplier = -1;
            str = result[1];
        }

        if (!str) {
            return 0;
        }

        // Check the multiplier
        var mul = str[str.length-1];
        if (mul == 'B') {
            multiplier *= 1000000000;
        } else if (mul == 'M') {
            multiplier *= 1000000;
        } else if (mul == 'K') {
            multiplier *= 1000;
        }
        if (multiplier != 1 && multiplier != -1) {
            str = str.slice(0, -1);
        }

        return parseFloat(str) * multiplier;

    } catch (exception) {
        logger.warn('Invalid number');
        return null;
    }
}

function parse(symbol, html, isQuarter) {
    try {
        var records = [];
        var $ = cheerio.load(html);

        // Get the time line: year or quarter
        var cols = $('.topRow').children();
        for (var i = 1; i <= 5; i++) {
            if (isQuarter) {
                records.push({
                    'Symbol' : symbol,
                    'Quarter': $(cols[i]).text()});
            } else {
                records.push({
                    'Symbol' : symbol,
                    'Year': parseInt($(cols[i]).text())});
            }
        }

        // Get the financial information from "<td class="partialSum">
        $('.partialSum').each(function() {
            var key = $(this).children('.rowTitle').text().trim();
            if (key == 'Sales/Revenue') {
                key = 'Revenue';
            }

            if (key == 'Revenue' || key == 'Gross Income') {
                $(this).children('.valueCell').each(function(i, value) {
                    records[i][key] = parseNumber($(value).text());
                });
            }
        });

        // Get the financial information "<td class="totalRow">
        $('.totalRow').each(function() {
            var key = $(this).children('.rowTitle').text().trim();
            if (key == 'Net Income') {
                $(this).children('.valueCell').each(function(i, value) {
                    records[i][key] = parseNumber($(value).text());
                });
            }
        });

        // Get the financial information "<td class="mainRow">
        $('.mainRow').each(function() {
            var key = $(this).children('.rowTitle').text().trim();

            if (key == 'Cost of Goods Sold (COGS) incl. D&A') {
                key = 'COGS';
            }

            if (key == 'COGS' ||
                key == 'EPS (Basic)' ||
                key == 'EPS (Diluted)' ||
                key == 'Basic Shares Outstanding' ||
                key == 'Diluted Shares Outstanding') {
                $(this).children('.valueCell').each(function(i, value) {
                    records[i][key] = parseNumber($(value).text());
                });
            }
        });

        // Sanity check the records because new stocks won't have
        // all five-year or five-quarter records
        var startIndex = 0;
        while (startIndex < 5) {
            if (isQuarter) {
                if (records[startIndex].Quarter != null && records[startIndex].Quarter.length > 0) {
                    break;
                }
            } else {
                if (!isNaN(records[startIndex].Year) && records[startIndex].Year >= 2010) {
                    break;
                }
            }
            startIndex++;
        }

        return records.splice(startIndex);

    } catch (exception) {
        logger.warn("Cannot parse the stock page for " + symbol);
        logger.warn(exception);
        return null;
    }
}

exports.scrape = function(symbol, isQuarter) {
    return when.promise(function(resolve, reject) {
        try {
            var urlPrefix = "http://www.marketwatch.com/investing/stock";
            var urlSuffixAnnual = "financials";
            var urlSuffixQuarter = "financials/income/quarter";
            var url = [urlPrefix, symbol.toLowerCase(), (isQuarter ? urlSuffixQuarter: urlSuffixAnnual)].join('/');

            request(url, function(error, response, html) {
                if (error) {
                    logger.error(error.message);
                    reject(symbol);
                } else {
                    var records = parse(symbol, html, isQuarter);
                    resolve(records);
                }
            });

        } catch (exception) {
            logger.warn(exception);
            reject(symbol);
        }
    }).timeout(120000); //timeout after 2 minutes
};

//exports.scrape('AVGO', true).then(function(records) {logger.info(JSON.stringify(records))});
