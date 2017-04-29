var when = require('when');
var config = require('../config');
var decodeUser = require('./auth').decodeUser;
var query = require('./dynamodb-query');
var priceAgent = require('../price_agent');
var logger = require('../utility').logger;

exports.addWatchList = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else {
            var symbol = req.body.symbol || '';
            if (!!symbol) {
                user.addToWatchList(symbol).then(function() {
                    res.json({success: true});
                });
            } else {
                res.status(400).send({success: false, msg: 'Invalid symbol'});
            }
        }
    });
};

exports.removeWatchList = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else {
            var symbol = req.query.symbol || '';
            if (!!symbol) {
                user.removeFromWatchList(symbol).then(function() {
                    res.json({success: true});
                });
            } else {
                res.status(400).send({success: false, msg: 'Invalid symbol'});
            }
        }
    });
};

exports.queryWatchList = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else if (!user.watch_list || user.watch_list.length == 0) {
            res.json([]); // We have to return an array to make Angular JS datatable happy
        } else {
            when.all([
                // promise 0: EPS data
                query.runSymbolList(user.watch_list),

                // promise 1: price snapshot
                priceAgent.getPriceSnapshot(user.watch_list)

            ]).then(function (values) {
                logger.info('Queried EPS data for ' + user.watch_list);

                var results = values[0];
                var snapshot = values[1];

                for (var i = 0; i < results.length; i++) {
                    if (snapshot.hasOwnProperty(results[i].Symbol)) {
                        results[i].Snapshot = snapshot[results[i].Symbol];
                    }
                }

                res.json(results);
            }).catch(function(error) {
                logger.error(error);
                res.status(400).send({success: false, msg: JSON.stringify(error, null, 2)});
            });
        }
    });
};

exports.getStock = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else {
            var symbol = req.params.symbol || '';
            if (!!symbol) {
                symbol = symbol.toUpperCase();
                query.getUserStockData(user.username, symbol)
                    .then(function(data) {
                        res.json(data);
                    })
                    .catch(function (err) {
                        logger.error(err);
                        res.status(500).send({success: false, msg: "Symbol " + symbol + " doesn't exist or error retrieving"});
                    });
            } else {
                res.status(400).send({success: false, msg: 'Invalid symbol'});
            }
        }
    });
};

exports.updateStockPosition = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else {
            var symbol = req.body.symbol || '';
            var price = parseFloat(req.body.price);
            var shares = parseFloat(req.body.shares);
            var datetime = new Date(req.body.datetime || '');
            var action = req.body.action || '';
            var errMsg = '';

            // Validate parameters
            if (!symbol) {
                errMsg = 'Invalid symbol';
            } else if (isNaN(price) || price <= 0) {
                errMsg = 'Invalid price';
            } else if (isNaN(shares) || shares <= 0) {
                errMsg = 'Invalid shares';
            } else if (action != 'BUY' && action != 'SELL') {
                errMsg = 'Invalid action. Must be either BUY or SELL';
            } else if (isNaN(Date.parse(datetime))) {
                datetime = new Date();
            }

            if (!!errMsg) {
                res.status(400).send({success: false, msg: errMsg});
            } else {
                query.updateUserStockPosition(user.username, symbol, price, shares, datetime, action)
                    .then(function(data) {
                        res.json({success: true, data: data});
                    })
                    .catch(function(error) {
                        res.status(400).send({success: false, msg: error.message});
                    });
            }
        }
    });
};

exports.getUserPositions = function(req, res) {
    decodeUser(req, function(err, user) {
        if (err) {
            res.status(403).send({success: false, msg: err.message});
        } else {
            var records;
            query.getUserPositions(user.username)
                .then(function(data) {
                    records = data;

                    var symbols = [];
                    for (var i = 0; i < data.length; i++) {
                        symbols.push(data[i].symbol);
                    }

                    return priceAgent.getPriceSnapshot(symbols);
                })
                .then(function(snapshot) {
                    for (var i = 0; i < records.length; i++) {
                        if (snapshot.hasOwnProperty(records[i].symbol)) {
                            records[i].snapshot = snapshot[records[i].symbol];
                        }
                    }
                    res.json(records);
                })
                .catch(function(error) {
                    logger.error(JSON.stringify(error, null, 2));
                    res.status(400).send({success: false, msg: JSON.stringify(error, null, 2)});
                })
        }
    });
};