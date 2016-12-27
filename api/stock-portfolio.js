var config = require('../config');
var decodeUser = require('./auth').decodeUser;
var query = require('./dynamodb-query');
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
            var symbol = req.body.symbol || '';
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
            query.runSymbolList(user.watch_list)
                .then(function(data) {
                    logger.info('Queried EPS data for ' + user.watch_list);
                    res.json(data);
                })
                .catch(function(error) {
                    logger.error(error);
                    res.status(400).send({success: false, msg: error});
                })
        }
    });
};