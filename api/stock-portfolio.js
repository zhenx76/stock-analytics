var config = require('../config');
var decodeUser = require('./auth').decodeUser;

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