var logger = require('../utility').logger;
var express = require('express');
var passport = require('passport');
var auth = require('./auth');
var stockFinancial = require('./stock-datatable');
var stockPortfolio = require('./stock-portfolio');

var router = express.Router();

module.exports = {
    init: function(app) {
        logger.info('Initializing API v1');

        // Use the passport package in our application
        app.use(passport.initialize());
        auth.config(passport);

        router.route('/stock-financial')
            .post(function(req, res) {
                stockFinancial.query(req, res);
            });

        router.route('/stock/:symbol')
            .get(function(req, res) {
                stockFinancial.getStock(req, res);
            });

        router.route('/signup')
            .post(function(req, res) {
                auth.signup(req, res);
            });

        router.route('/authenticate')
            .post(function(req, res) {
                auth.authenticate(req, res);
            });

        router.route('/memberinfo')
            .get(passport.authenticate('jwt', {session: false}), function(req, res) {
                auth.getUserProfile(req, res);
            });

        router.route('/portfolio/watchlist')
            .get(passport.authenticate('jwt', {session: false}), function(req, res) {
                stockPortfolio.queryWatchList(req, res);
            })
            .post(passport.authenticate('jwt', {session: false}), function(req, res) {
                stockPortfolio.addWatchList(req, res);
            })
            .delete(passport.authenticate('jwt', {session: false}), function(req, res) {
                stockPortfolio.removeWatchList(req, res);
            });

        app.use('/api/v1', router);
    }
};
