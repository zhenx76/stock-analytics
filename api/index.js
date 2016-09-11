var logger = require('../utility').logger;
var express = require('express');
var stockFinancial = require('./stock-datatable');

var router = express.Router();

module.exports = {
    init: function(app) {
        logger.info('Starting API v1');

        router.route('/stock-financial')
            .post(function(req, res) {
                stockFinancial(req, res);
            });

        app.use('/api/v1', router);
    }
};