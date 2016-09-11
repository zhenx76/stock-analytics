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

module.exports = {
    run: function(filters) {
        return finanicals.scanEPS(docClient, filters);
    }
};

