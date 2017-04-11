var config = require('../config');
var logger = require('../utility').logger;

var AWS = require('aws-sdk');

//
// Get a handle of Dynamodb
//
var db, docClient;
if (config.local) {
    db = new AWS.DynamoDB({
        endpoint: new AWS.Endpoint('http://localhost:8000'),
        region: 'us-west-1'
    });
    docClient = new AWS.DynamoDB.DocumentClient({
        endpoint: new AWS.Endpoint('http://localhost:8000'),
        region: 'us-west-1'
    });
} else {
    db = new AWS.DynamoDB({region: 'us-west-1'});
    docClient = new AWS.DynamoDB.DocumentClient();
}

var attributes = [
    {AttributeName: "User", AttributeType: "S"},
    {AttributeName: "Symbol", AttributeType: "S"}
];

var params = {
    TableName: 'stocks-portfolio',
    AttributeDefinitions: attributes,
    GlobalSecondaryIndexUpdates: [
        {
            Delete: {
                IndexName: 'stocks-portfolio-symbol-index'
            }
        }
    ]
};

db.updateTable(params, function(err, data) {
    if (err) {
        logger.error("Unable to update table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
        logger.info("Update table. Table description JSON:", JSON.stringify(data, null, 2));
    }
});
