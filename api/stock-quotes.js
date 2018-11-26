var config = require('../config');
var WebSocket = require('ws');
var WebSocketServer = WebSocket.Server;
var uuid = require('node-uuid');
var mqtt = require('mqtt');
var logger = require('../utility').logger;

var mqttClient;
var TOPIC_SYMBOL = config.mqttTopicSymbol;
var TOPIC_QUOTE = config.mqttTopicQutoes;

var clients = [];

function registerClient(ws) {
    var client = {
        id: uuid.v4(),
        ws: ws,
        symbols: []
    };
    clients.push(client);
    logger.info('StockQuoteServer: client connected:' + client.id);
    return client;
}

function deregisterClient(client) {
    var index = clients.indexOf(client);
    if (index > -1) {
        logger.info('StockQuoteServer: client disconnected:' + client.id);
        clients.splice(index, 1);
    }
}

function processClientMessage(client, message) {
    try {
        var params, action, symbols;

        params = JSON.parse(message);
        if (params.hasOwnProperty('action')) {
            action = params['action'].toUpperCase();
        }
        if (params.hasOwnProperty('symbols')) {
            symbols = params['symbols'];
        }

        // Validate command parameters
        if (action != 'ADD' && action != 'DELETE') {
            throw new Error('Invalid action parameter');
        }
        if (!Array.isArray(symbols) || !symbols.length) {
            throw new Error('Invalid symbols parameter');
        }

        logger.info(action + ' symbols: ' + symbols.toString());

        // Update symbols for client
        var newSymbols = [];
        for (var i = 0; i < symbols.length; i++) {
            var symbol = symbols[i];

            if (!/^[a-z]+$/i.test(symbol)) {
                //
                // Alpha Vantage API limitation: the symbol has to be all letters
                //
                logger.info('StockQuoteServer: Ignore symbol that contains non alphabetic: ' + symbol);
                continue;
            }

            if (action == 'ADD') {
                if (client.symbols.indexOf(symbol) == -1) {
                    client.symbols.push(symbol);
                    newSymbols.push(symbol);
                }
            } else if (action == 'DELETE') {
                var index = client.symbols.indexOf(symbol);
                if (index > -1) {
                    client.symbols.splice(index, 1);
                }
            }
        }

        // Subscribe new symbols
        if (action == 'ADD' && newSymbols.length) {
            mqttClient.publish(TOPIC_SYMBOL, JSON.stringify({
                action: 'ADD',
                symbols: newSymbols
            }));
        }

    } catch (err) {
        logger.error('StockQuoteServer: invalid parameters ' + err.message);
    }
}

// This should work in node.js and other ES5 compliant implementations.
function isEmptyObject(obj) {
    return !Object.keys(obj).length;
}

function processQuoteMessage(message) {
    try {
        var snapshot = JSON.parse(message.toString());

        for (var i = 0; i < clients.length; i++) {
            var client = clients[i];
            var quotes = {};

            if (client.ws.readyState != WebSocket.OPEN) {
                // Websocket connection may be closed by client
                continue;
            }

            for (var symbol in snapshot) {
                if (snapshot.hasOwnProperty(symbol)) {
                    if (client.symbols.indexOf(symbol) > -1) {
                        quotes[symbol] = snapshot[symbol];
                    }
                }
            }

            // send stock quotes to client
            if (!isEmptyObject(quotes)) {
                logger.info('StockQuoteServer: send stock quotes to client ' + client.id);
                client.ws.send(JSON.stringify(quotes));
            }
        }

    } catch (err) {
        logger.error('StockQuoteServer: invalid quote message ' + err.message);
    }
}

exports.startQuoteServer = function (server) {
    mqttClient = mqtt.connect(config.mqttBrokerURL);

    mqttClient.on('connect', function () {
        logger.info('StockQuoteServer: connected to ' + config.mqttBrokerURL);

        // Start listening to stock qutoes
        logger.info('StockQuoteServer: subscribing to topic: ' + TOPIC_QUOTE);
        mqttClient.subscribe(TOPIC_QUOTE);

        // Start WebSocket server
        logger.info('StockQuoteServer: start websocket server');
        var wss = new WebSocketServer({server: server});

        wss.on('connection', function (ws) {
            var client = registerClient(ws);

            ws.on('message', function (message) {
                processClientMessage(client, message);
            });

            ws.on('close', function () {
                deregisterClient(client);
            });

            ws.send(JSON.stringify({id: client.id}));
        });
    });

    mqttClient.on('message', function (topic, message) {
        if (topic == TOPIC_QUOTE) {
            processQuoteMessage(message);
        }
    });
};

exports.stopQuoteServer = function () {
    try {
        logger.info('StockQuoteServer: disconnecting all clients.');
        for (var i = 0; i < clients.length; i++) {
            clients[i].ws.close();
        }

        logger.info('StockQuoteServer: disconnecting from ' + config.mqttBrokerURL);
        mqttClient.end();

    } catch (err) {
        logger.error('StockQuoteServer: error when stopping server ' + err.message);
    }

};