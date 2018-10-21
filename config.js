module.exports = {
    secret: 'CANSLIM',
    alphaVantageKey: '3WBF9M8MO5DISZUE',
    mqttBrokerURL: 'mqtt://localhost:1883',
    mqttTopicSymbol: 'stock/symbols',
    mqttTopicQutoes: 'stock/quotes',
    local: false // Before deploy to AWS, change local to false
};