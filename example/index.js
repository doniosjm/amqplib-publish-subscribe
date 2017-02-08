const amqp = require("amqplib");
const amqpTopology = require("amqplib-topology");
const PubSub = require("../pubsub.js");
const amqpEndpoint = "amqp://localhost:5672";

/**
 * Create the topology required for the example
 */
var createTopology = function(endpoint, cb) {
    var ok = amqp.connect(endpoint);
    ok = ok
        .then(function (connection) {
            return amqpTopology(connection)
                .assert({
                    exchanges: {
                        "sensor.temperature": {
                            "type": "topic",
                            "queues": {
                                "inbound": {
                                    "name": "temperature.data.save",
                                    "patterns": ["temperature.#"],
                                    "durable": true,
                                    "prefetch": 50
                                }
                            },
                        },
                        "events.temperature": {
                            "type": "fanout",
                            "subscriptions": {
                                "sensor.temperature": {
                                    type: "fanout",
                                    patterns: ["temperature.#"]
                                }
                            }
                        },
                        "events": {
                            "type": "fanout"
                        },
                    },

                });
        })
        .then(function (topology) {
            topology["endpoint"] = endpoint;
            cb(null, topology);
        })
        .catch(function (err) {
            cb(err);
        });
};

// create the topology
createTopology(amqpEndpoint, function(err, topology){
    if(err) return console.error(err);

    console.log("Topology created");

    var publishFrequency = 4000; // 4s
    var options = {
        "debug": true,
        "debugPrefix": "Conn",
        "endpoint": topology.endpoint
    };

    // connect to server
    var pubSub = new PubSub(options);

    // connect & subscribe
    pubSub.connect('My Pub/Sub', function(err){
        if(err) return console.log(err);

        // Asserts a temporary, exclusive queue and creates a binding to receive fanout exchange messages
        pubSub.subscribeToFanout({}, 'events', '', function(msg, cb){
            console.log('Message received from `events` fanout exchange: ', JSON.parse(msg.content.toString()));
            // do work
            cb(true); // ack the message
        });

        // Subscribe to receive messages from a queue
        pubSub.subscribeToQueue({}, 'temperature.data.save', function(msg, cb){
            console.log('Message received from `temperature.data.save` queue: ', JSON.parse(msg.content.toString()));
            // do work
            cb(true); // ack the message
        });
    });


    // publish
    var count = 1;
    setInterval(function () {

        // publish a message to a fanout exchange
        pubSub.publishToFanout({}, 'events', 'event.message', JSON.stringify({to: "events exchange", routingKey: "event.message", count: count}));

        // publish a message to a topic exchange
        pubSub.publishToTopic({}, 'sensor.temperature', 'temperature.new', '' + JSON.stringify({to: "sensor.temperature exchange", routingKey: "temperature.#", count:count }));

        count++;
    }, publishFrequency);

});