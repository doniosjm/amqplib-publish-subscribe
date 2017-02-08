// http://www.squaremobius.net/amqp.node/channel_api.html
var amqp = require('amqplib');

/**
 * ctor
 * @param options
 * {
 *      "name": "default (will be overriden by the name attribute on start())"
 *      "debug": false,
 *      "debugPrefix": "PubSub",
 *      "endpoint": "amqp://localhost:5672"
 * }
 * @constructor
 */
var PubSub = function (options) {
    options = typeof options === 'undefined' ? {} : options;

    // Defaults
    this.name = 'default';
    this.debug = false;
    this.debugPrefix = 'Conn';
    this.endpoint = 'amqp://localhost:5672';

    // Override defaults
    if (Object.keys(options).length) {
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                this[i] = options[i];
            }
        }
    }

    /**
     * Logs a message to the console
     * // TODO: support a logger such as Winston
     * @param msg
     */
    this.logDebug = function(msg){
        var _this = this;
        if(this.debug) console.log('['+ _this.debugPrefix + ' `' + _this.name + '`] ' + msg);
    };

    /**
     * Logs an error to the console
     * // TODO: support a logger such as Winston
     * @param err
     */
    this.logError = function(err){
        console.log(err);
    };

    /**
     * Sends a message to an exchange
     * @param exchangeName
     * @param options
     *  {
    *      exchangeType: 'fanout',
    *      name: 'amq.fanout',
    *      assert: {
    *          exclusive: false
    *      },
    *      send: {
    *          persistent: true
    *      }
    *  }
     * @param message
     */
    this.publishToExchange = function(options, exchangeName, routingKey, message){
        var _this = this;
        var options = typeof options === 'undefined' ? {} : options;
        exchangeName = typeof options === 'undefined' ? 'amq.fanout' : exchangeName; // by default, publish message to the `amq.fanout` exchange
        routingKey = typeof options === 'undefined' ? '' : routingKey; // by default, publish all routing keys of the exchange to the queue

        // Defaults
        var opts = {
            "exchangeType": "fanout",
            "assert": {
                "exclusive": false
            },
            "send": {
                "persistent": true
            }
        }; // message survives broker restarts

        // Override defaults
        if (Object.keys(options).length) {
            for (var i in options) {
                if (options.hasOwnProperty(i)) {
                    opts[i] = options[i];
                }
            }
        }

        // create a channel on the connection
        _this.conn.createChannel()
            .then(function (ch) {
                ch.assertExchange(exchangeName, opts.exchangeType, opts.assert).then(
                    function (ok) {
                        if (ok) {
                            _this.logDebug('Publish message to ' + opts.exchangeType + ' exchange `' + exchangeName + '`');
                            ch.publish(exchangeName, routingKey, new Buffer(JSON.stringify(message)), options.send);
                        }
                        else {
                            _this.logDebug('Could not assert ' + opts.exchangeType + ' exchange `' + exchangeName + '`');
                        }
                        return ch.close();
                    }
                );
            })
            .catch(function (err) {
                _this.logError(err);
            });
    };
};

/**
 * Connects to an AMQP server.
 * Once connected, the callback returns and other functions of this object can operate on the connection.
 * on error: error is passed to the callback
 * on close: by default, automatically reconnects after 5 seconds
 * @param cb
 */
PubSub.prototype.connect = function(name, cb) {
    var _this = this;
    _this.name = name;

    // change prefix to include name
    this.logDebug(_this.endpoint);

    // connect to amqp
    amqp.connect(_this.endpoint)
        .then(function (conn) {

            // on connection error, log to the console and then cb
            conn.on("error", function (err) {
                if (err.message !== "Connection closing")
                    _this.logError(_this.debugPrefix + "conn error " + err.message);
                else
                    _this.logError(_this.debugPrefix + err.message);
                return cb(err);
            });

            // on connection close, log to the console and reconnect unless otherwise specified
            conn.on("close", function (reconnect) {
                _this.logDebug("conn closed");
                if(reconnect === undefined || reconnect === true) {
                    _this.logDebug("reconnecting");
                    return setTimeout(_this.start(_this.name, cb), 5000);
                }
            });

            _this.logDebug("connected");
            _this.conn = conn;
            cb(null);
        })
        .catch(function(err){
            if(_this.conn) _this.conn.close(false);
            cb(err);
        })
};

/**
 * Publish a message to a fanout exchange
 * @param exchangeName
 * @param options
 *  {
 *      name: 'amq.fanout',
 *      assert: {
 *          exclusive: false
 *      },
 *      send: {
 *          persistent: true
 *      }
 *  }
 * @param message
 */
PubSub.prototype.publishToFanout = function(options, exchangeName, routingKey, message) {
    var _this = this;
    var options = typeof options === 'undefined' ? {} : options;
    var opts = {};

    // Override defaults
    if (Object.keys(options).length) {
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                opts[i] = options[i];
            }
        }
    }

    // this should not be changed by options
    opts.exchangeType = 'fanout';

    // publish
    this.publishToExchange(opts, exchangeName, routingKey, message);
};

/**
 * Publish a message to a topic exchange
 * @param exchangeName
 * @param options
 *  {
 *      name: 'amq.topic',
 *      assert: {
 *          exclusive: false
 *      },
 *      send: {
 *          persistent: true
 *      }
 *  }
 * @param message
 */
PubSub.prototype.publishToTopic = function(options, exchangeName, routingKey, message) {
    var _this = this;
    var options = typeof options === 'undefined' ? {} : options;
    var opts = {};

    // Override defaults
    if (Object.keys(options).length) {
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                opts[i] = options[i];
            }
        }
    }

    // this should not be changed by options
    opts.exchangeType = 'topic';

    // publish
    this.publishToExchange(opts, exchangeName, routingKey, message);
};

/**
 * By default, asserts a temporary, exclusive queue and creates a binding to receive fanout exchange messages
 * @param options
 * {
 *      name: '',
 *      prefetch: 1,
 *      assert: {
 *          exclusive: true
 *      }
 * }
 * @param exchangeName
 */
PubSub.prototype.subscribeToFanout = function(options, exchangeName, routingKey, cb) {
    var _this = this;
    var options = typeof options === 'undefined' ? {} : options;
    routingKey = typeof routingKey === 'undefined' ? '' : routingKey; // by default, publish all routing keys of the exchange to the queue

    // Defaults
    var opts = {
        "prefetch": 1,
        "name": '', // temporary queue
        "assert": {
            "exclusive": true
        }
    };  // exclusive queue

    // Override defaults
    if (Object.keys(options).length) {
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                opts[i] = options[i];
            }
        }
    }

    // create a channel on the connection
    _this.conn.createChannel()
        .then(function (ch) {
            ch.prefetch(opts.prefetch);
            ch.assertQueue(opts.name, options.assert)
                .then(function(result){
                    if(result){
                        // bind the queue to the exchange
                        ch.bindQueue(result.queue, exchangeName, routingKey)
                            .then(function(bindOk){
                                if(bindOk) {
                                    _this.logDebug('Created queue and bound to fanout exchange `' + exchangeName + '`');
                                    // consume queue message
                                    ch.consume(result.queue, function (msg) {
                                        // do work on the message and acknowledge or reject
                                        cb(msg, function(ok){
                                            if (ok)
                                                ch.ack(msg);
                                            else
                                                ch.reject(msg, true);
                                        });
                                    });
                                }
                            })
                            .catch(function(err){
                                _this.logError(err);
                                return ch.close();
                            });
                    }
                })
                .catch(function(err){
                    _this.logError(err);
                    return ch.close();
                });
        })
        .catch(function (err){
            _this.logError(err);
        });
};

/**
 * Subscribe to receive messages from a queue
 * @param queueName
 * @param options
 *  {
 *      prefetch: 1,
 *      assert: {
 *          exclusive: false
 *      }
 *  }
 * @param cb expects 1 argument (a boolean) - true: acknowledge the message to the server; false: reject the message to the server
 */
PubSub.prototype.subscribeToQueue = function(options, queueName, cb) {
    var _this = this;
    var options = typeof options === 'undefined' ? {} : options;
    routingKey = typeof routingKey === 'undefined' ? '' : routingKey; // by default, publish all routing keys of the exchange to the queue

    // Defaults
    var opts = {
        "prefetch": 1,
        "assert": {
            "exclusive": false
        }
    };  // let other consumers receive messages from this queue

    // Override defaults
    if (Object.keys(options).length) {
        for (var i in options) {
            if (options.hasOwnProperty(i)) {
                opts[i] = options[i];
            }
        }
    }

    // create a channel on the connection
    _this.conn.createChannel()
        .then(function (ch) {
            ch.prefetch(opts.prefetch);
            ch.assertQueue(queueName, opts.assert)
                .then(function(result){
                    if(result){
                        _this.logDebug('Consume messages from queue `' + queueName + '`');
                        ch.consume(queueName, function (msg) {
                            // do work on the message and acknowledge or reject
                            cb(msg, function(ok){
                                if (ok)
                                    ch.ack(msg);
                                else
                                    ch.reject(msg, true);
                            });
                        });
                    }
                })
                .catch(function(err) {
                    _this.logError(err);
                });
        })
        .catch(function (err){
            _this.logError(err);
        });
};

module.exports = PubSub;