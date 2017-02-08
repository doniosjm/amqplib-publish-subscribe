# amqplib-pubsub
Simplified API for Pub/Sub with amqplib (e.g. RabbitMQ)

## Dependencies

https://www.npmjs.com/package/amqplib

## Example

```js
var pubSub = new PubSub(options);
 
// connect
pubSub.connect('My Pub/Sub', function(err, pubSub){
        
        // Asserts a temporary, exclusive queue and creates a binding to receive fanout exchange messages
        pubSub.subscribeToFanout({}, 'amqp.fanout', '', function(msg, cb){
            // do work
            cb(true); // ack the message
        });
         
    });
 
// publish
var count = 1;
setInterval(function () {
    // publish a message to a fanout exchange
    pubSub.publishToFanout({}, 'amqp.fanout', 'event.message', 'my message body');
    count++;
}, 4000);

```

## Options

### `name`

If specified, this will name the connection and used in console output when debug is enabled

### `debug`

Enable/disable console output

### `debugPrefix`

Used to identify console output when debug is enabled
 
 ### `endpoint`
 
AMQP server endpoint

## MIT Licenced