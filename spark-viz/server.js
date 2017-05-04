var app = require('./app');
var server = app.listen(3000, function () {
    console.log('Spark Viz now listening on port 3000');
});

var io = require('socket.io')(server);

var redis = require("redis");

var redisClient = redis.createClient(6379, 'localhost', {});
redisClient.on('error', function (err) {
    console.log(err);
});
redisClient.on('connect', function () {
    console.log('Redis client connected');
});
redisClient.subscribe('detail-metrics',
    'summary-metrics',
    'raw-messages',
    'stream-transitions');

redisClient.on('message', function (channel, msg) {
    io.sockets.emit(channel, msg);
});

io.on('connection', function (socket) {
    console.log('Socket io client ' + socket.id + ' connected');
});

setInterval(function () {
    io.sockets.emit('time', new Date());
}, 1000);