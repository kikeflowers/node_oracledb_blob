var domain = require('domain');
var http = require('http');
var express = require('express');
var morgan = require('morgan');
var serveStatic = require('serve-static');
var dbconfig = require('./server/dbconfig.js');
var database = require('./server/database.js');
var api = require('./server/api.js');
var serverDomain = domain.create();
var openHttpConnections = {};
var app;
var httpServer;
var Buffer = require('buffer');



serverDomain.on('error', function(err) {
    console.error('Domain error caught', err);

    shutdown();
});

serverDomain.run(initApp);

function initApp() {



    app = express();
    httpServer = http.Server(app);

    app.use(morgan('combined')); //logger

    app.use('/', serveStatic(__dirname + '/public'));
  
    app.use('/vendor', serveStatic(__dirname + '/bower_components'));

    app.use('/api', api.getRouter());

    httpServer.on('connection', function(conn) {
        var key = conn.remoteAddress + ':' + (conn.remotePort || '');

        openHttpConnections[key] = conn;

        conn.on('close', function() {
            delete openHttpConnections[key];
        });
    });

    database.connect(dbconfig, function() {
        app.listen(3000, function() {
            console.log('Webserver listening on localhost:3000');
        });
    });
}

function shutdown() {
    console.log('Shutting down');
    console.log('Closing web server');

    httpServer.close(function () {
        console.log('Web server closed');

        database.disconnect(function() {
            console.log('Exiting process');
            process.exit(0);
        });
    });

    for (key in openHttpConnections) {
        openHttpConnections[key].destroy();
    }
}