var express = require('express');
var database = require('./database.js');
var oracledb = require('oracledb');
var http = require('http');

var async = require('async');

function getRouter() {
    var router = express.Router();

    router.route('/files')
        .get(getFiles)
        .post(postFiles);


    router.route('/file/:file_name')
        .get(getFileByName)
        .delete(delFileByName);

    router.route('/filess/:file_name')
        .get(handleRequest);

    return router;
}

module.exports.getRouter = getRouter;

function handleRequest(req, res, next) {
    var startDate = new Date();
    var moreToSend = true;
    database.getPool().getConnection(function(err, dbConnection) {
        if (err) return next(err);
        
        async.whilst(
            function () { return moreToSend;},
            function (callback) {
                

                dbConnection.execute(
                    'SELECT ' +
                    ' blob_data, content_type ' +
                    ' FROM jsao_files where file_name = :file_name',

                    { 
                        file_name: req.params.file_name,
                    },
                    function(err, results) {
                        if (err) {
                            return dbConnection.release(function() {
                                next(err);
                            });
                        }

                    var lob = results.rows[0][0];
                    var mimeType = results.rows[0][1]; 
                    
                        const doStream = new Promise((resolve, reject) => {
                            lob.on('end', () => {
                                dbConnection.release(function() {});
                                moreToSend = false;
                                res.end();
                            });
                            lob.on('close', () => {
                                dbConnection.release(function() {});
                                moreToSend = false;
                                resolve();
                            });
                            lob.on('error', (err) => {
                                dbConnection.release(function() {});
                                moreToSend = false;
                                reject(err);
                            });
                            res.writeHead(200, {'Content-Type': mimeType });
                            lob.pipe(res);  
                            
                        });
                    
                        
                    }
                );
            },
            function (err) {
                console.log('Finished sending file. Total time: ', new Date() - startDate);

                dbConnection.release(function() {});

                res.end();
            }

            
        );
    });
    

}


function getFiles(req, res, next) {
    database.getPool().getConnection(function(err, connection) {
        if (err) return next(err);

        connection.execute(
            'SELECT file_name, ' +
            '   dbms_lob.getlength(blob_data) AS file_length, ' +
            '   content_type AS file_type, ' +
            '   blob_data ' +
            'FROM jsao_files',
            {},//no binds
            {
                outFormat: oracledb.OBJECT
                //outFormat: oracledb.OUT_FORMAT_ARRAY
            },
            function(err, results) {
                if (err) {
                    return connection.release(function() {
                        next(err);
                    });
                }

                connection.release(function(err) {
                    if (err) return next(err);

                    res.send(results);
                });
            }
        );
    });
}


function postFiles(req, res, next) {
    var totalBytesReceived = 0;
    var totalBytesInBuffer = 0;
    var maxFileSize = 1024 * 1024 * 30; //30MB
    var maxBytesInBuffer = 1024 * 1024 * 1; //1MB
    var maxPieceSize = 20000;
    var chunkQueue = [];
    var chunkNumber = 0;
    var processingStream = true;
    var startDate = new Date();
    var responseSent = false;
    var dbConnection;

    database.getPool().getConnection(function(err, connection) {
        if (err) return next(err);

        dbConnection = connection;

        async.whilst(
            function () { return processingStream || chunkQueue.length; },
            function (callback) {
                var idx;
                var chunk;
                var chunkPiece;
                var chunkPieceCount;
                var start;
                var end;
                var plsql;
                var binds = [];

                if (chunkQueue.length) {
                    chunkNumber += 1;
                    chunk = chunkQueue.shift();
                    totalBytesInBuffer -= chunk.length;

                    if (totalBytesInBuffer < maxBytesInBuffer) {
                        req.resume();
                    }

                    chunkPieceCount = Math.ceil(chunk.length/maxPieceSize);

                    binds.push(req.headers['x-file-name']);
                    binds.push(req.headers['x-content-type']);
                    binds.push(chunkNumber);
                    binds.push(chunkPieceCount);

                    plsql = '' +
                        'BEGIN ' +
                        '   jsao_file_load.upsert_file_chunk( \n' +
                        '      p_file_name    => :file_name, \n' +
                        '      p_content_type => :content_type, \n' +
                        '      p_chunk_number => :chunk_number, \n' +
                        '      p_piece_count  => :piece_count, \n';

                    for (idx = 0; idx < chunkPieceCount; idx += 1) {
                        start = maxPieceSize * idx;
                        end = start + maxPieceSize;

                        chunkPiece = chunk.slice(start, end);
                        binds.push(chunkPiece.toString('base64'));
                        plsql += '      p_b64_piece_' + (idx + 1) + ' => :piece_' + (idx + 1) + ', \n';
                    }

                    plsql = plsql.replace(/\, \n$/, '') + //remove the trailing comma and new line
                        '   ); \n' +
                        'END;';

                    dbConnection.execute(
                        plsql,
                        binds,
                        function(err, results) {
                            if (err) {
                                return dbConnection.release(function() {
                                    next(err);
                                });
                            }

                            console.log('Chunk processed, chunkQueue.length: ', chunkQueue.length, totalBytesInBuffer);

                            callback();
                        }
                    );
                } else {
                    console.log('Nothing to process, waiting for more data ', new Date());
                    setTimeout(callback, 50);
                }
            },
            function (err) {
                console.log('Finished processing queue. Total time: ', new Date() - startDate);

                dbConnection.commit(function(err) {
                    if (err) {
                        console.log('Error committing file upload');

                        return dbConnection.release(function() {
                            next(err);
                        });
                    }

                    if (!responseSent) {
                        res.end('File upload complete');
                    }

                    dbConnection.release(function() {});
                });
            }
        );
    });

    req.on('data', function(chunk) {
        chunkQueue.push(chunk);
        totalBytesReceived += chunk.length;
        totalBytesInBuffer += chunk.length;

        console.log('Chunk queued, chunkQueue.length: ', chunkQueue.length, totalBytesInBuffer);

        if (totalBytesReceived > maxFileSize) {
            console.log('File size exceeded limit');

            req.pause();
            processingStream = false;
            chunkQueue = [];
            totalBytesInBuffer = 0;

            if (dbConnection) {
                dbConnection.rollback(function(err) {
                    if (err) {
                        return dbConnection.release(function() {
                            next(err);
                        });
                    }

                    dbConnection.release(function(err) {
                        next(err);
                    });
                });
            }

            res.header('Connection', 'close');
            res.status(413).end('The file size exceeded the limit of ' + maxFileSize + ' bytes');
            responseSent = true;

            req.connection.destroy();
        }

        if (totalBytesInBuffer > maxBytesInBuffer) {
            req.pause();
        }
    });

    req.connection.on('close',function(){
        processingStream = false;
        chunkQueue = [];
    });

    req.on('error', function() {
        processingStream = false;
        chunkQueue = [];
    });

    req.on('end', function() {
        processingStream = false;

        console.log('All chunks queued');
    });
}



function getFileByName(req, res, next) {
    var startDate = new Date();
    var moreToSend = true;
    var currentOffset = 1; //Oracle uses a 1 based index
    var bytesPerFetch = 400000;//Max is 400000
    var totalBytesFetched = 0;
    var piecesIdx;
    var headerWritten = false;

    database.getPool().getConnection(function(err, dbConnection) {
        if (err) return next(err);

        async.whilst(
            function () { return moreToSend;},
            function (callback) {
                var plsql;

                plsql = '' +
                    'BEGIN \n' +
                    '   jsao_file_load.get_file_chunk( \n' +
                    '      p_file_name      => :file_name, \n' +
                    '      p_byte_offset    => :byte_offset, \n' +
                    '      p_bytes_to_fetch => :bytes_to_fetch, \n' +
                    '      p_file_size      => :file_size, \n' +
                    '      p_bytes_fetched  => :bytes_fetched, \n' +
                    '      p_content_type   => :content_type, \n' +
                    '      p_piece_count    => :piece_count, \n' +
                    '      p_b64_piece_1    => :b64_piece_1, \n' +
                    '      p_b64_piece_2    => :b64_piece_2, \n' +
                    '      p_b64_piece_3    => :b64_piece_3, \n' +
                    '      p_b64_piece_4    => :b64_piece_4, \n' +
                    '      p_b64_piece_5    => :b64_piece_5, \n' +
                    '      p_b64_piece_6    => :b64_piece_6, \n' +
                    '      p_b64_piece_7    => :b64_piece_7, \n' +
                    '      p_b64_piece_8    => :b64_piece_8, \n' +
                    '      p_b64_piece_9    => :b64_piece_9, \n' +
                    '      p_b64_piece_10   => :b64_piece_10, \n' +
                    '      p_b64_piece_11   => :b64_piece_11, \n' +
                    '      p_b64_piece_12   => :b64_piece_12, \n' +
                    '      p_b64_piece_13   => :b64_piece_13, \n' +
                    '      p_b64_piece_14   => :b64_piece_14, \n' +
                    '      p_b64_piece_15   => :b64_piece_15, \n' +
                    '      p_b64_piece_16   => :b64_piece_16, \n' +
                    '      p_b64_piece_17   => :b64_piece_17, \n' +
                    '      p_b64_piece_18   => :b64_piece_18, \n' +
                    '      p_b64_piece_19   => :b64_piece_19, \n' +
                    '      p_b64_piece_20   => :b64_piece_20 \n' +
                    '   ); \n' +
                    'END;';

                dbConnection.execute(
                    plsql,
                    {
                        file_name: req.params.file_name,
                        byte_offset: currentOffset,
                        bytes_to_fetch: bytesPerFetch,
                        file_size: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.NUMBER
                        },
                        bytes_fetched: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.NUMBER
                        },
                        content_type: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 200
                        },
                        piece_count: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.NUMBER
                        },
                        b64_piece_1: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_2: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_3: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_4: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_5: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_6: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_7: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_8: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_9: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_10: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_11: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_12: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_13: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_14: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_15: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_16: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_17: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_18: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_19: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        },
                        b64_piece_20: {
                            dir: oracledb.BIND_OUT,
                            type: oracledb.STRING,
                            maxSize: 32767
                        }
                    },
                    function(err, results) {
                        if (err) {
                            return dbConnection.release(function() {
                                next(err);
                            });
                        }

                        totalBytesFetched += results.outBinds.bytes_fetched;

                        if (!headerWritten) {
                            res.writeHead(200, {
                                //'Cache-Control': 'no-cache',
                                'Content-Type': results.outBinds.content_type,
                                //'Content-Length': results.outBinds.file_size,
//'Content-disposition': 'attachment; filename=' + req.params.file_name
                            });

                            headerWritten = true;
                        }
                        

                        for (piecesIdx = 0; piecesIdx < results.outBinds.piece_count; piecesIdx += 1) {
                            res.write(new Buffer(results.outBinds['b64_piece_' + (piecesIdx + 1)], 'base64'));
                        }

                        moreToSend = results.outBinds.file_size > totalBytesFetched;

                        if (moreToSend) {
                            currentOffset += results.outBinds.bytes_fetched;
                        }

                        callback();
                    }
                );
            },
            function (err) {
                console.log('Finished sending file. Total time: ', new Date() - startDate);

                dbConnection.release(function() {});

                res.end();
            }
        );
    });
}

function delFileByName(req, res, next) {
    database.getPool().getConnection(function(err, dbConnection) {
        if (err) return next(err);

        dbConnection.execute(
            'DELETE FROM jsao_files WHERE file_name = :FILE_NAME',
            {
                FILE_NAME: req.params.file_name
            },
            {
                isAutoCommit: true
            },
            function(err, results) {
                if (err) {
                    return dbConnection.release(function() {
                        next(err);
                    });
                }

                dbConnection.release(function(err) {
                    if (err) return next(err);

                    res.send(results);
                });
            }
        );
    });
}