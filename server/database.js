
var oracledb = require('oracledb');
var pool;

async function connect(config, callback) {
    try {
    await oracledb.createPool(
        config,
        function(err, p) {
            if (err) throw err;

            pool = p;

            callback();
        }
    );
    } catch (err) {
        console.error('init() error: ' + err.message);
    }
}

function disconnect(callback) {
    if (pool) {
        console.log('Terminating node-oracledb connection pool');

        pool.terminate(function(err) {
            if (err) {
                console.error('Error terminating the node-oracledb connection pool: ' + err.message);
            } else {
                oracledb.getPool().close(10);
                console.log('Node-oracledb connection pool terminated');
            }

            callback();
        });
    } else {
        callback();
    }
}

function getPool() {
    return pool;
}

async function closePoolAndExit() {
  console.log('\nTerminating');
  try {
    // Get the pool from the pool cache and close it when no
    // connections are in use, or force it closed after 10 seconds
    // If this hangs, you may need DISABLE_OOB=ON in a sqlnet.ora file
    await oracledb.getPool().close(10);
    console.log('Pool closed');
    process.exit(0);
  } catch(err) {
    console.error(err.message);
    process.exit(1);
  }
}

process
  .once('SIGTERM', closePoolAndExit)
  .once('SIGINT',  closePoolAndExit);

module.exports.connect = connect;
module.exports.disconnect = disconnect;
module.exports.getPool = getPool;