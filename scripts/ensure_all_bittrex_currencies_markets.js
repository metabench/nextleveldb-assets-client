const jsgui = require('jsgui3');
const each = jsgui.each;

const Crypto_Model = require('nextleveldb-crypto-model');

// Could do some kind of mixin to make Stocks_Model
// Then collect data from the NASDAQ frequently.

// For the moment, should update github, npm, and deploy this to a remote server.
//  Then keep it running on that server
//  Could try having it running on another server here, even sync them 2.5s apart.

// Should soon expand the number of markets stored
// Should get other exchanges too

// For the moment, it's worth deploying this to a remote server.

// Assets Model


const Model = require('nextleveldb-model');
const Client = require('nextleveldb-client');
const Binary_Encoding = require('binary-encoding');
const Assets_Client = require('../nextleveldb-assets-client');
const Bittrex_Watcher = require('bittrex-watcher');
const Array_Table = require('arr-table');


if (require.main === module) {
    // use bittrex watcher to get all bittrex currencies

    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420
    }

    // ensure all of these currencies, and get back info that contains their IDs.

    var bw = new Bittrex_Watcher();
    client = new Assets_Client(local_info);

    // Could automatically load the core upon start.

    // Would be nice if it returned the number of records added.

    client.start((err) => {
        if (err) { throw err; } else {
            client.load_core_plus_tables(['bittrex markets', 'bittrex currencies'], (err) => {
                // Seems like it has not set up pk_incrementor of the table.

                if (err) { throw err; } else {
                    bw.get_at_all_currencies_info((err, at_all_currencies_info) => {
                        if (err) { throw err; } else {
                            console.log('at_all_currencies_info.length', at_all_currencies_info.length);
                            
                            var arr_currencies = at_all_currencies_info.values;
                            console.log('arr_currencies.length', arr_currencies.length);
                            
                            console.log('at_all_currencies_info.keys', at_all_currencies_info.keys);
                            client.ensure_bittrex_currencies(arr_currencies, (err, res_ensured) => {
                                if (err) {
                                    throw err;
                                } else {
                                    console.log('res_ensured', res_ensured);

                                    // Ensure all bittrex markets too...



                                }
                            });
                
                
                        }
                    });
                }
            })

            
        }
    });


    
    

} else {
    throw 'Run this as a main js file.';
}

