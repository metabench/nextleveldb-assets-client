// Extend the normal client.
//  That normal client would have a few more functions for getting the table names, or some other things, without loading the model.
//  Loading the system model from the server would be a lot of use.

// Crypto Model?
// Normal Model

// Important to now work on the continuity of data - reloading Model after restart, and resuming.

// Could be worth having a variety of collection processes too.




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


class Assets_Client extends Client {

    constructor(spec) {
        super(spec);

        // maybe best not to set the model like this.
        //  may be best to load the model from remote.

        this.model = new Crypto_Model.Database();

        // bittrex markets


    }

    // load model with structural tables.
    //  Can we replace everything in an existing Model with things from other tables?

    ensure_bittrex_currencies(arr_currencies, callback) {
        // need to get a map of all currencies by id.

        var that = this;
        var model = that.model;
        that.get_obj_map_bittrex_currencies_ids_by_name((err, map_currencies) => {
            if (err) { callback(err); } else {
                console.log('map_currencies', map_currencies);
                //throw 'stop';


                // then go through the array of currencies to see which are missing

                var arr_missing_currency_symbols = [];
                var map_missing_currency_symbols = {};
                each(arr_currencies, (currency) => {
                    console.log('currency', currency);
                    
                    console.log('typeof map_currencies[currency[0]] === \'number\'', typeof map_currencies[currency[0]] === 'number');
                    //throw 'stop';
                    if (typeof map_currencies[currency[0]] !== 'number') {
                        arr_missing_currency_symbols.push(currency[0]);
                        map_missing_currency_symbols[currency[0]] = true;
                    }
                });
                console.log('arr_missing_currency_symbols', arr_missing_currency_symbols);
                console.log('map_missing_currency_symbols', map_missing_currency_symbols);
                //throw 'stop';

                // then we push the specific currencies.
                //  make new currency records in the model
                var tbl_bittrex_currencies = model.map_tables['bittrex currencies'];
                //var currency_record = tbl_bittrex_currencies.add_record();

                var currency_data_to_add = [];

                each(arr_currencies, (currency) => {
                    console.log('currency', currency);
                    console.log('map_missing_currency_symbols[currency[0]', map_missing_currency_symbols[currency[0]]);
                    //throw 'stop';
                    if (map_missing_currency_symbols[currency[0]]) {
                        currency_data_to_add.push(currency);
                    }
                });

                console.log('currency_data_to_add', currency_data_to_add);
                console.log('currency_data_to_add[0]', currency_data_to_add[0]);
                //throw 'stop';


                // may need to fix / ensure the tbl_bittrex_currencies pk_incrementor
                console.log('tbl_bittrex_currencies.pk_incrementor', tbl_bittrex_currencies.pk_incrementor);
                console.log('tbl_bittrex_currencies.records.length', tbl_bittrex_currencies.records.length);



                tbl_bittrex_currencies.pk_incrementor = new Model.Incrementor('inc_pk_bittrex currencies', tbl_bittrex_currencies.records.length, model.inc_incrementor.increment());

                
                var added_records = tbl_bittrex_currencies.add_records(currency_data_to_add);

                // 
                console.log('added_records', added_records);
                throw 'stop';


                // could get the records including their index values from the model.

                // Those records could be a record collection. Then should be able to get their buffer representation.


                // May be worth having a script to ensure indexes for a table.



                // Server-side indexing would be useful too.
                //  Seems important to have these indexed on the server so that they can be looked up.






                //console.log('added_records', added_records);
                //console.log('added_records[0]', added_records[0]);

                // then put these records in the db


                // adding the index as well is one possibility.

                that.put_table_arr_records('bittrex currencies', added_records, (err, res_put) => {
                    if (err) { callback(err); } else {
                        console.log('res_put.length', res_put.length);
                        throw 'stop';
                    }
                });



                


            }
        });

    }


    // the map should be ids by name
    get_obj_map_bittrex_currencies_ids_by_name(callback) {
        // get the table records for bittrex currencies.
        this.get_table_records('bittrex currencies', (err, records) => {
            if (err) { callback(err); } else {
                //console.log('records', records);

                // then decode these records

                // 
                //var decoded = Crypto_Model.Database.decode_model_rows(records);
                //console.log('decoded', decoded);

                var res = {};
                each(records, (record) => {

                    res[record[1][0]] = record[0][1];
                });

                callback(null, res);
            }
        })
    }

    get_at_bittrex_currencies(callback) {
        // 
        this.get_at_table_records('bittrex currencies', callback);

    }

    get_at_bittrex_markets(callback) {
        // 
        this.get_at_table_records('bittrex markets', callback);

    }

    get_bittrex_market_names(callback) {
        //var tbl = 

        // get the records in the 'bittrex markets' table

        this.get_table_records('bittrex markets', (err, records) => {
            if (err) { callback(err); } else {
                //console.log('records', records);
                // then decode these records

                // 
                //var decoded = Crypto_Model.Database.decode_model_rows(records);
                //console.log('decoded', decoded);

                var res = [];
                each(records, (v) => {
                    res.push(v[1][1]);
                });
                callback(null, res);
            }
        })

    }

    // This db seems much more efficiently sized than before.
    //  Will also help with distributing data in a more compressed form.
    //   Should be nice to see plenty of records per second.
    //   Datasets available for download quickly in formats that are efficient to process in RAM.
    //    Ability to index the records on the server (quickly) according to the Model.


    // Seems important now, because having the records indexed on the server seems important for retrieval (in the conventional way).
    //  Indexes look like they would increase the data size a fair bit, but it seems necessary for the functionality.

    //  ensure_bittrex_market
    //  ensure_bittrex_currency

    // Then ability to ensure multiple bittrex currencies and markets
    //  Want to do that with all of the currencies and markets

    /*

    get_bittrex_market_id(market_name, callback) {
        // Markets are indexed according to name

        // Will need to do an index lookup.

        // Its not certain that various records on the server will be indexed.
        //  Could have a maintain indexes function.



    }
    */

    // Could be useful to have a db function to get the number of index records in any given table.
    //  Currently, it's not adding the index records to the server unless they are given.

    // ll_count_table_index_records(table_name, callback)




    // Think this needs indexing first.
    //  Work on nextleveldb-client to check for and ensure the server has its index records
    //   Server-side db lookups could mean some records could be made without using a Model.
    //    Using a Model may be most logical and reasonably fast.


    // want the means to get the table indexes from the live db.
    // want to be able to view the indexes easily when its in the client
    // find out about the index key construction

    // index_kp, index id, index fields with the last being the id

    // More gradually ensuring the parts of the DB will help the DB to have tables and further structural records augmented to it.
    //  Want to ensure a bunch of currencies are correctly represented within the system.

    // Using the Model to put together key lookups would make sense.
    // model.assemble_index_key (index fields)

    

    // Will then be able to do a start with many 'ensure' method calls.
    //  In many cases the data will already be there, but ensuring the existance and structure of data in the db will ensure it starts correctly.
    //  





    get_bittrex_currency_id_by_symbol(currency_symbol, callback) {
        // could reduce this to some simpler kind of lookup function.

        // refers to a table. needs a table key prefix. needs an index number

        //var 

        // will need to look up on an index.
        //  should look up on the first index field.

        // thing we will need to do recreate / ensure table indexes.
        //  some tables will not be indexed properly.

        // Can do more to 1.check indexes
        //  2. fix indexes where they are lacking.

        // Fixing indexes on the server would make sense.
        //  Server would have a loaded version of the model.

        // Server would be able to load the model out of a query of a selection of records.
        //  Model could get loaded from a binary buffer.
        //  Would probably have a few functions to query the server model to see how many records it has / see which tables it has loaded records
        //   Ability to verify that the server model has got some specific things loaded that will enable it to normalise and index.

        // could look at the model on the client too.
        //  not sure there are the indexed records there.

        if (this.model.map_tables['bittrex currencies']) {
            // Maybe it's got its rows indexed.

            // use this model to pu together the index query?

            // idx: table id, index id, keys to lookup




            // tbl.indexes[0].find(currency_symbol)

        }


    }

    // soon, want to get numeric data back as a typed array.
    //  Transmitting these over binary seems like a good option.

    get_bittrex_market_id_by_name(market_name, callback) {
        // could separate the market name into the two currencies

        // then get the ids for those two currencies. that is the market id.



    }

    // in near future, want it so the server has got the model loaded.
    //  currently have it so the client can load the model from the server, ie using the system tables, and then some other tables.
    //  want to have it so that the indexing gets done successfully.
    //   then could do some work as denormalisation.





    get_bittrex_market_snapshot_record_count(market_name, callback) {
        // construct the key lookup section
        //  tkp, market name

        // get_table_index_selection_records

        // lookup the id of the market name

        // no, need to look up the market name in the bittrex markets table.

        // Not sure that has been indexed on the client side either.

        // need to look up the 


        // table selection record count
        //  maybe choose the beginning of the key.

        this.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
            if (err) { callback(err); } else {

                //var key = [market_name];
                var key = [market_id];
        
                this.get_table_selection_record_count('bittrex market summary snapshots', key, (err, count) => {
                    if (err) { callback(err); } else {
                        console.log('count', count);
        
                        //throw('stop');
                        callback(null, count);
        
                    }
                });
            }
        });

        /*
        this.get_table_index_selection_records('bittrex markets', key, (err, records) => {
            if (err) { callback(err); } else {

            }
        });
        */

    }
}





if (require.main === module) {
        //setTimeout(() => {
    //var db = new Database();
    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420
    }
    
    var client = new Assets_Client(local_info);

    client.start((err, res_start) => {
		if (err) {
			throw err;
		} else {
            console.log('Assets Client connected.');

            // get market providers...
            //  not currently working right.


            // Referencing assets by table / tables.
            //  Would definitely be better if it had the list of market providers to start with.

            // Focus on bittrex for the moment.

            // get the time series data from within one table.

            // get the markets... bittrex markets



            // query to find out what time the data begins

            //client.maintain_table_index('bittrex markets', (err, res) => {
            //    console.log('res', res);
            //});

            // validate_core_index_table

            // though the core index table will refer to non-core/non-system tables.

            // 'table indexes', 

            
            /*
            client.validate_core_index_table((err, res) => {
                //console.log('res', res);

                if (err) { throw(err); } else {
                    console.log('res', res);


                }

            });
            */
            

            /*

            client.replace_core_index_table((err, res) => {
                //console.log('res', res);

                if (err) { throw(err); } else {
                    console.log('res', res);


                }

            });
            */
            
            /*

            client.get_bittrex_market_names((err, names) => {
                if (err) { throw(err); } else {
                    console.log('names', names);

                    client.get_bittrex_market_snapshot_record_count('BTC-NEO', (err, count) => {
                        if (err) { throw(err); } else {
                            console.log('BTC-NEO count', count);
                        }
                    });


                }
            });
            */

            var test_first_last_keys = () => {
                client.count_table_records('bittrex markets', (err, count) => {
                    console.log('bittrex markets records count', count);
    
                    client.count_table_index_records('bittrex markets', (err, count) => {
                        //console.log('bittrex markets index records count', count);
    
                        // seems like the index records were sent to the server.
    
                        if (count > 0) {
                            // Let's look at these index records.
                            // Probably best to get table index keys.
                            //  The values of these are blank.
    
                            // get_table_index_keys instead
                            //  not using the full record for indexes for the moment, maybe ever.
                            //console.log('pre get keys');
                            client.get_table_index_keys('bittrex markets', (err, keys) => {
                                if (err) { throw err; } else {
                                    //console.log('bittrex markets index keys', keys);
                                    // already decoded since it's not an ll version.
    
    
                                    //var decoded_keys = Model.Database.decode_keys(keys);
                                    //console.log('decoded_keys', decoded_keys);
    
                                    // should be able to do a table index lookup.
    
                                    // Maybe the index records are wrong.
                                    //  It may be best putting primary keys into arrays when the indexes point to multiple values.
    
    
                                    
                                    // not just for 1 record
                                    client.table_index_lookup('bittrex markets', 0, 'BTC-ETH', (err, res_btc_eth_record_key) => {
                                        if (err) { throw err; } else {
                                            //console.log('res_btc_eth_record_key', res_btc_eth_record_key); // the primary key for that record.
                                            // the pk is two values at once.
    
                                            
    
                                            // then we can use that key to look up / sub-reference the 'bittrex market summary snapshots' table.
                                            //  for the moment, lets get the count of that table.
    
                                            // Also want a general purpose time value series assets API.
                                            //  There will probably be some particular data that can be omitted or generalised.
                                            //  Could have data transformers that convert between APIs.
    
                                            client.count_table_key_selection('bittrex market summary snapshots', [res_btc_eth_record_key], (err, count) => {
                                                if (err) { throw err; } else {
                                                    //console.log('count', count);
    
                                                    // get_first_last_table_keys_in_range
    
                                                    client.get_first_last_table_keys_in_key_selection('bittrex market summary snapshots', [res_btc_eth_record_key], (err, first_last_keys) => {
                                                        if (err) { throw err; } else {
                                                            //console.log('first_last_keys', first_last_keys);
                                                            //var d_start = Date.parse(first_last_keys[0][2]);
                                                            //var d_end = Date.parse(first_last_keys[1][2]); 
                                                            var d_start = new Date(first_last_keys[0][2]);
                                                            var d_end = new Date(first_last_keys[1][2]); 
    
                                                            console.log('d_start', d_start.toUTCString());
                                                            console.log('d_end', d_end.toUTCString());
    
                                                            console.log('UTC now', new Date().toUTCString());
    
    
            
                                                            // get_first_last_table_keys_in_range
                                                            
            
            
                                                            //  will be useful for getting the time ranges for data in the db.
            
            
            
            
            
                                                            /*
            
                                                            client.get_table_keys('bittrex market summary snapshots', (err, table_keys) => {
                                                                if (err) { throw err; } else {
                                                                    console.log('table_keys', table_keys);
                    
                    
                    
                                                                }
                                                            })
                                                            */
            
                                                        }
                                                    })
    
    
    
                                                    //  will be useful for getting the time ranges for data in the db.
    
    
    
    
    
                                                    /*
    
                                                    client.get_table_keys('bittrex market summary snapshots', (err, table_keys) => {
                                                        if (err) { throw err; } else {
                                                            console.log('table_keys', table_keys);
            
            
            
                                                        }
                                                    })
                                                    */
    
                                                }
                                            })
    
                                            // 
    
                                        }
                                    });
    
                                }
                                
            
                                // seems like the index records were sent to the server.
                                
                            })
    
                        }
                    })
                })
            }

            var test_bittrex = () => {
                client.get_at_bittrex_currencies((err, at_currencies) => {
                    if (err) { throw err; } else {
                        //console.log('at_currencies', at_currencies);

                        var currency_names = at_currencies.get_arr_field_values('Currency');
                        console.log('currency_names', currency_names);


                        client.get_at_bittrex_markets((err, at_markets) => {
                            if (err) { throw err; } else {
                                //console.log('at_markets', at_markets);
        
                                var market_names = at_markets.get_arr_field_values('MarketName');
                                console.log('market_names', market_names);

                                // 

                                //client.get_table_selection_record_count()

                                //client.get_bittrex_market_snapshot_record_count('BTC-MONA', (err, ));

                                // then get the snapshot record counts for all markets.



                                // Find out how many records we have for each of the bittrex market summary snapshots


                                //  in the future this will use srver-side counting, going over the table.
                                //   counts of distinct values within range
                                // count_bittex_market_summary_snapshots_per_market

        
                                process.exit();
        
                                
                            }
                        });

                        

                        
                    }
                });
            }
            test_bittrex();

            
            

            // get record counts for these market names.
            //  for each of the market names, get the count of records in that market, for the snapshots

            // get_bittrex_market_snapshot_record_count('USDT-OMG');


        }
    });



    
} else {
    //console.log('required as a module');
}

module.exports = Assets_Client;

/*

Connect to the server
Get a copy of the system model, load it
Load some further structural tables
Query the Bittrex market snapshots table

This could have expanded capability to save data to the server.

*/

