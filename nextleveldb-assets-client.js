// Extend the normal client.
//  That normal client would have a few more functions for getting the table names, or some other things, without loading the model.
//  Loading the system model from the server would be a lot of use.

// Crypto Model?
// Normal Model

// Important to now work on the continuity of data - reloading Model after restart, and resuming.

// Could be worth having a variety of collection processes too.

// For the moment - will do more client-side work, so that we can get good backups from the existing db server.
//  Don't change server-side functionality for the moment.
//  Need to be able to import data from the existing server, and have a fully running data node that also has continuously maintained data.
//   This could then be used for the first server to connect to.

// Seems like we are some way towards syncing the servers anyway.
// Two servers could subscribe to the changes of each other. 


// Getting and saving compressed backups
// Restoring backups to a server.
//  Uploading records from multiple paths.


// For the moment, more advanced server-side functionality makes sense.
//  We have the backups, got a nice amount of data.
//   May be better identifying the backups by date, with a time range.












const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;

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

const Arr_KV_Table = require('arr-kv-table');

const Float64_KV_Table = require('float64-kv-table');
const Evented_Float64_KV_Table = require('float64-kv-table');

// This typed arrays kv table is going to store quite a lot of data as 64 bit floats to begin with.
//  Want to handle satoshi conversion and types quite soon.



//const Typed_Arrays_KV_Table = require('typed-arrays-kv-table');

const Model = require('nextleveldb-model');
const Client = require('nextleveldb-client');
const Binary_Encoding = require('binary-encoding');
const path = require('path');
const date_fns = require('date-fns');

const Bittrex_Watcher = require('bittrex-watcher');


const fs = require('fs');

// Path gets resolved from app start dir I think.




function ensure_exists(path, mask, cb) {
    if (typeof mask == 'function') { // allow the `mask` parameter to be optional
        cb = mask;
        mask = 0777;
    }
    fs.mkdir(path, mask, function(err) {
        if (err) {
            if (err.code == 'EEXIST') cb(null); // ignore the error if the folder already exists
            else cb(err); // something else went wrong
        } else cb(null); // successfully created folder
    });
}


/*
var get_directories = function(dir, cb) { 
    //dir = dir.split('/').join('\\');

    //console.log('* get_directories', dir);
    fs.readdir(dir, function(err, files) {
        if (err) {
            //console.log('err', err);
            //throw err;
            cb(err);
        } else {
            //console.log('files.length', files.length);
            var dirs = [],
            filePath, c = files.length, i = 0, d = 0
            
            checkDirectory = function(err, stat) {
                //console.log('checkDirectory');
                if(stat.isDirectory()) {
                    dirs.push(files[d]);
                }
                d++;
                //console.log('i', i);
                c--;
                //console.log('c', c);
                if(c === 0) { // last record
                    cb(null, dirs);
                }
            };
            
    
            for(i=0, l=files.length; i<l; i++) {
                if(files[i][0] !== '.') { // ignore hidden
                    filePath = dir + '/' + files[i];
                    fs.stat(filePath, checkDirectory);
                }
            }
            if (files.length === 0) {
                cb(null, []);
            }
        }
    });
}
*/


/**
 * 
 * 
 * @class Assets_Client
 * @extends {Client}
 */
class Assets_Client extends Client {

    constructor(spec) {
        super(spec);

        // maybe best not to set the model like this.
        //  may be best to load the model from remote.

        //this.model = new Crypto_Model.Database();

        this.bittrex_watcher = new Bittrex_Watcher();

        // bittrex markets


    }

    // load model with structural tables.
    //  Can we replace everything in an existing Model with things from other tables?
    
    // A version to ensure bittrex currencies that actually looks them up.


    
    download_ensure_bittrex_currencies(callback) {
        // Find out which bittrex are missing?
        //  Ideally, want a put operation that also says which have been overwritten.
        //  Seems like a lower level server function would do this OK.
        // Get the original keys and values, go through them doing the replacements.

        console.log('download_ensure_bittrex_currencies');


        var that = this;


        this.bittrex_watcher.get_at_all_currencies_info((err, at_all_currencies) => {
            

            that.ensure_bittrex_currencies(at_all_currencies, callback);

            
        })
    }
    download_ensure_bittrex_markets(callback) {
        
    }
    


    // Downloading to the 

    download_ensure_bittrex_currencies_markets(callback) {

        console.log('download_ensure_bittrex_currencies_markets');
        /*
        Fns([
            [this.download_ensure_bittrex_currencies, this],
            [this.download_ensure_bittrex_markets, this]
        ]).go(callback);
        */

        // Do it together...

        // Dont want to redownload all bittrex, as we already have keys for existing records.

        // Makes most sense to add the records into an existing model.
        //  Have the local model with Bittrex data from the server
        
        // Load the local model from the server
        //  Ensure that model has got the necessary records.
        //   (reindex if necessary)

        // We can use a new Model to download the current data from the server.

        var model = new Crypto_Model.Database();
        var client = this, bw = this.bittrex_watcher;
        
        client.load_core_plus_tables(['bittrex markets', 'bittrex currencies'], (err) => {
            // Seems like it has not set up pk_incrementor of the table.

            if (err) {
                callback(err);
            } else {
                console.log('download_ensure_bittrex_currencies_markets pre fns');
                Fns([
                    //[client, client.download_ensure_bittrex_currencies, []],
                    //[client, client.download_ensure_bittrex_markets, []]
                    [client, client.download_ensure_bittrex_currencies, []],
                    [client, client.download_ensure_bittrex_markets, []]
                ]).go((err, res) => {

                    if (err) {
                        callback(err);
                    } else {
                        var [markets, currencies] = res;

                        callback(null, res);

                        /*
                        console.log('currencies.length', currencies.length);
                        
                        client.ensure_bittrex_currencies(currencies.values, (err, res_ensured) => {
                            if (err) {
                                throw err;
                            } else {
                                console.log('res_ensured', res_ensured);
    
                                // Ensure all bittrex markets too...
    
                                client.ensure_bittrex_markets(markets.values, (err, res_ensured) => {
                                    if (err) { callback(err); } else {
                                        console.log('res_ensured', res_ensured);
    
    
                                    }
                                })
    
    
    
                            }
                        });

                        */
                    }

                    
                });
            }

            //download the data

            

            /*

            if (err) { throw err; } else {
                bw.get_at_all_currencies_info((err, at_all_currencies_info) => {
                    if (err) { throw err; } else {
                        console.log('at_all_currencies_info.length', at_all_currencies_info.length);
                        
                        var arr_currencies = at_all_currencies_info.values;
                        console.log('arr_currencies.length', arr_currencies.length);
                        
                        console.log('at_all_currencies_info.keys', at_all_currencies_info.keys.length);

                        // Ensure these recods are in the local model.
                        //  Some of these currencies would already be there.

                        // ensure records by key.
                        // ensure_records_by_kv
                        //  finds which records (were unchanged)    - can leave this out
                        //  which were added
                        //  changed



                        //model.ensure_bittrex_currencies(at_all_currencies_info);
                        //model.ensure_bittrex_markets(at_all_markets_info);



                        / *

                        model.download_ensure_bittrex_currencies(at_all_currencies_info, (err, changed_currency_records) => {
                            // Find records which are new
                            // Find records which have changed.

                            console.log('changed_records', changed_currency_records);

                        });

                        * /

                        / *
                        client.ensure_bittrex_currencies(arr_currencies, (err, res_ensured) => {
                            if (err) {
                                throw err;
                            } else {
                                console.log('res_ensured', res_ensured);



                            }
                        });
                        * /
            
            
                    }
                });
            }

            */
        })


    }



    // others will be download_ensure

    /**
     * 
     * 
     * @param {array} arr_currencies 
     * @param {any} callback 
     * @memberof Assets_Client
     */
    ensure_bittrex_currencies(arr_currencies, callback) {
        // need to get a map of all currencies by id.
        //  Notso sure about using new copies of the Model all of the time.
        //  May be best to ensure they are in the client-side model as well as the remote DB.
        //   Transfer data from the local model first?



        var that = this;
        //var model = that.model;

        console.log('ensure_bittrex_currencies arr_currencies.length', arr_currencies.length);

        // Time to send these to the database?
        //console.trace();

        // Better to load the local / new crypto model with data from the db about these currencies.



        // load a new model with the data from the db server.
        var crypto_model = new Crypto_Model.Database();
        crypto_model.download_ensure_bittrex_currencies((err, res_ensure) => {
            if (err) { callback(err); } else {
                console.log('download_ensure_bittrex_currencies res_ensure', res_ensure);

            }
        });


        // get the new records from a diff.








        /*

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
        */

    }

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof Assets_Client
     */
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

    

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof Assets_Client
     */
    get_at_bittrex_currencies(callback) {
        // 
        this.get_at_table_records('bittrex currencies', callback);

    }



    /**
     * 
     * 
     * @param {any} callback 
     * @memberof Assets_Client
     */
    get_at_bittrex_markets(callback) {
        // 
        this.get_at_table_records('bittrex markets', callback);

    }



    /**
     * 
     * 
     * @param {any} callback 
     * @memberof Assets_Client
     */
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




    /**
     * 
     * 
     * @param {string} currency_symbol 
     * @param {any} callback 
     * @memberof Assets_Client
     */
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


    /**
     * 
     * 
     * @param {string} market_name 
     * @param {any} callback 
     * @memberof Assets_Client
     */
    get_bittrex_market_id_by_name(market_name, callback) {

        // Don't want to do a client-side lookup for this?

        // The local model can contain different amounts of data.
        //  Will be worth having the operations being able to complete without there being a local model.
        //  Making or using the local model can make sense for larger data-set operations, and creating tables.
        //  When just accessing records or doing index lookups, should be able to use the db itself.

        if (this.model) {
            // 


            this.table_index_lookup('bittrex markets', 0, market_name, callback);
        } else {

            // Do a remote call to the db.

            // Need to do an index lookup in the remote db.
            //  But so far indexing has not been done in the remote db.

            // Looks like getting the remote db to validate its indexes would be useful.
            //  However, that could be about having a Model on the server
            //   Maybe not with all the records, but being able to take a record array of data, and turning it into 

            // Index lookup on the server
            console.log('market_name', market_name);

            // get_table_record_by_index_lookup
            // get_table_record_field_by_index_lookup

            // look up the market name
            //  we should be able to work out which index to use.

            this.get_table_record_field_by_index_lookup('bittrex markets', 'id', 'name', market_name, callback);


            //throw 'nyi';
        }



        // could separate the market name into the two currencies

        // then get the ids for those two currencies. that is the market id.


    }

    // in near future, want it so the server has got the model loaded.
    //  currently have it so the client can load the model from the server, ie using the system tables, and then some other tables.
    //  want to have it so that the indexing gets done successfully.
    //   then could do some work as denormalisation.




    /**
     * 
     * 
     * @param {string} market_name 
     * @param {any} callback 
     * @memberof Assets_Client
     */
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
        var that = this;
        //console.log('market_name', market_name);
        this.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
            if (err) { callback(err); } else {

                //var key = [market_name];
                var key = [market_id];
                //console.log('key', key);
        
                that.get_table_selection_record_count('bittrex market summary snapshots', key, (err, count) => {
                    if (err) { callback(err); } else {
                        //console.log('count', count);
        
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

    get_bittrex_market_snapshot_records(market_name, callback) {
        // table selection record count
        //  maybe choose the beginning of the key.
        var that = this;
        //console.log('market_name', market_name);
        this.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
            if (err) { callback(err); } else {

                //var key = [market_name];
                var key = [market_id];
                console.log('key', key);
        
                // Did this get deleted too?
                that.get_table_selection_records('bittrex market summary snapshots', key, (err, records) => {
                    if (err) { callback(err); } else {
                        //console.log('count', count);
        
                        //throw('stop');
                        callback(null, records);
        
                    }
                });
            }
        });
    }

    get_bittrex_at_currencies_markets(callback) {
        Fns([
            [this, this.get_at_bittrex_currencies, []],
            [this, this.get_at_bittrex_markets, []]
        ]).go(callback); 
    }


    get_akvt_bittrex_market_snapshot_records(market_name, callback) {
        // get the fields
        var that = this;
        this.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_field_names) => {
            if (err) { callback(err); } else {
                that.get_bittrex_market_snapshot_records(market_name, (err, snapshot_records) => {
                    if (err) { callback(err); } else {

                        // 
                        console.log('kv_field_names', kv_field_names);
                        //console.log('snapshot_records', snapshot_records);
                        //throw 'stop';

                        var res = new Arr_KV_Table(kv_field_names, snapshot_records);
                        callback(null, res);
                    }
                });
            }
        });
    }



    subscribe_bittrex_market_snapshots(market_name, cb_event) {
        var that = this;

        // low level subscribe.
        //  Put together the key.

        // get the market id by the market name

        // table kp, market id
        //  market id is made out of 2 fields though.

        var cb_err = function(err) {
            var e = {
                'error': err
            }
            console.trace();
            cb_event(e);
        }

        that.get_table_kp_by_name('bittrex market summary snapshots', (err, table_kp) => {
            if (err) { cb_err(err); } else {

                that.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
                    if (err) { cb_err(err); } else {
                        console.log('market_id', market_id);

                        // The key itself contains an array.

                        //var arr_key = [];

                        // Market id field is an array, but it's enclosed in an array because the param wants an array of fields.
                        //  But that did not work. Seems its expected just as the array, as the two form the key, but are not one field within that table.
                        //   Seems like a bit of complexity to enable the simplicity of using more than one field as a single one - its a compound field.
                        

                        //var buf_kp = Model.Database.encode_key(table_kp, [market_id]);
                        var buf_kp = Model.Database.encode_key(table_kp, [market_id]);
                        console.log('buf_kp', buf_kp);
                        console.log('2) hex buf_kp', buf_kp.toString('hex'));

                        /*

                        that.count_records_by_key_prefix(buf_kp, (err, count) => {
                            if (err) { cb_err(err); } else {
                                console.log('count beginning with buf_kp', count);
                                throw 'stop';
                            }
                        });
                        */


                        // Will be subscription events like before.
                        //  Could return a Promise.

                        // subscribe_table_puts

                        this.subscribe_key_prefix_puts(buf_kp, (subscription_event) => {
                            console.log('subscription_event', subscription_event);
                        });
                    }
                });
            }
        });

        /*
        this.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_field_names) => {
            if (err) { callback(err); } else {
                that.get_bittrex_market_snapshot_records(market_name, (err, snapshot_records) => {
                    if (err) { callback(err); } else {

                        // 
                        console.log('kv_field_names', kv_field_names);
                        //console.log('snapshot_records', snapshot_records);
                        //throw 'stop';

                        var res = new Arr_KV_Table(kv_field_names, snapshot_records);
                        callback(null, res);
                    }
                })
            }
        });
        */

        // need table id?
        //  or 
        //this.subscribe_key_prefix_puts(buf_kp, cb_event);
    }


    // market name as an array
    //  market id is the array

    get_bittrex_market_snapshots_time_range(arr_market_id, callback) {
        // Need to construct the index range for this.

        //var buf_query = this.model.map_tables['bittrex market summary snapshots'].buf_pk_query([arr_market_id]);
        //console.log('buf_query', buf_query);

        this.get_first_last_table_keys_in_key_selection('bittrex market summary snapshots', [arr_market_id], (err, res_query) => {
            if (err) { throw err; } else {
                //console.log('res_query', res_query);
                var res = [res_query[0][2], res_query[1][2]];
                //console.log('res', res);



                callback(null, res);
                //throw 'stop';
            }
        });
    }

    // Could have a compressed version.
    //  Could compress by default.
    //  Binary_Encoding will read and write compressed data.



    get_buf_bittrex_market_snapshots_in_time_range(arr_market_id, arr_time_range, callback) {
        // A buffer for all of these records, not decoded.
        //  Will be easier to combine and save that way.

        var kp = this.model.map_tables['bittrex market summary snapshots'].key_prefix;
        //console.log('kp', kp);

        //console.log('arr_market_id', arr_market_id);

        var l = Crypto_Model.Database.encode_key(kp, [arr_market_id, arr_time_range[0]]);
        var u = Crypto_Model.Database.encode_key(kp, [arr_market_id, arr_time_range[1]]);

        //console.log('l', l);
        //console.log('u', u);

        //throw 'stop';
        // Not sure if this is too much to download at once, for some reason.
        //  Too big for the server's ram, without paging.

        this.ll_get_buf_records_in_range(l, u, callback);

    }

    download_save_bittrex_market_time_range_snapshots_by_day(arr_market_id, path, callback) {
        
        // Call this multiple times from the backup function.
        //  Show progress of how much gets downloaded
        //   will be nice to see how many MB per second or minute.

        // Get the bittrex market snapshots within a time range


        // Get the daily market snapshots in a time range.
        //  May require other server options.
        //   Could be best to break the data down into smaller portions.
        //    Daily should probably be OK. Not sure though.



        this.get_buf_bittrex_market_snapshots_in_time_range(arr_market_id, (err, buf_res) => {
            // Could take a while...
            if (err) { callback(err); } else {
                console.log('buf_res', buf_res);
                console.log('buf_res.length', buf_res.length);
            }
        });






        // return downloaded size, then possibly compressed size.
        //  later, will have Binary_Encoding allow for compressed data to be saved within.
        //   will reference a compression algorythm and give its settings too. Could start with one or two with sensible default settings.



    }

    // Could do some backups in the more general purpose client.
    //  Backup range
    //  Give it a backup path

    // .new_backup_path






    // Backup path
    //  

    backup_bittrex_market_snapshots(path, arr_market_id, market_name, callback) {
        // Backup path management.
        //  Each backup will have its own path.
        //   Could operate using backup numbers, batches.

        // Will be called by backup_all_bittrex_market_snapshots

        // Could add extra handling for this later, and just use one backup path.
        //  Backups could subdivide into multiple files, binary encoding, simply named.
        //   Could undergo further compression.

        // download the records, day by day.
        var that = this;

        console.log('backup_bittrex_market_snapshots', market_name);

        this.get_bittrex_market_snapshots_time_range(arr_market_id, (err, time_range) => {
            if (err) { throw err; } else {
                //console.log('time_range', time_range);


                var tr = [(new Date(time_range[0])).toISOString(), (new Date(time_range[1])).toISOString()];
                //console.log('tr', tr);

                //console.log(tr[0] + ' to ' + tr[1]);


                var str_format = 'dddd D MMMM YYYY hh.mma'

                var f_start = date_fns.format(tr[0], str_format);
                //console.log('f_start', f_start);

                var f_end = date_fns.format(tr[1], str_format);
                //console.log('f_end', f_end);

                var tr_msg = market_name + ' ' + f_start + ' to ' + f_end;
                //console.log('tr_msg', tr_msg);

                // then save them to disk

                //console.log('arr_market_id, time_range', arr_market_id, time_range);

                that.get_buf_bittrex_market_snapshots_in_time_range(arr_market_id, time_range, (err, buf_data) => {
                    if (err) { callback(err); } else {
                        //console.log('buf_data', buf_data);
                        //console.log('market_name', market_name);

                        //var save_dir_path = 

                        ensure_exists(path, (err, exists) => {
                            if (err) { callback(err); } else {
                                var save_path = path + '/' + tr_msg + '.be';
                                console.log('save_path', save_path);

                                // Tricky, because this buffer contains XAS2 prefixed data, the records.
                                //  Would be better to have the server return a different record format.
                                //  However, that may be a tricky change to make totally reliably right now.

                                //



                                var comp_buf = Binary_Encoding.compress_buffer_zlib9(buf_data);
                                console.log('comp_buf.length', comp_buf.length);

                                //fs.writeFile(save_path, buf_data, callback);
                                fs.writeFile(save_path, comp_buf, callback);
                            }
                        });

                        // Could be better to save 1 file per day.
                        //  That could also be nice with p2p sharing.

                        // then save it to a specific backup path



                    }
                });

                // then download all of those records.
                //  could do download to disk.

                // Could have some kind of a 'lock' system to say when a backup is still running.
                //  Completed backups have got a note saying when they were completed.
                //  It could always create a new backup directory.
                //  00001 - [Date] [Backup name]


                // Backup functions should be given a backup directory, or they should create a new one.
                // Date-fns would greatly help.

            }
        });

    }

    backup_bittrex_data(callback) {
        console.log('begin backup_bittrex_data');

        // This would create its own backup task and path.
        //  Create backup root path.
        //  Or maybe do this as part of another backup.



        // For every market and currency, download all the data about them, separately.
        //  Then encode the data to disk.

        // Backups directory
        //  Then date and time of backup
        //   Then the table name dir
        //    Then directories for each of the markets
        //     Then 1 binary file per day.

        // Want easier to use, more logical functions to do this.

        //  iterate_markets
        //  each_bittrex_market
        //   could provide the record data as well as some decodings of it.

        // Finding out the first and the last key within ranges.

        // get the bittrex markets and currencies together.
        //  seems like a useful function, as the two are needed together.
        //   map of currencies, list of markets

        var friendly_tr = arrayify((time_range) => {
            var tr = [(new Date(time_range[0])).toISOString(), (new Date(time_range[1])).toISOString()];
            //console.log('tr', tr);

            //console.log(tr[0] + ' to ' + tr[1]);


            var str_format = 'dddd D MMMM YYYY hh:mma'

            var f_start = date_fns.format(tr[0], str_format);
            //console.log('f_start', f_start);

            var f_end = date_fns.format(tr[1], str_format);
            //console.log('f_end', f_end);

            var tr_msg = f_start + ' to ' + f_end;
            //console.log('tr_msg', tr_msg);

            //console.log('at_markets.values[0]', at_markets.values[0]);

            var market_name = at_markets.values[0][3];

            return ([market_name, [f_start, f_end], [time_range[0], time_range[1]]]);
        });

        var that = this;
        var model = that.model;
        var tbl_bittrex_market_snapshots = model.map_tables['bittrex market summary snapshots'];

        that.new_backup_path('bittrex market summary snapshots', (err, backup_path) => {

            console.log('backup_path', backup_path);

            that.get_bittrex_at_currencies_markets((err, data) => {
                if (err) { callback(err); } else {
                    var [at_currencies, at_markets] = data;
    
                    // want to repeat through the map currencies...
                    //  could use fns though.
                    var map_currencies = {};
                    
                    each(at_currencies.values, (currency) => {
                        //console.log('currency', currency);
                        map_currencies[currency[0]] = currency[1];
                    });
    
                    // then go through the markets.
                    //  for every market, download the full set of data
                    //  download it by day, in managable chunks, where progress can be viewed.
    
                    // Should not take all that long to do a full backup of the past 2 week's bittrex data.
                    //  Expect a good few MB/s to be processed and downloaded.
                    //   Not using paging, but downloading relatively small daily datasets.
    
                    // Seems we need to put together the market key (again).
                    //  A bit tricky with the key having a compound field, referring to 2 separate pks and records of currencies.
    
                    // find out the timings of all of the market data.
    
                    var fns = Fns();
                    each(at_markets.values, (arr_market) => {
                        console.log('arr_market', arr_market);
    
                        // then we can look up the records from server.
                        //  could possibly do model_table.buf_pk_query([arr_market[0], arr_market[1]])
    
                        // bittrex market summary snapshots
    
                        //var buf_query = tbl_bittrex_market_snapshots.buf_pk_query([arr_market[0], arr_market[1]]);
                        //console.log('buf_query', buf_query);
                        //throw 'stop';
    
                        fns.push([that, that.backup_bittrex_market_snapshots, [backup_path, [arr_market[0], arr_market[1]], arr_market[3]]]);
    
    
                        // or to get the 
    
                    }, (err, res) => {
                        if (!err) {
                            console.log('backup arr_market', arr_market, 'complete');
                        }
                    });
                    fns.go((err, res_all) => {
                        if (err) {
                            callback(err);
                        }
                        console.log('all bittrex backups complete')
                    });
    
                    var process_multiple = () => {
                        var arr_market_snapshot_time_ranges = [];
                        // get the snapshot time ranges for all markets.
                        var fns = Fns();
                        each(at_markets.values, (market_value) => {
                            //console.log('market_value', market_value);
                            fns.push([that, that.get_bittrex_market_snapshots_time_range, [[market_value[0], market_value[1]]]]);
                        });
                        console.log('getting available asset price time ranges')
                        fns.go(8, (err, arr_time_ranges) => {
                            if (err) { callback(err); } else {
                                console.log('arr_time_ranges', arr_time_ranges);
                            }
                        });
                    }
                }
            });
        })
    }

    live_bittrex_snapshots(market_name) {

        // load the data for that market.
        //  for the moment put it into a typed-arrays-kv-table

        var that = this;
        that.get_akvt_bittrex_market_snapshot_records(market_name, (err, akvt_snapshots) => {
            if (err) { callback(err); } else {
                console.log('akvt_snapshots.length', akvt_snapshots.length);
                //var live_snapshots = new Typed_Arrays_KV_Table(akvt_snapshots.keys, akvt_snapshots.values);

                // Use the Evented version to deal with the live snapshots events.
                //  Subscribe to the incoming events, add them to this live table.

                // Use the evented table, and set up the events here.

                var live_snapshots = new Evented_Float64_KV_Table(akvt_snapshots.keys, akvt_snapshots.values);

                /*

                that.subscribe_bittrex_market_snapshots(market_name, (snapshot_event) => {
                    console.log('snapshot_event', snapshot_event);


                });
                */

                that.subscribe_table_puts('bittrex market summary snapshots', (snapshot_event) => {
                    console.log('snapshot_event', snapshot_event);
                });

                console.log('live_snapshots.length', live_snapshots.length);
            }
        });
        

    }
}

if (require.main === module) {
        //setTimeout(() => {
    //var db = new Database();
    var config = require('my-config').init({
        path : path.resolve('../../config/config.json')//,
        //env : process.env['NODE_ENV']
        //env : process.env
    });

    var server_data1 = config.nextleveldb_connections.data1;
    //var server_data1 = config.nextleveldb_connections.localhost;

    // The table field (for info on the fields themselves) rows are wrong on the remote database which has got approx 12 days of data.
    //  Can still extract the data, I expect.

    // Don't want to replace the code on the server quite yet.

    // May be possible to edit the fields, possibly validate the fields?
    
    var client = new Assets_Client(server_data1);

    client.start((err, res_start) => {
		if (err) {
			throw err;
		} else {
            console.log('Assets Client connected to', server_data1);

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

                // Will be better to get_bittrex_market_snapshots_time_range



                /*

                client.count_table_records('bittrex markets', (err, count) => {
                    console.log('bittrex markets records count', count);
    
                    client.count_table_index_records('bittrex markets', (err, count) => {
                        console.log('bittrex markets index records count', count);
    
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
                                    console.log('bittrex markets index keys', keys);
                                    // already decoded since it's not an ll version.
    
    
                                    //var decoded_keys = Model.Database.decode_keys(keys);
                                    //console.log('decoded_keys', decoded_keys);
    
                                    // should be able to do a table index lookup.
    
                                    // Maybe the index records are wrong.
                                    //  It may be best putting primary keys into arrays when the indexes point to multiple values.
    
                                    // count_bittrex_market_snapshot_records

                                    // get_bittrex_market_snapshot_records
                                    / *
                                    client.get_bittrex_market_snapshot_record_count('BTC-ETH', (err, count) => {
                                        if (err) { throw err; } else {
                                            console.log('BTC-ETH count', count);
                                        }
                                    });
                                    * /

                                    / *
                                    
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
    
                                                    
    
                                                    //  will be useful for getting the time ranges for data in the db.
    
    
                                                    / *
    
                                                    client.get_table_keys('bittrex market summary snapshots', (err, table_keys) => {
                                                        if (err) { throw err; } else {
                                                            console.log('table_keys', table_keys);
            
            
            
                                                        }
                                                    })
                                                    * /
                                                }
                                            })
                                        }
                                    });
                                    * /
                                }
                            })
                            
                        }
                    })
                })
                */
            }
            //test_first_last_keys();

            var test_get_btc_eth_snapshot_records = () => {

                // get_akvt_bittrex_market_snapshot_records
                client.get_akvt_bittrex_market_snapshot_records('BTC-ETH', (err, recordset) => {
                    if (err) { throw err; } else {
                        console.log('BTC-ETH', recordset.keys);
                        console.log('BTC-ETH', recordset.length);

                        // would be nice to load them into an arr-kv-table
                        //  Then could select the more important field values from them.
                        //  Also can do some processing to get them at specific indexed time values.

                        // Go through all of them.

                        // Select specific fields out of such a kv arr table.
                        //  just get these as an array / array table
                        //  could get these as a typed array table.
                        //  
                        // the timestamp, last, volume

                        // to_flat_arr
                        // select_to_arr
                        // select_to_flat_arr

                        var timed_prices = recordset.flatten(['timestamp', 'last']);
                        console.log('timed_prices', timed_prices);

                        // Would be worth putting these on a graph.

                        // Downloading a large amount of data quickly, alongside subscribing to updates, would help to allow decisions to be made using the available data

                        // Would like an in-memory data object that subscribes to the online data.

                        // Setting up subscriptions to the data coming in from the server makes a lot of sense.
                        //  May need some functions to do some simple transformations on incoming data / records too.

                        // Could subscribe to new records going into a table.
                        //  Table subscription.
                        //   Then there would be message numbers within the subscription.


                        // May be cool to compress these to a typed array.
                        //  Number of satoshis could be stored as an xas2.

                        // Should be possible to do this kind of selection from the DB itself - but the DB would need to decode,select, then encode the data.
                        //  Should not be all that hard to do, would lead to improved performance in the future.

                        // For the moment, want to use the results sets that are available.

                        // Should be possible to download lots of different markets / all markets using one command.
                        //  Would need the right data structures though.
                        //  Would be nice to have the data get arranged on the server into an easy to use format.

                        // Then would be nice if it was dealing with change over time events.
                        //  Once this has been going a month (or even a week / few more days) it will be much more of a trove of useful data.

                        // Can work on executing existing algorithmic strategies.

                        // Downloading multiple datasets with multiple requests.

                        // Definitely looks like subscribing to table updates makes sense.
                        //  Would require checking which table any record is going to.
                        //  Could subscribe based on key prefix.
                        //  Any matching key prefix of a record would then be sent along to the subscriber.

                        //

                        // Subscribing to real-time data coming in seems very important to this.
                        //  5s response time seems OK to start with.


                        /*
                        each(recordset.records, (record) => {
                            console.log('record', JSON.stringify(record));
                        })
                        */


                    }
                });
                //test_get_btc_eth_snapshot_records();

                //client.

                /*

                client.get_bittrex_market_snapshot_records('BTC-ETH', (err, records) => {
                    if (err) { throw err; } else {
                        console.log('BTC-ETH', records);

                        // would be nice to load them into an arr-kv-table
                        //  Then could select the more important field values from them.
                        //  Also can do some processing to get them at specific indexed time values.




                    }
                });
                */
            }
            //test_get_btc_eth_snapshot_records();

            // get these records as an arr-table.
            // get both the keys and values.

            // Could get the snapshots as an Array_Table.
            //  Array_Table could have num_keys or num_key_fields to help the records get encoded into the db.

            var test_get_snapshot_fields = () => {


                // get the fields just as names

                client.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_fields) => {
                    if (err) { throw(err); } else {
                        //console.log('kv_fields', JSON.stringify(kv_fields, null, 2));
                        //console.log('kv_fields', (kv_fields[0]));
                        //console.log('kv_fields', (kv_fields[1]));


                        client.get_table_field_names('bittrex market summary snapshots', (err, fields) => {
                            if (err) { throw(err); } else {
                                //console.log('kv_fields', JSON.stringify(kv_fields, null, 2));
                                console.log('fields', fields);
        
                            }
                        });
                    }
                });

                /*
                client.get_table_id('bittrex market summary snapshots', (err, table_id) => {
                    if (err) { throw(err); } else {
                        console.log('table_id', table_id);


                    }
                });

                */
            }
            //test_get_snapshot_fields();


            // 

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
            //test_bittrex();

            // Want to have a component that listens for these updates and creates appropriate records.
            //  Not adding them to the model?
            //   May be tricky with their reference back to the table.
            
            var test_subscribe_all = () => {
                console.log('test_subscribe_all');
                //client.ll_subscribe_all((evt) => {
                var unsubscribe = client.subscribe_all((evt) => {
                    console.log('evt', evt);
                    
                    if (evt.type === 'connected') {
                        console.log('connected');
                        console.log('client_subscription_id', evt.client_subscription_id);

                        setTimeout(() => {
                            console.log('pre unsubscribe');
                            unsubscribe();

                        }, 10000)
                    }

                    if (evt.type === 'batch_put') {
                        var records = evt.records;
                        //console.log('records', JSON.stringify(records));
                        console.log('records.length', JSON.stringify(records.length));
                        
                    }
                });

            }
            //test_subscribe_all();


            var test_live_btc_eth = () => {
                var live_hist_btc_eth = client.live_bittrex_snapshots('BTC-ETH');

                // Will return an object which itself processes events, as it gets the data back from the server.
                //  Server-side subscriptions are a good way to do this.



            }
            //test_live_btc_eth();

            // Loading the core does not work (any longer) because the fields get loaded wrong.
            //  Could have bug fix swap, now the bug has been found and fixed.
            console.log('pre load core');
            client.load_core((err, core_model) => {
                if (err) {
                    throw err;
                } else {
                    //console.log('core_model', core_model);
                    var dmr = core_model.get_model_rows_decoded();
                    console.log('decoded model from remote', dmr);
                    // Has even applied some fixes to malformed rows.


                    client.backup_bittrex_data((err, res_backup) => {
                        if (err) { console.trace(); throw err; } else {
                            console.log('res_backup', res_backup);


                        }
                    });

                }
            });

            // get the bit-eth prices... all of them
            //  Later on, want to get a dataset that holds all the the values for a bunch of currencies, all within 1 month.
            //   This should be a very useful window of data. Should be fine for many decisions. Would be of use for training.

            // Definitely want to do more than just get all of them eventually.

            // would need to compose the keys for the search.

            // ll_subscribe_all

            // Client-side - want to put records together in an efficient way.
            //  Could even start with all market snapshot records.
            //  Put all sets of them into a data structure.

            // Want some kind of a connected data structure.
            //  Would have some transformation functions given as params.

            // Could be part of Assets_Client.

            // Time indexed values?
            //  Or have time indexes as part of some other table functionality.

            // Deal with db connectedness first.

            // Would be best if it loaded itself gradually to start with.
            //  Then could optimize.

            // Lets have a table / typed array table that holds the price history.
            //  Don't think we properly get the volumes right now.
            // Live_Price_History
            // Live_Time_Value_Series

            // Live_Table
            // Need to choose the naming of whatever live control to use.
            // Would connect to a ll nldb client.
            //  Assets_Client could use it and return it.
            // client.live_bittrex_snapshots
            // var live_hist_btc_eth = client.live_time_value_series('BTC-ETH');

            // and that live object would raise some events, and tap into the all data subscription.

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