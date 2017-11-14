// Extend the normal client.
//  That normal client would have a few more functions for getting the table names, or some other things, without loading the model.
//  Loading the system model from the server would be a lot of use.

// Crypto Model?
// Normal Model

// Important to now work on the continuity of data - reloading Model after restart, and resuming.

// Could be worth having a variety of collection processes too.




const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;

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


const Bittrex_Watcher = require('bittrex-watcher');

var config = require('my-config').init({
	path : path.resolve('../../config/config.json')//,
	//env : process.env['NODE_ENV']
	//env : process.env
});


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

            
            // Worth using Model to create the records?
            //  Active record which was connected to the db would be useful for this, but just putting the currencies and then getting back their records would be fine.

            // Think a lower level ensure function would be of use.
            //  Would return any records that were added.
            //   Checks by keys? Checks keys and values to see if they are the same, if not updates them.

            // Or still could use Model to better represent the data and the rows.
            //  Model could also check that the records are in the right data types and formats.


            // Creating Model records should be fine here.
            //  The Model has got OO functions that construct records.

            // Better to add records into the local model, and then check that they are correct in the database.

            // Ensure the records in the model

            // Load a model including a number of tables

            //that.load_model([]);


            /*

            var tbl_bittrex_currencies = crypto_db.map_tables['bittrex currencies'];
            tbl_bittrex_currencies.add_arr_table_records(at_all_currencies);
            //console.log('tbl_bittrex_currencies', tbl_bittrex_currencies);
            //var top_bittrex_currency_symbols = at_top_currencies;
            var bittrex_currency_symbols = at_all_currencies.get_arr_field_values('Currency');
            //console.log('bittrex_top_currency_symbols', bittrex_top_currency_symbols);

            */


            // then get the market names for these.
            //  will then lookup bittrex markets for these markets
            // 

            // 

            /*

            bw.get_markets_info((err, at_markets_info) => {
                if (err) { callback(err); } else {
                    // transform it to follow the fields.
                    //console.log('at_top_markets_info', at_top_markets_info);
                    //console.log('at_top_markets_info.length', at_top_markets_info.length);

                    // then create records for each of these markets.
                    var tbl_bittrex_markets = crypto_db.map_tables['bittrex markets'];

                    var field_names = tbl_bittrex_markets.field_names;
                    //console.log('field_names', field_names);

                    // Would need to do some lookups on IDs.

                    // Are we able to do that easily here?
                    //  tbl_bittrex_currencies.get_record_by_index_value(0, field_name);

                    // Should be able to look it up in the model's own index.

                    // then for each of the values of the arr-table, go through it to lookup some of the values.
                    //  need to look up the currency ids

                    // create/get a map of currency ids by their name.

                    var map_ids_by_currency = tbl_bittrex_currencies.get_map_lookup('Currency');
                    //console.log('map_ids_by_currency', map_ids_by_currency);
                    var arr_markets_records = [];
                    var arr_market_names = [];

                    each(at_markets_info.values, (v) => {
                        //console.log('v', v);
                        var str_market_currency = v[0];
                        var str_base_currency = v[1];

                        //arr_market_names.push(str_market_currency + '-' + str_base_currency);
                        arr_market_names.push(str_base_currency + '-' + str_market_currency);
                        
                        var market_currency_key = map_ids_by_currency[str_market_currency];
                        var base_currency_key = map_ids_by_currency[str_base_currency];

                        var market_currency_id = market_currency_key[0];
                        var base_currency_id = base_currency_key[0];

                        //console.log('market_currency_id', market_currency_id);
                        //console.log('base_currency_id', base_currency_id);

                        // then reconstruct the record. We need the right fields for the record.
                        var record_def = [[market_currency_id, base_currency_id], v.slice(4)];

                        arr_markets_records.push(record_def);
                        // then we have the base currency key values...
                    });

                    tbl_bittrex_markets.add_records(arr_markets_records);

                    callback(null, true);
                }
            })
            */
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



        var that = this;
        //var model = that.model;

        console.log('ensure_bittrex_currencies arr_currencies.length', arr_currencies.length);

        // Time to send these to the database?
        //console.trace();


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

    backup_bittrex_data(callback) {
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

        var that = this;

        that.get_bittrex_map_currencies_at_markets((err, data) => {
            if (err) { callback(err); } else {
                var [map_currencies, at_markets] = data;


                // want to repeat through the map currencies...
                //  could use fns though.
                each(map_currencies, (currency, name) => {
                    console.log('currency, name', currency, name);
                })




            }
        });




        //get_bittr








        


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

                //  want to select specific fields from that Float64_KV_Table

                // Want to return an object that itself keeps updating.

                // Want some kind of evented Float64_KV_Table
                //  Make it an Evented class anyway?
                //  So when new records get added to it, events get called.
                //  An Evented_Float64_KV_Table would be very useful for this.
                //   Would have an event for every new record.

                // Also want ability to subscribe to individual table updates.
                //  Would require scanning key prefix changes on the server.
                //   Would also allow updates within key ranges.
                //    A key prefix subscription seems like the best approach to this. It's low level, and the client can turn this into table and table
                //    selection subscriptions.

                // 

                // May be better just before some intensive processing to extract float32 data. Maybe as satoshis.


                
                // then iterate through the live snapshot records...

                // inefficient each function, I presume.
                //  would copy the values from their current places in the typed arrays.

                /*
                live_snapshots.each((snapshot_kv_record) => {
                    //console.log('snapshot_kv_record', snapshot_kv_record);

                    // now we have them, we could process them.

                    // Getting the live updating prices 

                });

                */

                console.log('live_snapshots.length', live_snapshots.length);

            }
        });



        // Returns something a bit like a promise, but a data structure too.

        // Live_Table

        // NextlevelDB_Live_Table

        // Should hold data in typed arrays.
        
        // Typed_Arrays_Table
        //  Could use specific data types for data.

        // Want to find some max values & estimate ranges.


        // Getting the serialised optimised objects from the db server would be best.




        // Maybe make a Live_Table that has a client as a property. Loads up its data with using that client. Subscribes using that client.
        //  Uses an established connection, downloads the table, stores it, subscribes to updates.



        // Should also be able to operate as a table selection.




        // Client, table_name, 




    }
}

if (require.main === module) {
        //setTimeout(() => {
    //var db = new Database();

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
                                    /*
                                    client.get_bittrex_market_snapshot_record_count('BTC-ETH', (err, count) => {
                                        if (err) { throw err; } else {
                                            console.log('BTC-ETH count', count);
                                        }
                                    });
                                    */

                                    /*
                                    
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
            
            
            
            
            
                                                            / *
            
                                                            client.get_table_keys('bittrex market summary snapshots', (err, table_keys) => {
                                                                if (err) { throw err; } else {
                                                                    console.log('table_keys', table_keys);
                    
                    
                    
                                                                }
                                                            })
                                                            * /
            
                                                        }
                                                    })
    
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
                                    */
                                }
                            })
    
                        }
                    })
                })
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
                        console.log('kv_fields', (kv_fields[0]));
                        console.log('kv_fields', (kv_fields[1]));


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


                    //throw 'stop';

                    /*

                    

                        */

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


                        //Crypto_Model.

                        // Could get these updated records back into a model.
                        //  Maybe not adding them?

                        



                        // Could find out which table each of them are on.
                        //  Tables should be indexed by key prefix.
                        //   Or (kp - 2) / 2 for the table id.
                        //    Maybe better to look them up for long term flexibility.





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

            client.load_core((err, core_model) => {
                if (err) {
                    throw err;
                } else {
                    //console.log('core_model', core_model);
                    var dmr = core_model.get_model_rows_decoded();
                    console.log('dmr', dmr);

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

