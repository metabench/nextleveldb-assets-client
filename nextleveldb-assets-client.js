const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;
const is_array = lang.is_array;
const Evented_Class = lang.Evented_Class;

const Crypto_Model = require('nextleveldb-crypto-model');

const xas2 = require('xas2');

const Arr_KV_Table = require('arr-kv-table');

const Float64_KV_Table = require('float64-kv-table');
const Evented_Float64_KV_Table = require('float64-kv-table');
const Model = require('nextleveldb-model');
const Client = require('nextleveldb-client');
const Binary_Encoding = require('binary-encoding');
const flexi_encode_item = Binary_Encoding.flexi_encode_item;


const path = require('path');
const date_fns = require('date-fns');

const Bittrex_Watcher = require('bittrex-watcher');


const fs = require('fs');
//const { promisify } = require('util');
const util = require('util');
//const promisify = util.promisify;
const promisify = require('bluebird').promisify;

// Path gets resolved from app start dir I think.

// This may get phased out because currently it's only Bittrex.
//  Should have some fairly small, concise declarations of db functionality that will be fairly pluggable.


// Eventually could get the DB itself to run collection processes, possibly collection services within its own process?
//  For the moment, want to keep the core DB functionality very certain to work and separate from some expiremental features.

// Getting the DB able to ensure that various tables are there will be very useful. Creating them if they are not there.

// Then will be easy to add further markets, data sources, and features.
//  Will get the NextLevelDB as a very solid basis for other pieces of functionality.



// 13/03/2018 - Needs further coding and testing to make sure it's adding the Bittrex structure records properly.
//  Will not be long until this is a very capable DB system that acquired a lot of data, makes history data available in convenient formats, and allows subscriptions to ongoing data events.






const table_defs = require('./tables');






function ensure_exists(path, mask, cb) {
    if (typeof mask == 'function') { // allow the `mask` parameter to be optional
        cb = mask;
        mask = 0777;
    }
    fs.mkdir(path, mask, function (err) {
        if (err) {
            if (err.code == 'EEXIST') cb(null); // ignore the error if the folder already exists
            else cb(err); // something else went wrong
        } else cb(null); // successfully created folder
    });
}

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
        this.model = new Crypto_Model.Database();
        //console.log('this.model.download_ensure_bittrex_currencies', this.model.download_ensure_bittrex_currencies);
        //throw 'stop';
        this.bittrex_watcher = new Bittrex_Watcher();
    }

    get_bittrex_market_id_by_currency_ids(arr_currency_pair_ids, callback) {
        // The bittrex markets are indexed according to their currency pairs.
        //  It's actually their primary key.

    }


    // Currency lookup function.

    get_bittrex_currency_id_by_code(currency_code, callback) {

        // If there is no callback, then it returns a promise, using the inner function which has a callback.

        let inner = (callback) => {
            this.get_table_id_by_name('bittrex currencies', (err, table_id) => {
                if (err) {
                    callback(err);
                } else {
                    // compose the index
                    // Maybe the tables have become messed up.
                    //  Best to start again on dev machine?
                    //console.log('table_id', table_id);
                    //throw 'stop';
                    let kp = table_id * 2 + 2;
                    let ikp = kp + 1;
                    var arr_buf = [xas2(ikp).buffer, xas2(0).buffer, flexi_encode_item(currency_code)];
                    //console.log('arr_buf', arr_buf);
                    let buf = Buffer.concat(arr_buf);
                    //console.log('buf', buf);

                    this.ll_get_records_by_key_prefix(buf, (err, res_records) => {
                        if (err) {
                            throw err;
                        } else {

                            var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_records);
                            //console.log('arr_kv_buffers', arr_kv_buffers);


                            //console.log('res_records', res_records);
                            let decoded = Model.Database.decode_model_rows(arr_kv_buffers, 2);
                            //console.log('* decoded', decoded);
                            //throw 'stop';
                            if (decoded.length > 0) {
                                let found_index_row = decoded[0];
                                //console.log('found_index_row', found_index_row);
                                let currency_id = found_index_row[0][1];
                                callback(null, currency_id);
                            } else {
                                callback(null, undefined);
                            }
                        }
                    });
                }
            })
        }

        if (callback) {
            inner(callback);
        } else {
            return new Promise((resolve, reject) => {
                inner((err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                })
            })
        }

        // Maybe this could be done using observable / promise.

        //console.log('currency_code', currency_code);


    }

    // ensure_record

    // Making a simple DB with test case would be nice.


    ensure_bittrex_market(arr_market, callback) {
        console.log('ensure_bittrex_market arr_market', arr_market);

        // Could have JS Market objects.
        //  It could work in a functional way where it creates record arrays.

        // Or a flexible record system.

        // Could be worth making, and testing different component parts of this.
        //  Load the db, the model, and the collector all at once.


        throw 'stop';



    }
    // This could be broken into separate functions.

    ensure_at_bittrex_currencies(at_bittrex_currencies, callback) {
        // iterate through the bittrex currencies.
        let go = async () => {

            // Are all of the existing currencies in the DB indexed?

            // await maintain_table_index

            // maintain_table_index could operate on the server.

            // Having a server side function to do that would be quite useful.

            //  It would check to see if every index record that should be there actually is there.

            // Indexing and index lookups is one part of the DB that is not fully there yet.
            //  Likely to require a fair bit more coding on the server, and it will be best to encapsulate it clearly into the relevant concepts.

            // Server side maintenance and checking of indexes will be very useful.
            //  Would help to keep the client application simpler at least, and it should not require too much client-server communication.






            // after maintain_table_index, ensuring currencies should work fine.
            //  possibly in the past a number of currencies were added, without being indexed

            // Also need to get indexing working as standard when adding records that get indexed.




            for (let arr_bittrex_currency of at_bittrex_currencies.values) {
                let res_ensure = await this.ensure_arr_bittrex_currency(arr_bittrex_currency);
            }
        }
        go().catch(err => {
            console.trace();
            throw err;
        }).then(res => {
            console.log('res', res);
            console.log('then ensure_at_bittrex_currencies');
            callback(null, true);
        });
    }

    // ensure_at_bittrex_markets
    ensure_at_bittrex_markets(at_bittrex_markets, callback) {
        // iterate through the bittrex currencies.
        let go = async () => {
            console.log('at_bittrex_markets.values.length', at_bittrex_markets.values.length);
            for (let arr_bittrex_market of at_bittrex_markets.values) {
                let res_ensure = await this.ensure_arr_bittrex_market(arr_bittrex_market);
            }
        }
        go().catch(err => {
            console.trace();
            throw err;
        }).then(res => {
            console.log('res', res);
            console.log('then ensure_at_bittrex_markets');
            callback(null, true);
        });
    }

    // Would get called when there is a new bittrex currency.
    put_bittrex_currency(arr_bittrex_currency, callback) {
        //console.log('put_bittrex_currency', arr_bittrex_currency);

        // This does not work if the currencies are not indexed.


        // A streaming validate table indexes looks possible.
        //  Could get back the records as well as validation that the indexes are correct.

        // Server side validation of index records looks like it would be useful.
        //  For any record, should be able to get its index keys.






        // Maybe validate the record to go in against the Model.
        //  We don't already have the key to these records.

        // Using the Model at some stage could be useful. It's got pk incrementor values.

        // Using a lower level table_pk_increment function could do the job, where the incrementor values in the DB are updated.


        // Index records have not been loaded into the model.




        // do this using the Model for the moment.
        let tbl_bittrex_currencies = this.model.map_tables['bittrex currencies'];
        // For some reasons, the pk incrementor has not been set up properly in the model.
        //  Its value is at 0.
        //  Value should be appropriately high.


        //console.log('tbl_bittrex_currencies.pk_incrementor', tbl_bittrex_currencies.pk_incrementor);

        // Then use add records to this.
        let new_record = tbl_bittrex_currencies.add_record(arr_bittrex_currency);
        console.log('new_record', new_record);

        //console.trace();
        //throw 'stop';


        // The client should be able to put a model record.
        //  That would use a batch put operation that also puts the index values into place.

        this.put_model_record(new_record, callback);


        // then put_model_record.
        //  It would use the model to generate the index values, and then put them into the DB.

    }

    put_bittrex_market(arr_bittrex_market, callback) {

        // Some kind of transformation / lookup

        //  Could have a lower level advanced record put.
        //   It would check that the fields align, if not, selects the fields appropriately.

        //  Could define alternate data input structures, with it carrying out the lookups.

        // Defining Data Transformations and Lookups would be useful.
        //  Could just use Binary_Encoding to send data to the server, not as the DB record types, but as they come in.
        //  The DB transforms them according to a given definition.


        // Having data transformation within the Model would be useful for various reasons.
        //  Could call upon it server side too, and it would be convenient doing it in the client in a variety of cases.

        // For the moment though, it would not be the fastest way to do it.
        //  Seems like the most interesting way though.

        let tbl_bittrex_currencies = this.model.map_tables['bittrex currencies'];
        let tbl_bittrex_markets = this.model.map_tables['bittrex markets'];
        // For some reasons, the pk incrementor has not been set up properly in the model.
        //  Its value is at 0.
        //  Value should be appropriately high.


        //console.log('tbl_bittrex_currencies.pk_incrementor', tbl_bittrex_currencies.pk_incrementor);

        // Then use add records to this.

        // The model table could use transformations / lookups.

        // Defining transformations in the DB layer looks like it could be too much to do right now.
        //  Definitely seems like the most advanced way...


        // Need to modify / map the bittrex market record so that it refers to the currencies.
        //  Can do that by doing a DB lookup, ie get_bittrex_currency_id_by_code for the two currencies that together make the market.

        // No need to do automated FK lookup here to save a small amount of code here, creating a much larger amount of code and complexity elsewhere.
        //  Then put these market records into the DB properly.

        console.log('arr_bittrex_market', arr_bittrex_market);

        let base_code = arr_bittrex_market[0];
        let market_code = arr_bittrex_market[1];

        // then we get these ids.

        let pr_base_id = this.get_bittrex_currency_id_by_code(base_code);
        let pr_market_id = this.get_bittrex_currency_id_by_code(market_code);


        Promise.all([pr_base_id, pr_market_id]).then(values => {
            console.log('values', values);
            let [base_id, market_id] = values;

            let kv_record = [
                [base_id, market_id],
                [arr_bittrex_market[4], arr_bittrex_market[5], arr_bittrex_market[6], arr_bittrex_market[7], arr_bittrex_market[8], arr_bittrex_market[9], arr_bittrex_market[10]]
            ];

            //console.log('kv_record', kv_record);
            //throw 'stop';

            let new_record = tbl_bittrex_markets.add_record(kv_record);
            //  This could possibly do some reference lookups when the wrong data types are given.

            //console.log('new_record', new_record);

            this.put_model_record(new_record, callback);

            // 

            //console.trace();
            //throw 'stop';

        });



        //let akv_bittrex_market = [];

        /*

            
            field_info {"id":0,"name":"market_currency_id","type_id":null,"fk_to":{"table_name":"bittrex currencies","table_id":5,"fields":[[0,"id",1]]}}
            field_info {"id":1,"name":"base_currency_id","type_id":null,"fk_to":{"table_name":"bittrex currencies","table_id":5,"fields":[[0,"id",1]]}}
            field_info {"id":2,"name":"MinTradeSize","type_id":null}
            field_info {"id":3,"name":"MarketName","type_id":null}
            field_info {"id":4,"name":"IsActive","type_id":null}
            field_info {"id":5,"name":"Created","type_id":null}
            field_info {"id":6,"name":"Notice","type_id":null}
            field_info {"id":7,"name":"IsSponsored","type_id":null}
            field_info {"id":8,"name":"LogoUrl","type_id":null}

        */


        // Basically, want to work more on constructing the Bittrex market and snapshot records, and get them flowing into the system.

        // Should be able to do the necessary lookups here and have logic to build the records. No need to have the DB work out how to do record transformations.
        //  Could send it a table with named fields, and have it work out how to persist, but it will be faster now to get it to push the data quickly, having done the right transformations.

        // Having it store data very soon is going to be very useful.
        //  Need to make it so the data can be retrieved easily, operating with 'workstation mode' syncing.








        // The client should be able to put a model record.
        //  That would use a batch put operation that also puts the index values into place.

        //


        // then put_model_record.
        //  It would use the model to generate the index values, and then put them into the DB.



    }


    ensure_arr_bittrex_currency(arr_bittrex_currency, callback) {
        let a = arguments,
            l = a.length;
        //console.log('l', l);
        //console.log('arr_bittrex_currency', arr_bittrex_currency);

        // get the currency by name

        let inner = (callback) => {
            let code = arr_bittrex_currency[0];
            //console.log('pre get_bittrex_currency_id_by_code');
            // These should be indexed in the DB.

            this.get_bittrex_currency_id_by_code(code, (err, currency_id) => {
                if (err) {
                    callback(err);
                } else {
                    console.log('currency_id', currency_id);
                    if (typeof currency_id === 'undefined') {
                        // it's not already in the DB.
                        //  add the currency record to the DB.

                        this.put_bittrex_currency(arr_bittrex_currency, (err, res_put) => {
                            if (err) {
                                callback(err);
                            } else {
                                console.log('put_bittrex_currency res_put', res_put);
                                callback(null, res_put);
                            }
                        });

                    } else {

                        callback(null, true);
                    }
                }
            });
        }

        if (callback) {
            inner(callback);
        } else {
            return new Promise((resolve, reject) => {
                inner((err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                })
            })
        }

        // transform it into the proper form of currency record.
        //  The Model can do that.

        // is array of length 2, items 0 and 1 are also arrays.

    }

    ensure_arr_bittrex_market(arr_bittrex_market, callback) {
        //console.log('arr_bittrex_market', arr_bittrex_market);
        let inner = (callback) => {
            // need to lookup the market currency and mase currency

            let market_currency_code = arr_bittrex_market[0];
            let base_currency_code = arr_bittrex_market[1];

            // get currency ids for both of them.

            Fns([
                [this, this.get_bittrex_currency_id_by_code, [market_currency_code]],
                [this, this.get_bittrex_currency_id_by_code, [base_currency_code]]
            ]).go((err, market_currency_ids) => {
                if (err) {
                    callback(err);
                } else {

                    console.log('market_currency_ids', market_currency_ids);

                    this.get_table_record('bittrex markets', market_currency_ids, (err, res_record) => {
                        if (err) {
                            callback(err);
                        } else {

                            // Better if it returns undefined if the record is not found, rather than an error.

                            //console.log('res_record', res_record);
                            //console.log('res_records.length', res_records.length);

                            if (res_record) {
                                callback(null, res_record);
                            } else {
                                // 

                                //console.trace();
                                //console.log('arr_bittrex_market', arr_bittrex_market);

                                //throw 'not found';

                                this.put_bittrex_market(arr_bittrex_market, (err, res_put) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        console.log('put_bittrex_market res_put', res_put);
                                        callback(null, res_put);
                                    }
                                });
                            }
                        }
                    })
                }
            });

        };

        if (callback) {
            inner(callback);
        } else {
            return new Promise((resolve, reject) => {
                inner((err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                })
            })
        }
    }

    ensure_bittrex_structure_current(callback) {


        // though maybe this won't use a callback.



        let res = new Evented_Class();

        // Needs to load up the relevant data tables for bittrex, and ensure they are in the DB.

        // Improve the data structure import features.
        //  Will put something in the lower level API, ensure tables / ensure table.
        //   Ensure tables could return / use an observable, so that feedback for each table is provided.

        // An observable would be useful for providing incremental results until the function completes.

        // ensure the db structure of the table_defs

        let obs_ensure_tables = this.ensure_tables(table_defs);
        //console.log('callback', callback);
        //throw 'stop';






        if (callback) {
            obs_ensure_tables.on('next', data => {
                console.log('data', data);

            })
            obs_ensure_tables.on('complete', res_complete => {

                //console.log('* res_complete', res_complete);
                console.log('ensure tables complete');

                // Should have ensured the tables.
                //  And when doing so, should create the table index records in the DB too.




                // then ensure we have the right currencies and markets in the tables.

                this.bittrex_watcher.download_bittrex_structure((err, bittrex_structure) => {
                    if (err) {
                        callback(err);
                    } else {
                        let [at_currencies, at_markets] = bittrex_structure;
                        //  console.log('at_currencies', at_currencies);
                        // Ensure the tables.
                        //  The tables will be given declaratively.
                        // Could have an observable return the results for each item in the loop.
                        //  this.ensure_tables(tables)
                        console.log('have [at_currencies, at_markets]');

                        //throw 'stop';
                        // Use a map function to turn them into the arr kv data
                        //  Though we don't have the keys assigned at the moment.
                        // Then ensure the 
                        this.ensure_at_bittrex_currencies(at_currencies, (err, res_ensure_currencies) => {
                            if (err) {
                                callback(err);
                            } else {
                                console.log('res_ensure_currencies', res_ensure_currencies);
                                // Then ensure the markets.
                                this.ensure_at_bittrex_markets(at_markets, (err, res_ensure_markets) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        //console.log('res_ensure_markets', res_ensure_markets);
                                        //callback(null, [res_ensure_currencies, res_ensure_markets]);
                                        // Then ensure the markets.
                                        callback(null, true);
                                    }
                                })
                            }
                        })

                    }
                })



                //callback(null, res_complete);
            });
        } else {
            return res;
        }
















        // This version won't use the Model??? Components may do so, in order to create and validate rows.


        // load those bittrex tables into the model
        //  including their records

        // Function to get the primary key incrementor value from the db, for a table.
        //  Will look it up from the table name.
        //  Use this to compare with what is in the model
        //   Would need to look at the table record, to view the incrementors.


        /*

            

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
            if (err) {
                callback(err);
            } else {
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
        });
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
            if (err) {
                callback(err);
            } else {
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
            console.log('market_name', market_name);

            this.get_table_record_field_by_index_lookup('bittrex markets', 'id', 'name', market_name, callback);
            //throw 'nyi';
        }
    }

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
            if (err) {
                callback(err);
            } else {

                //var key = [market_name];
                var key = [market_id];
                //console.log('key', key);

                that.get_table_selection_record_count('bittrex market summary snapshots', key, (err, count) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('count', count);

                        //throw('stop');
                        callback(null, count);

                    }
                });
            }
        });
    }

    get_bittrex_market_snapshot_records(market_name, callback) {
        // table selection record count
        //  maybe choose the beginning of the key.
        var that = this;
        //console.log('market_name', market_name);
        this.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
            if (err) {
                callback(err);
            } else {

                //var key = [market_name];
                var key = [market_id];
                console.log('key', key);

                // Did this get deleted too?
                that.get_table_selection_records('bittrex market summary snapshots', key, (err, records) => {
                    if (err) {
                        callback(err);
                    } else {
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

        // If the table does not exist, could raise a callback or resolvable error.
        //  array key value table.
        // check if the table exists.


        // Want a simple function that returns true or false.
        //  See if the table can be found in the index records.

        this.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_field_names) => {
            if (err) {
                callback(err);
            } else {
                //console.trace();
                //throw 'stop';

                that.get_bittrex_market_snapshot_records(market_name, (err, snapshot_records) => {
                    if (err) {
                        callback(err);
                    } else {

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

        var cb_err = function (err) {
            var e = {
                'error': err
            }
            console.trace();
            cb_event(e);
        }

        that.get_table_kp_by_name('bittrex market summary snapshots', (err, table_kp) => {
            if (err) {
                cb_err(err);
            } else {

                that.get_bittrex_market_id_by_name(market_name, (err, market_id) => {
                    if (err) {
                        cb_err(err);
                    } else {
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
    }


    // market name as an array
    //  market id is the array

    get_bittrex_market_snapshots_time_range(arr_market_id, callback) {
        // Need to construct the index range for this.

        //var buf_query = this.model.map_tables['bittrex market summary snapshots'].buf_pk_query([arr_market_id]);
        //console.log('buf_query', buf_query);

        this.get_first_last_table_keys_in_key_selection('bittrex market summary snapshots', [arr_market_id], (err, res_query) => {
            if (err) {
                throw err;
            } else {
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
        var kp = this.model.map_tables['bittrex market summary snapshots'].key_prefix;

        var l = Crypto_Model.Database.encode_key(kp, [arr_market_id, arr_time_range[0]]);
        var u = Crypto_Model.Database.encode_key(kp, [arr_market_id, arr_time_range[1]]);

        this.ll_get_buf_records_in_range(l, u, callback);

    }

    download_save_bittrex_market_time_range_snapshots_by_day(arr_market_id, path, callback) {

        this.get_buf_bittrex_market_snapshots_in_time_range(arr_market_id, (err, buf_res) => {
            // Could take a while...
            if (err) {
                callback(err);
            } else {
                console.log('buf_res', buf_res);
                console.log('buf_res.length', buf_res.length);
            }
        });

    }

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
            if (err) {
                throw err;
            } else {
                var tr = [(new Date(time_range[0])).toISOString(), (new Date(time_range[1])).toISOString()];

                var str_format = 'dddd D MMMM YYYY hh.mma'

                var f_start = date_fns.format(tr[0], str_format);

                var f_end = date_fns.format(tr[1], str_format);

                var tr_msg = market_name + ' ' + f_start + ' to ' + f_end;

                that.get_buf_bittrex_market_snapshots_in_time_range(arr_market_id, time_range, (err, buf_data) => {
                    if (err) {
                        callback(err);
                    } else {
                        ensure_exists(path, (err, exists) => {
                            if (err) {
                                callback(err);
                            } else {
                                var save_path = path + '/' + tr_msg + '.be';
                                console.log('save_path', save_path);

                                var comp_buf = Binary_Encoding.compress_buffer_zlib9(buf_data);
                                console.log('comp_buf.length', comp_buf.length);

                                //fs.writeFile(save_path, buf_data, callback);
                                fs.writeFile(save_path, comp_buf, callback);
                            }
                        });
                    }
                });
            }
        });
    }

    backup_bittrex_data(callback) {
        console.log('begin backup_bittrex_data');

        var friendly_tr = arrayify((time_range) => {
            var tr = [(new Date(time_range[0])).toISOString(), (new Date(time_range[1])).toISOString()];
            var str_format = 'dddd D MMMM YYYY hh:mma'
            var f_start = date_fns.format(tr[0], str_format);
            var f_end = date_fns.format(tr[1], str_format);
            var tr_msg = f_start + ' to ' + f_end;
            var market_name = at_markets.values[0][3];

            return ([market_name, [f_start, f_end],
                [time_range[0], time_range[1]]
            ]);
        });

        var that = this;
        var model = that.model;
        var tbl_bittrex_market_snapshots = model.map_tables['bittrex market summary snapshots'];

        that.new_backup_path('bittrex market summary snapshots', (err, backup_path) => {

            console.log('backup_path', backup_path);

            that.get_bittrex_at_currencies_markets((err, data) => {
                if (err) {
                    callback(err);
                } else {
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
                            fns.push([that, that.get_bittrex_market_snapshots_time_range, [
                                [market_value[0], market_value[1]]
                            ]]);
                        });
                        console.log('getting available asset price time ranges')
                        fns.go(8, (err, arr_time_ranges) => {
                            if (err) {
                                callback(err);
                            } else {
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
            if (err) {
                callback(err);
            } else {
                console.log('akvt_snapshots.length', akvt_snapshots.length);
                //var live_snapshots = new Typed_Arrays_KV_Table(akvt_snapshots.keys, akvt_snapshots.values);

                // Use the Evented version to deal with the live snapshots events.
                //  Subscribe to the incoming events, add them to this live table.

                // Use the evented table, and set up the events here.

                var live_snapshots = new Evented_Float64_KV_Table(akvt_snapshots.keys, akvt_snapshots.values);

                that.subscribe_table_puts('bittrex market summary snapshots', (snapshot_event) => {
                    console.log('snapshot_event', snapshot_event);
                });

                console.log('live_snapshots.length', live_snapshots.length);
            }
        });
    }

    // Connect to an existing DB, and be able to get all Bittrex records easily.

    // Syncing an entire database seems possibly more important / useful though.
    //  Syncing the tables that go below snapshots in structure (currencies, markets)



    // For the moment, need to test getting large record sets in multiple pages.

    // Do want it so that a relatively large dataset can be loaded to RAM quickly.

    // For the moment, copying all rows in a table seems appropriate.
    //  Comparing records in structural tables
    //  Could be done with table hashes.


    // copy_table_from_remote

    // Client could carry out the syncing with get and then put
    //  Or server could do the syncing where it itself gets the data.

    // Telling a server to sync from another, within the config, would be very useful.
    //  This way it would gather all of the table records from the source server.
    //  Would need to do it in the right order.


    // Syncing would have to be a somewhat long, multi-part process.
    //  It should be available within the server module.
    //  Should be done automatically when configured to do so.

    // Need to get the records to be local, and quickly / continually available.


















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



    var config = require('my-config').init({
        path: path.resolve('../../config/config.json') //,
        //env : process.env['NODE_ENV']
        //env : process.env
    });


    let access_token = config.nextleveldb_access.root[0];


    var server_data3 = config.nextleveldb_connections.data3;
    server_data3.access_token = access_token;
    //var server_data1 = config.nextleveldb_connections.localhost;

    // The table field (for info on the fields themselves) rows are wrong on the remote database which has got approx 12 days of data.
    //  Can still extract the data, I expect.

    // Don't want to replace the code on the server quite yet.

    // May be possible to edit the fields, possibly validate the fields?

    var client = new Assets_Client(server_data3);

    //var client = new Assets_Client(local_info);

    client.start((err, res_start) => {
        if (err) {
            throw err;
        } else {
            console.log('Assets Client connected to', server_data3);


            // 22/03/2018 - Nice to see this still works.
            //  Could retrieve data as more directly encoded TypedArray recordsets.
            //   Would not retrieve them as records, but using Binary_Encoding, with Binary_Encoding more advanced than it is now.





            var test_get_btc_eth_snapshot_records = () => {

                // If the market snapshots table is not there, we could return an empty array, or raise an error.
                // could first test if the bittrex market snapshots table exists.
                // At a different part, we can ensure the Bittrex data structures.

                // get_akvt_bittrex_market_snapshot_records
                client.get_akvt_bittrex_market_snapshot_records('BTC-ETH', (err, recordset) => {
                    if (err) {
                        // Table not found: ...
                        console.log('Probably: Table was not found.')
                        throw err;
                    } else {
                        console.log('BTC-ETH', recordset.keys);
                        console.log('BTC-ETH', recordset.length);

                        var timed_prices = recordset.flatten(['timestamp', 'last']);
                        console.log('timed_prices', timed_prices);
                        console.log('timed_prices.length', timed_prices.length);


                    }
                });
            }
            //test_get_btc_eth_snapshot_records();

            // get all snapshot records

            let test_get_bittrex_snapshot_records = () => {
                // Paged

                // Get all the table records, paged.
                let decode = true;

                // If we do use compression, that option could be hidden from here, and a sensible default used, or it be able to be changed elsewhere dynamically.
                //  Just maybe further options would be enabled in the fn call, but it's not the best way.

                let table_name = 'bittrex market summary snapshots';

                // Observable count table records, with delay paging would be cool.
                //  Gives the count update every second (or so).

                // A count can (unfortunately) take a few minutes.
                //  Could have a db option of updating counts upon (successful) completion of operations.
                //   Would be more complexity at more places.

                // Giving progress for the count through observables would be great.
                //  Would help to show that server1 works properly when the db gets restarted there.

                // Definitely want compressed data being sent fairly soon, but observable count makes sense.






                let use_cb_count = () => {
                    client.count_table_records(table_name, (err, count) => {
                        if (err) {
                            throw err;
                        } else {
                            console.log('count', count);

                            let obs_table_records = client.get_table_records(table_name, decode);

                            let page_number = 0;
                            obs_table_records.on('next', data => {
                                console.log('data.length', data.length);
                                console.log('page_number', page_number++);
                            });
                            obs_table_records.on('complete', last_data_page => {
                                //console.log('data', data);
                                console.log('complete');
                            });
                        }
                    })
                }

                let use_obs_count = () => {
                    let obs_count = client.count_table_records(table_name);
                    obs_count.on('next', count => {
                        console.log('count', count);
                    })
                    obs_count.on('complete', count => {
                        console.log('count complete', count);
                    })


                }
                use_obs_count();










                //client.get_
            }
            test_get_bittrex_snapshot_records();


            // 

            // get these records as an arr-table.
            // get both the keys and values.

            // Could get the snapshots as an Array_Table.
            //  Array_Table could have num_keys or num_key_fields to help the records get encoded into the db.

            var test_get_snapshot_fields = () => {


                // get the fields just as names

                client.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_fields) => {
                    if (err) {
                        throw (err);
                    } else {
                        //console.log('kv_fields', JSON.stringify(kv_fields, null, 2));
                        //console.log('kv_fields', (kv_fields[0]));
                        //console.log('kv_fields', (kv_fields[1]));


                        client.get_table_field_names('bittrex market summary snapshots', (err, fields) => {
                            if (err) {
                                throw (err);
                            } else {
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
                    if (err) {
                        throw err;
                    } else {
                        //console.log('at_currencies', at_currencies);

                        var currency_names = at_currencies.get_arr_field_values('Currency');
                        console.log('currency_names', currency_names);


                        client.get_at_bittrex_markets((err, at_markets) => {
                            if (err) {
                                throw err;
                            } else {
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

            let do_backup = () => {
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
                            if (err) {
                                console.trace();
                                throw err;
                            } else {
                                console.log('res_backup', res_backup);


                            }
                        });

                    }
                });
            }

            // get the fields for the table.
            //  Probably best to read this out of the model, server side.
            //  Though more advanced model usage on the client could mean no need to call the server.
            //   Matching server-side functionality would be useful too.
            //    Name, type
            //    Info about any references the fields have - what table it refers to (id, name), the id within the table of the reference, the data types of those fields.

            // Easy to use info on the fields would help data transformations.
            //  Could just tell it data formats it may be given, it figures out how to transform them.

            // Seems like it is worth more fully making and testing the client and server functionality, carefully deploying it to a few servers, then seeing about copying over data from 
            //  data1, which is running right now.

            // Very significant changes have been made, with a vastly expanded API.



            // 

            // get_table_field_info should be enabled on the server.
            //  Be able to give the table by id or by name.



            /*

            client.get_table_field_info('bittrex market summary snapshots', (err, fields) => {
                if (err) {
                    throw err;
                } else {
                    console.log('fields', fields);
                }
            });

            */

            // Can run tests to see 



            /*

            client.get_bittrex_currency_id_by_code('ETH', (err, res_eth_id) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_eth_id', res_eth_id);
                }
            });
            */

            /*

            client.ensure_bittrex_structure_current((err, res_structure) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_structure', res_structure);
                }
            })
            */
        }
    });
} else {
    //console.log('required as a module');
}
module.exports = Assets_Client;