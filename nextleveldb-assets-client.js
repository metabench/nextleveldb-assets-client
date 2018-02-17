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


// If we do ensure data in the model, we just call a function in the model to do so.



const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;
const is_array = lang.is_array;

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


const xas2 = require('xas2');

const Arr_KV_Table = require('arr-kv-table');

const Float64_KV_Table = require('float64-kv-table');
const Evented_Float64_KV_Table = require('float64-kv-table');

// This typed arrays kv table is going to store quite a lot of data as 64 bit floats to begin with.
//  Want to handle satoshi conversion and types quite soon.



//const Typed_Arrays_KV_Table = require('typed-arrays-kv-table');

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

        // bittrex markets


    }

    // Ensuring bittrex structure is proving to be quite combersome.
    //  Future exchanges should probably be easier, plus should be able to create some abstractions to deal with exchanges or data providers.


    // Exchange markets, exchange currencies, exchange trades, exchange order books, exchange order book modifications
    //  Would be excellent to get this for a variety of exchanges.

    // Don't know exactly how many MB / day it would take. Could be quite intensive on storage.
    //  Moving data off the cloud servers to historical data pages makes sense.
    //  Could have a bunch of files with the data encoded, ready to be loaded into a db or other data structure.
    //   Could be available on an Amazon or equivalent db or file storage system.


    // The next stage is to make sure that the currencies can be put in the DB OK
    //  Checking which are to be added before adding them helps debugging and resiliance.

    // Will then get it to automatically update the bittrex markets.

    // Then want to get it running on a server, or a few.


    // Having crypto-data-collector able to run its own db process would be useful too.
    //  The data collector should be able to accsess and store a variety of data from different sources.
    //  Possibly that would also mean that by default it has some UI capabilities, later on?


    // download ensure bittrex currencies
    //  This would index the currencies as well.


    // download ensure bittrex markets

    // the market id basically is the two currency ids together.

    // Could get the record at some stages to test if it exists.

    get_bittrex_market_id_by_currency_ids(arr_currency_pair_ids, callback) {
        // The bittrex markets are indexed according to their currency pairs.
        //  It's actually their primary key.




    }


    // Currency lookup function.

    get_bittrex_currency_id_by_code(currency_code, callback) {
        // refers to the index.

        // could use a lower level index lookup function.

        // kp, index id, the currency code.

        //console.log('currency_code', currency_code);

        this.get_table_id_by_name('bittrex currencies', (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                // compose the index

                let kp = table_id * 2 + 2;
                let ipk = kp + 1;


                var arr_buf = [xas2(ipk).buffer, xas2(0).buffer, flexi_encode_item(currency_code)];
                let buf = Buffer.concat(arr_buf);

                //console.log('buf', buf);

                this.ll_get_records_by_key_prefix(buf, (err, res_records) => {
                    if (err) {
                        throw err;
                    } else {
                        //console.log('res_records', res_records);

                        let decoded = Model.Database.decode_model_rows(res_records, 2);
                        //console.log('decoded', decoded);

                        if (decoded.length > 0) {
                            let found_index_row = decoded[0];
                            //console.log('found_index_row', found_index_row);

                            let currency_id = found_index_row[0][1];
                            callback(null, currency_id);
                        } else {
                            callback(null, undefined);
                        }


                        //this.
                        //Model.Database.

                        // Can this work where it decodes the index records?
                    }
                });
            }
        })
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



    put_bittrex_currency(arr_bittrex_currency, callback) {
        console.log('put_bittrex_currency', arr_bittrex_currency);

        // Maybe validate the record to go in against the Model.
        //  We don't already have the key to these records.

        // Using the Model at some stage could be useful. It's got pk incrementor values.

        // Using a lower level table_pk_increment function could do the job, where the incrementor values in the DB are updated.


        // do this using the Model for the moment.
        let tbl_bittrex_currencies = this.model.map_tables['bittrex currencies'];
        // For some reasons, the pk incrementor has not been set up properly in the model.
        //  Its value is at 0.
        //  Value should be appropriately high.


        console.log('tbl_bittrex_currencies.pk_incrementor', tbl_bittrex_currencies.pk_incrementor);

        // Then use add records to this.
        let new_record = tbl_bittrex_currencies.add_record(arr_bittrex_currency);
        console.log('new_record', new_record);

        console.trace();
        throw 'stop';




    }


    ensure_arr_bittrex_currency(arr_bittrex_currency, callback) {
        // May need to change the format of the currency arr.

        // kv [[], []]
        // flat []

        // if there is no callback, return a promise.


        // Could have an inner function that gets called / used differently depending on if there is a promise or not.



        let a = arguments,
            l = a.length;
        //console.log('l', l);
        //console.log('arr_bittrex_currency', arr_bittrex_currency);

        // get the currency by name
        let that = this;

        let inner = (callback) => {
            let code = arr_bittrex_currency[0];
            this.get_bittrex_currency_id_by_code(code, (err, currency_id) => {
                if (err) {
                    callback(err);
                } else {
                    //console.log('currency_id', currency_id);
                    if (typeof currency_id === 'undefined') {
                        // it's not already in the DB.
                        //  add the currency record to the DB.

                        that.put_bittrex_currency(arr_bittrex_currency, (err, res_put) => {
                            if (err) {
                                callback(err);
                            } else {
                                console.log('res_put', res_put);
                                callback(null, res_put);
                            }
                        });

                    } else {
                        // if it's not given a callback then return a promise.
                        // But maybe return the promise before this executes, and have the promise called at this stage.


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
                    //console.log('market_currency_ids', market_currency_ids);

                    // Then check for the market record with those currency ids.

                    // could do a count by key.

                    // could put the buffer key prefix together.

                    // get_table_records_by_key
                    //  a bit like searching by key prefix, except the system sets the key prefix according to the table, then we give it the key.

                    //this.get_table_records_by_key('bittrex markets', [market_currency_ids], (err, res_records) => {

                    /*
                    this.get_table_keys('bittrex markets', (err, keys) => {
                        console.log('keys', keys);
                        throw 'stop';
                    })
                    */

                    // Get single record.

                    // getting the key beginning does not work with that key itself.
                    //  only with key_beginning...

                    // Need just the key itself looked up when doing key beginning.

                    // get_table_record_by_key

                    // looks up the table kp.
                    //  

                    this.get_table_record('bittrex markets', market_currency_ids, (err, res_record) => {
                        if (err) {
                            callback(err);
                        } else {
                            //console.log('res_record', res_record);
                            //console.log('res_records.length', res_records.length);

                            if (res_record) {
                                callback(null, res_record);
                            } else {
                                console.trace();
                                throw 'not found';
                            }
                        }
                    })

                    /*
                    this.get_table_records_by_key_beginning('bittrex markets', market_currency_ids, (err, res_records) => {
                        if (err) {
                            callback(err);
                        } else {
                            console.log('res_records', res_records);
                            console.log('res_records.length', res_records.length);


                        }
                    })
                    */




                    //this.get_record

                    //this.ll_get_records_by_key_prefix()


                    // check if the record exists?

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
        // This version won't use the Model??? Components may do so, in order to create and validate rows.


        // load those bittrex tables into the model
        //  including their records

        // Function to get the primary key incrementor value from the db, for a table.
        //  Will look it up from the table name.
        //  Use this to compare with what is in the model
        //   Would need to look at the table record, to view the incrementors.








        this.bittrex_watcher.download_bittrex_structure((err, bittrex_structure) => {
            if (err) {
                callback(err);
            } else {
                let [at_currencies, at_markets] = bittrex_structure;
                //console.log('at_currencies', at_currencies);

                // Use a map function to turn them into the arr kv data
                //  Though we don't have the keys assigned at the moment.

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

                                callback(null, [res_ensure_currencies, res_ensure_markets]);
                                // Then ensure the markets.


                            }
                        })





                    }
                })




            }
        })

    }

    _ensure_bittrex_structure_current(callback) {


        // .load_core_plus_tables(['bittrex markets', 'bittrex currencies']

        // Assume the core is loaded?
        //  No harm in reloading it?
        //let p_bw = promisify(this.bittrex_watcher);

        let bw = this.bittrex_watcher;
        let that = this;

        this.load_core_plus_tables(['bittrex currencies', 'bittrex markets'], (err, model) => {
            if (err) {
                callback(err);
            } else {
                // Get the currencies and markets (together) from bittrex-watcher
                // Check that each of the currencies has a record in the DB.
                //  Could use the model for this.
                //  However, directly checking in the DB itself may be better.

                // Seems to be how Bluebird promisify works.
                let p_bittrex_downloaded_structure = promisify(bw.download_bittrex_structure, {
                    context: bw
                });

                // using await and destructuring?
                // download_bittrex_structure returns [at_currencies, at_markets]
                //  or it should do.

                console.log('p_bittrex_downloaded_structure', p_bittrex_downloaded_structure);

                p_bittrex_downloaded_structure().then(([at_currencies, at_markets]) => {
                    console.log('at_currencies', at_currencies);
                    console.log('at_markets', at_markets);

                    //console.log('at_currencies.length', at_currencies.length);
                    //console.log('at_markets.length', at_markets.length);


                    // Here it may be worth doing checks to see if we have those records.

                    // client.check_records
                    //  would check those records exist on the server.
                    //   we may not know what the assigned key is.
                    //   so it may check the records against the fields that the records are indexed by.

                    // Also want a systematic way to transform records.
                    //console.log('at_currencies.keys', at_currencies.keys);

                    // So we can use just one field from this array_table, 'Currency', and then search by indexed value within the DB.

                    //var currency_0 = at_currencies.values[0];
                    //console.log('currency_0', currency_0);

                    // then try to ensure that bittrex currency.
                    //  or do the index check on it.

                    /*
                    that.check_table_record_index_lookup('bittrex currencies', currency_0, (err, res_lookup) => {
                        if (err) { callback(err); } else {
                            console.log('res_lookup', res_lookup);
                        }
                    });
                    */

                    that.count_table_index_records('bittrex currencies', (err, count) => {
                        console.log('count', count);

                        // so seems like the records have been indexed :)

                        // Will still need to automate server-side indexing.

                        // A bit of coding to do with input => output transformations and pipelines.
                        //  Want it so that we can easily define (and save?) functions of mappings between data as it arrives from an API such as bittrex,
                        //  and the DB's records. It could involve mapping

                        // We could do the lookup on all the currencies, and see which of them are within the system.

                        // Could do a lookup on all the currency data.
                        //  Would be interesting to send it to the server...
                        //   But possibly the server would need to know more about adding keys.
                        //   Or it could be told that the data is arriving without keys.

                        // Don't want particularly complex rules for adding the keys to data
                        // Or modifying the keys, normalising data by looking up what's there.

                        // that.check_table_records_exist
                        //  it would do that using the index lookup.

                        // Will need a bit of extra handling for how pks could have 2 fields, and dealing with their references, treating them as a single value.

                        // that.check_table_records_exist would return false for the records that don't exist.

                        // It could be changed to use upgraded server capabilities.
                        //  The server still is not adding records with automatic indexing.

                        // Would be worth having the server index these records when they get added - will use a different function, not the ll_put, to add these records.
                        //  Possibly calling it INSERT would be better.

                        // Ensuring records meaning insert, no overwrite, and returning the keys to the records.

                        // The markets need to have concise keys, so that the trade data is small too.
                        //  Don't want to require a lot of space to refer to which market some data refers to.


                        // Before long, will be worth taking daily backups of whatever data gets generated by this system.
                        //console.log('pre check_table_records_exist');

                        // ensure_arr_bittrex_currencies



                        that.check_table_records_exist('bittrex currencies', at_currencies.values, (err, res_records_exist) => {
                            if (err) {
                                callback(err);
                            } else {
                                console.log('res_records_exist', JSON.stringify(res_records_exist));

                                // then for the currency records that don't exist, we add them.


                                // An INSERT_RECORD function could be useful for this rather than lower level db put.
                                //  It would also insert the relevant index records.
                                //  This would be done using a server-side model.

                                // Very soon, need to have a replicated and backed up bittrex data system.
                                // Need to verify the data is correct.
                                // Expand to other exchanges.

                                // Get the streaming bittrex data working too.

                                // Its proving a very longwinded process to get some data updated and normalised.
                                //  Seems necessary to solve the general programming case of doing it, not just for this specific bittrex data, while still having bittrex specific code.


                                // Smaller asset client systems would make sense.
                                //  They could use some general purpose functionality from here.
                                //  Also should have a table of the exchanges / data providers.
                                //  A users table seems very important for the database too.
                                //   Though its possible that a separate authentication database could be used.

                                let missing_records = [];

                                let missing_record_arr_indexes = [];
                                res_records_exist.forEach((item, i) => {
                                    if (item === false) {
                                        missing_record_arr_indexes.push(i);
                                        missing_records.push(at_currencies.values[i]);
                                    }
                                });
                                console.log('missing_record_arr_indexes', missing_record_arr_indexes);
                                console.log('missing_records', missing_records);

                                //throw 'stop';

                                // then insert the missing records



                                let ensure_bittrex_markets = () => {
                                    console.log('ensure_bittrex_markets');

                                    // Need the map of currencies by id.
                                    //  May be worth getting this anew from the database.

                                    //  Possibly doing an index lookup in the DB would be best.

                                    // Get the records, map them with a function.
                                    //  Get the records, construct a map object by iterating them, and putting them to the map obj.

                                    // get_table_records_obj_map_by_field_value.
                                    //  

                                    that.get_table_records_fields_value_map('bittrex currencies', 1, ((err, map_currencies_by_code) => {
                                        if (err) {
                                            callback(err);
                                        } else {
                                            console.log('map_currencies_by_code', map_currencies_by_code);

                                            var nxt = map_currencies_by_code['NXT'];
                                            var ukg = map_currencies_by_code['UKG'];


                                            console.log('nxt', nxt);
                                            console.log('ukg', ukg);

                                            //throw 'stop';





                                            // Check the market records exist

                                            // The market records get transformed somewhat.
                                            //console.log('at_markets.values', at_markets.values);

                                            // what is the market record def in the DB / model?
                                            //  Get the table field info from the db.

                                            //let markets
                                            let arr_record_data = at_markets.values.map(item => {
                                                console.log('item', item);
                                                let arr_item_res = item.slice();
                                                //let key = item[0], value = item[1];
                                                //console.log('key', key);
                                                //console.log('key[0]', key[0]);
                                                arr_item_res[0] = map_currencies_by_code[item[0]][0][0];
                                                arr_item_res[1] = map_currencies_by_code[item[1]][0][0];
                                                return arr_item_res;
                                            });

                                            console.log('arr_record_data', arr_record_data);

                                            // Probably needs to put the records into the same format somehow.

                                            that.check_table_records_exist('bittrex markets', arr_record_data, (err, res_records_exist) => {
                                                if (err) {
                                                    callback(err);
                                                } else {
                                                    console.log('res_records_exist', res_records_exist);

                                                    // Then find any records that don't exist.
                                                    //  The records exist function could separate out the records that don't exist from the ones that do.

                                                    // We are looking for their keys, 


                                                    throw 'stop';
                                                }
                                            });


                                            // Then check that these records are in the database.
                                        }
                                    }))
                                }

                                if (missing_records.length > 0) {
                                    that.insert_table_record('bittrex currencies', missing_records[0], (err, res_insert) => {
                                        if (err) {
                                            callback(err);
                                        } else {

                                            console.log('res_insert', res_insert);
                                            // Make sure the bittrex markets are within the DB.

                                            // The inserting of records would also assign them a new primary key if one is not given.
                                            //  Want there to be some intelligence on the DB side, client side to begin with, about how to insert a record
                                            //  that is given as an array, when:
                                            //   We may not have the primary key
                                            //   (The primary key may need some translation / normalisation.)

                                            // With bittrex markets, it is most likely that we would construct / reconstruct the primary keys?
                                            //  Or have a feature on the client or server, or both, that can normalise / translate fields.
                                            //   We can give it a translation schema, and it uses that to lookup the values within a primary key.

                                            // Really need to get these servers back online, and able to stream real-time data to clients.
                                            //  May actually take a while longer working on it.

                                            // Want to have a local client that also collects data from remote clients, backing it up too.

                                            // The silverstone PC may prove useful for such a long-running process. It could save the backups to HD and over the network every day.

                                            // Want to have it very easy to start up a node using code that's available through NPM and github.


                                            // Crypto data collector could become a more advanced app that by default runs a server node that makes the data available over the network.



                                            // Need the info about the bittrex currencies to ensure the markets have been properly added.

                                            ensure_bittrex_markets();
                                            //throw 'stop';
                                        }
                                    })
                                } else {
                                    ensure_bittrex_markets();
                                }
                            }
                        });

                    });
                })
            }
        })
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
        this.get_table_kv_field_names('bittrex market summary snapshots', (err, kv_field_names) => {
            if (err) {
                callback(err);
            } else {
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
            if (err) {
                callback(err);
            } else {
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
            if (err) {
                throw err;
            } else {
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
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('buf_data', buf_data);
                        //console.log('market_name', market_name);

                        //var save_dir_path = 

                        ensure_exists(path, (err, exists) => {
                            if (err) {
                                callback(err);
                            } else {
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



    var server_data1 = config.nextleveldb_connections.data1;
    //var server_data1 = config.nextleveldb_connections.localhost;

    // The table field (for info on the fields themselves) rows are wrong on the remote database which has got approx 12 days of data.
    //  Can still extract the data, I expect.

    // Don't want to replace the code on the server quite yet.

    // May be possible to edit the fields, possibly validate the fields?

    var client = new Assets_Client(local_info);

    client.start((err, res_start) => {
        if (err) {
            throw err;
        } else {
            console.log('Assets Client connected to', server_data1);

            var test_get_btc_eth_snapshot_records = () => {

                // get_akvt_bittrex_market_snapshot_records
                client.get_akvt_bittrex_market_snapshot_records('BTC-ETH', (err, recordset) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('BTC-ETH', recordset.keys);
                        console.log('BTC-ETH', recordset.length);

                        var timed_prices = recordset.flatten(['timestamp', 'last']);
                        console.log('timed_prices', timed_prices);

                    }
                });
            }
            //test_get_btc_eth_snapshot_records();

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

            /*

            client.get_bittrex_currency_id_by_code('ETH', (err, res_eth_id) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_eth_id', res_eth_id);
                }
            })

            */

            client.ensure_bittrex_structure_current((err, res_structure) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_structure', res_structure);
                }
            })

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