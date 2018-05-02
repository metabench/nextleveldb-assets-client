const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;
const is_array = lang.is_array;
const Evented_Class = lang.Evented_Class;
const get_a_sig = lang.get_a_sig;
const clone = lang.clone;

const Model = require('nextleveldb-model');
const database_encoding = Model.encoding;

const Record_List = Model.Record_List;
const get_truth_map_from_arr = lang.get_truth_map_from_arr;

const xas2 = require('xas2');

const Arr_KV_Table = require('arr-kv-table');

const Float64_KV_Table = require('float64-kv-table');
const Evented_Float64_KV_Table = require('float64-kv-table');
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
//  Still looks like there is a problem with id incrementor on inc_bittrex currencies_id

// Whenever it adds a new currency record, assigning it an id, and puts that data into the DB. it's got to update the incrementor.

// A function to put a record into the db, where it knows it has an autoincrementing primary key.
//  It does not get 
// Seems quite likely that previous currency data did not get added properly, or that it even overwrote another currency.

// With autoincrementing tables, generally the incrementor should be set to the value of the highest pk + 1.





// This could do with repair mode.
//  It seems as though some bittrex currencies were not added successfully (at all), then their markets were added with broken keys

// Check all bittrex currency records against the actual bittrex currencies
//  Remplacement records / new records
// Check the bittrex markets
//  Do any have broken keys
//   How many records refer to those broken keys?
// Put new currency and market records in place, while updating references to broken ones.

//  Maybe the broken keys hav been reused for different things though.
//   Possibly some of the snapshot records have been corrupted quite a lot more.
//    Could stop if there is more than one currency which was not added.
//    Would need cross-referencing to put these records back correctly.

// Check for pk-fk references to malformed keys?

// .repair_bittrex
//  will get the rows with bad indexes from bittrex markets
//   will check to see if their currencies are in the system / ensure they are
//   note down what the bad indexes are.
//  look for any rows that refer to them

// scan db for all records that have got malformed keys, including referring to a malformed key

// Try getting data8 verified to refer to the correct data.



// pre_repair_scan
//  in-depth, looks for market records with bad indexes
//   looks for the currencies they refer to

// Also, checking of corrupt index records.
//  Check that each index record refers to an actual record

// Worth scanning for quite a lot of problems before fixing them.
//  The problems can overlap, need an action plan.

// This has got complicated, but I see no better way than to get this working.
//  Want the live and historical data object containing the bittrex data.

// Running the data recovery on older servers seems important in some ways.
//  Data recovery even seems like it could be a 1 week project.
//  At least recently it seems like it has stopped overwriting markets.

// Getting data8 back in shape seems to be a priority.
//  Could quite possibly do this within a day, or a few hours at least.

// Validation of all records would be quite a long-running operation.
//  May be worthwhile in many cases.



// The malformed bittrex market records could be indexed too.
//  Finding malformed bittrex market records and currency records seems like the way.


// more specific operations
//  malformed bittrex currencies keys
//   

// Once we find mal-formed records, we could search for all records that refer to them.
//  Also, index records that reference a mal-formed key.
//   Index records don't have a value themselves, we need to read the record and see what it refers to.

// Ideally, find a binch or records relating to one problem, and fix them all together.


// table_get_key_range_referring_to
// get records_referring_to




// Going with checking for broken keys seems the right way.
//  Looking for all records that refer to that broken key.
//   The index record would refer to the broken key as well.

// What about a GUI for modifying records / making changes?


// For the moment, will fix the current Bittrex problems.

// Also, making a list here of what some lower Bittrex ids should be would make sense.


// So if one of these is not correct then we have identified a problem.



let map_list_correct_bittrex_currency_ids = {
    '0': 'BTC',
    '1': 'LTC',
    '2': 'DOGE',
    '3': 'VTC',
    '4': 'PPC',
    '5': 'FTC',
    '6': 'RDD',
    '7': 'NXT',
    '8': 'DASH',
    '9': 'POT',
    '10': 'BLK',
    '11': 'EMC2',
    '12': 'XMY',
    '13': 'AUR',
    '14': 'EFL',
    '15': 'GLD',
    '16': 'FAIR'
}




// Missing low id bittrex coins - check for them












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

    // Will be extended to handle the current (20/04/2018) problems with what has already been written to databases.
    //  Will enable moving onto the next stage - may eventually find problems are not resolvable at present, and back up the database.
    //  May well require changing and remaking various functions to use newer objects, testing them too.



    // This is all client-side. Essentially need to hack the DBs into working forms.
    //  Will try to do some error fixes in generalisable ways.
    //  

    // problem diagnosis on db

    // full scan
    constructor(spec) {
        super(spec);
        // maybe best not to set the model like this.
        //  may be best to load the model from remote.
        this.model = new Model.Database();
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
                console.log('arr_bittrex_currency', arr_bittrex_currency);
                let res_ensure = await this.ensure_arr_bittrex_currency(arr_bittrex_currency);
                console.log('res_ensure', res_ensure);
            }
        }
        go().catch(err => {
            console.trace();
            throw err;
        }).then(res => {
            //console.log('res', res);
            //console.log('then ensure_at_bittrex_currencies');
            callback(null, true);
        });
    }

    // ensure_at_bittrex_markets
    ensure_at_bittrex_markets(at_bittrex_markets, callback) {
        // iterate through the bittrex currencies.
        let go = async () => {
            //console.log('at_bittrex_markets.values.length', at_bittrex_markets.values.length);
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

    get_bittrex_currency_codes(callback) {
        let obs_bittrex_currencies = this.get_table_records('bittrex currencies');
        obs_bittrex_currencies.unpaged = true;
        // Then need to take care while decoding them, some currency records are encoded wrong.

        // leave them out because we only want valid ones here.
        let codes = [];

        obs_bittrex_currencies.on('next', data => {
            //console.log('obs_bittrex_currencies data', data);
            codes.push(data[1][0]);
        })
        obs_bittrex_currencies.on('complete', () => {
            callback(null, codes);
        })

    }

    get_bittrex_currencies_map_by_id(callback) {
        let obs_bittrex_currencies = this.get_table_records('bittrex currencies');
        obs_bittrex_currencies.unpaged = true;
        // Then need to take care while decoding them, some currency records are encoded wrong.

        // leave them out because we only want valid ones here.
        //let codes = [];
        let res = {};

        obs_bittrex_currencies.on('next', data => {
            //console.log('obs_bittrex_currencies data', data);

            //console.log('data', data);

            let id = data[0][1];
            res[id] = data;

            // may need to split the data.
            //  not ideal that it does not unpage.



            //throw 'stop';


            //codes.push(data[1][0]);

        })
        obs_bittrex_currencies.on('complete', () => {
            callback(null, res);
        })

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
        console.trace();
        throw 'stop';

        // But this overwrites the last one.
        //  Not sure we have loaded the right incrementor values into the model upon start of the assets client.
        //   Need to check.




        // Has the side effect of maybe changing an incementor value.
        //console.log('new_record', new_record);

        //console.trace();
        //throw 'stop';

        // The client should be able to put a model record.
        //  That would use a batch put operation that also puts the index values into place.

        // What about incrementors?

        // Option to set the incrementor value?
        //  The relevant table would need to have its incrementor value updated.

        //  Put the record, and update the table's incrementor value in the db.

        // We may have lost data or data reliability. Possibility of records having got in there with the wrong currency and / or market.

        // Updating the db's incrementor from the model when putting a record makes a lot of sense.

        // update_incrementor_value

        // Update the incrementor value from the model.
        //  Using a less low level interface would help at times.



        this.put_model_record(new_record, callback); // This does put the index records too.



        // then put_model_record.
        //  It would use the model to generate the index values, and then put them into the DB.

    }

    // does full table scan right now.
    //  maybe change fn name because this is done for error correction.

    // Rename this to indicate full table scan

    get_bittrex_currency_by_code(code) {
        // A promise would be better in general.

        // could callbackify an inner promise.

        // and do checks to see if the last function is a callback

        let fn_sig_pr_cb = (a, fn) => {
            //let last_a = a[a.length - 1]
            // ? operator instead would be better
            let args;
            // If we have given a callback

            let callback;

            if (typeof a[a.length - 1] === 'function') {
                args = Array.prototype.slice.apply(a, 0, a.length - 1);
                callback = a[a.length - 1];

            } else {
                args = a;
            }
            let sig = get_a_sig(args);

            if (callback) {
                fn(args, sig, (res) => {
                    callback(null, res);
                }, (err) => {
                    callback(null, err);
                })
            } else {
                let res = new Promise((resolve, reject) => {
                    fn(args, sig, (res) => {
                        resolve(res);
                    }, (err) => {
                        reject(err);
                    })
                });
                return res;
            }


        }

        let res = fn_sig_pr_cb(arguments, (a, sig, done, reject) => {
            console.log('get_bittrex_currency_by_code inner sig', sig);

            // do the table lookup.
            // right now, get all the table records.

            let obs_currencies_records = this.get_table_records('bittrex currencies');
            obs_currencies_records.decoded = true;
            obs_currencies_records.unpaged = true;

            // Make the observer result have a .stop function.

            // Possibly should create a new bittrex model to compare to the current bittrex tables.
            //  That would help us identify what should be at earlier record values.


            // To veriy that this won't happen in the future, will check on incrementor values.
            //  Very close now to having a decently working db system....


            let found_item;
            obs_currencies_records.on('next', record => {
                //console.log('record', record);
                let record_code = record[1][0];

                if (code === record_code) {
                    // we have found it.   

                    // 
                    console.log('found record', record);
                    found_item = record;
                    // Seems like it's overwritten Bitcoin's record.

                    // 

                    // Move it towards the end, then ensure the Bitcoin record at position 0.
                    //  So this coin has been given an index of 0.
                    //   That would mess up with the consistency

                    // It seems now like a CockroachDB deployed a little while back would be faster to get working.



                    //obs_currencies_records.stop();
                    done(record);
                }
            });
            obs_currencies_records.on('complete', () => {
                // not found
                //reject();

                if (found_item) {
                    done(found_item);
                } else {
                    reject;
                }
            });
        });

        if (res) {
            return res;
        }


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

            // This does an index lookup.
            //  With repairing, best to fix the indexes (on all indexed records?) to start with.

            // A procedure to go through the database, checking for any index records that do not correctly refer to a record.
            //  Will need to check that the record's index value corresponds to that index data.

            // Seems like a somewhat complex checking procedure, but it will solve the wrong lookups of coins.
            //  It looks like a record was overwritten because the incrementor was wrong (or not adding 1), and a new index record was made too.

            // Seems like potentially quite a bit more work to get the data back out of these databases, while correcting the structural records.

            // Will be best to also get the lower resolution historic data for checking as the data gets put into place in the corrected database.
            //  May be worth running a DB with a limited number of currencies for testing purposes.

            // CockroachDB may still be the best option.
            //  However would still require a fair bit of code to wrap it, but core db is proven to be reliable.

            // Could have a reindex_table function.
            //  Or 2-way where it goes through all index records, checking they are correct
            //   Then it goes through the table records, checking they have correct index records.













            this.get_bittrex_currency_id_by_code(code, (err, currency_id) => {
                if (err) {
                    callback(err);
                } else {
                    console.log('found currency_id', currency_id);
                    if (typeof currency_id === 'undefined') {
                        // it's not already in the DB.
                        //  add the currency record to the DB.

                        // Could check the index value is correct here.

                        this.put_bittrex_currency(arr_bittrex_currency, (err, res_put) => {
                            if (err) {
                                callback(err);
                            } else {
                                //console.log('put_bittrex_currency res_put', res_put);
                                callback(null, res_put);
                            }
                        });
                    } else {
                        callback(null, currency_id);
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
                                        //console.log('put_bittrex_market res_put', res_put);
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

    ensure_bittrex_currencies(callback) {
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
                //console.log('have [at_currencies, at_markets]');

                //throw 'stop';
                // Use a map function to turn them into the arr kv data
                //  Though we don't have the keys assigned at the moment.
                // Then ensure the 
                this.ensure_at_bittrex_currencies(at_currencies, (err, res_ensure_currencies) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('res_ensure_currencies', res_ensure_currencies);
                        // Then ensure the markets.
                        callback(null, true);
                    }
                })

            }
        })
    }

    // 

    ensure_bittrex_structure_current(callback) {

        console.log('ensure_bittrex_structure_current');
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
                //console.log('* data', data);

            })
            obs_ensure_tables.on('complete', res_complete => {

                //console.log('* res_complete', res_complete);
                //console.log('ensure tables complete');

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
                        //console.log('have [at_currencies, at_markets]');

                        //throw 'stop';
                        // Use a map function to turn them into the arr kv data
                        //  Though we don't have the keys assigned at the moment.
                        // Then ensure the 
                        this.ensure_at_bittrex_currencies(at_currencies, (err, res_ensure_currencies) => {
                            if (err) {
                                callback(err);
                            } else {
                                //console.log('res_ensure_currencies', res_ensure_currencies);
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

        var l = Model.Database.encoding.encode_key(kp, [arr_market_id, arr_time_range[0]]);
        var u = Model.Database.encoding.encode_key(kp, [arr_market_id, arr_time_range[1]]);

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

    diagnose_bittrex_currency(currency_code) {
        // 

        // And this is a promise, not a normal observable.



        // would be worth scanning a table for anything matching this code.

        let pr_res = new Promise((resolve, reject) => {
            let pr_get = this.get_bittrex_currency_by_code(currency_code);



            pr_get.then(res => {
                console.log('pr_get res', res);

                resolve(res);


                // 


                // if it's 0...
                //  or compare it to a map of what it should actually be.
                //  

                //let wrong_coin_id = res[0][1]; // after kp




                /*

                let indexes = this.model.get_idx_records_by_record(res);
                //console.log('indexes', indexes);

                // Then check for these index records.
                //  get the records by those given keys.

                // Get an index record by key...

                let fns = Fns();

                // [ [ 13, 0, 'PRO', 0 ], [ 13, 1, 'Propy', 0 ] ]

                // OK, so are these records indexed after all?
                //  Why is the index lookup not working?

                // A server-side delete record by key that also deletes the index records would be helpful.
                //  Deletes the index records if there are any.


                // It definitely would be nice to get DB sharding working before long, but undoubtedly that will take quite some time and effort to get working properly.
                //  Not sure how incremental it is worth being in approach to this task. Seems like getting trading working without sharding is more important.

                // May wind up with sharding by key ranges, or times. Sharding by key sub-ranges dependant on timestamp perhaps.

                // Could have another level of operations
                //  Get keys in range, and get the results from other machines in the shard.

                //indexes.push([13, 0, 'ETH']);

                // Server-side delete records and delete all their indexed records would be cool.

                //  Find records referring to...
                //   That seems like a useful operation because when changing one record, may want to find all records that refer to it to get them to refer to the new record.

                // Definitely want to get this to respond properly to a new coin being launched on Bittrex.
                //  Also to analyse coin launches, what happens in the very short time after launched. Basically need to wait for a floor (or unload) and buy after a small amount of time. Then sell soon after.
                //   

                // Write protecting records would also be useful.
                //  Need to prevent overwrite of some records, such as bittrex currencies and markets. Better to raise an error in some cases.

                // When changing a record id - need to delete the record, and replace it.

                //  Also need to delete the old index records, and replace them.
                //   Can do that all client-side.

                each(indexes, idx => {
                    //idx.pop();
                    let idx_without_id = idx.slice(0, -1);

                    // then we want to get the keys beginning with that, encoded.
                    console.log('idx_without_id', idx_without_id);

                    // then get keys in range.
                    //  searching for indexes will just be about key range / key operations, successfully encoded

                    // doing this while encoding the whole key at once?

                    

                    let encoded_index_part = database_encoding.encode_index_key(idx);


                    console.log('encoded_index_part', encoded_index_part);

                    // then lets get the keys beginning with it
                    //console.log('!!this.get_keys_beginning', !!this.get_keys_beginning);


                    fns.push([this, this.ll_get_keys_beginning, [encoded_index_part], (err, res) => {
                        if (err) {
                            console.log('err');
                        } else {
                            console.log('res', res);
                        }
                    }]);




                });

                fns.go((err, res_all) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('res_all', res_all);

                        // seems not to have found any index keys for the item.


                    }
                })

                */





                // Then can check for these index values being as we have here...
                //  


                // What about its index records if it's in the wrong place?
                //  Want a function to calculate the index record from the record itself.
                //   Would load it up into a model table, and then get the result.
                //   

                // get_idx_records_by_record

                // could use the model much more for doing this?

                // It does seem like keeping a CockroachDB going would make sense.







                // if it's 0, then it's in Bitcoin's place.

                //  we need to change it's ID to go to be a new incrementor record.
                //   currency at position 0 would no longer exist.








            }, err => {

            });
        });

        return pr_res;


    }


    // Does seem important to use validate/fix methods to get the data8 and maybe some other data working properly.
    //  There is quite a nice amount of data that should be available.
    //   It seems like an overall quite complex system. 

    // Should be possible to calculate moving averages and their crossovers.
    //  Will begin with a smaller amount of datasets, to help to verify that the data is valid.



    // function converters
    //  have cb fn -> pr, cb
    //       ob fn -> ob, cb
    //       pr fn -> pr, cb

    // Putting a .then method on observables could do the trick.
    //  Puts together an array of results, and returns it.


    // Will do a full diagnosis in terms of searching for problems in the database.



    diagnose(callback) {

        // Probably best to fix all the problems at once, in the right sequence.
        //  Find out what diagnosis there is, ie get a list describing all faults, then apply it to fix_problems

        // May as well use a callback generally here because 




        let obs_problems = new Evented_Class();


        let dodgy_item_codes = [];
        let map_dodgy_item_codes = {};



        // missing codes
        //  found on bittrex, missing from db

        // corrupt codes.

        // 


        this.bittrex_watcher.get_currency_codes((err, arr_bittrex_codes) => {
            if (err) {
                throw err;
            } else {

                console.log('arr_bittrex_codes', arr_bittrex_codes);

                //throw 'stop';


                // Want to see which of the currencies are missing (from here would be inactive currencies)
                //  Want to see which currencies here are missing in the db records.

                // the bittrex currencies map by id as well...



                this.get_bittrex_currency_codes((err, db_bittrex_currency_codes) => {
                    if (err) {
                        throw err;
                    } else {
                        let tm_bittrex_currency_codes = get_truth_map_from_arr(db_bittrex_currency_codes);
                        let currency_codes_missing_from_db = [];

                        let tm_current_bittrex_currency_codes = get_truth_map_from_arr(arr_bittrex_codes);

                        //console.log('db_bittrex_currency_codes', JSON.stringify(db_bittrex_currency_codes));

                        each(arr_bittrex_codes, code => {
                            let exists = tm_bittrex_currency_codes[code] || false;
                            //console.log('code', code);
                            //console.log('code exists', exists);

                            if (!exists) {
                                currency_codes_missing_from_db.push(code);
                            }
                        });

                        let delisted_codes = [];


                        each(db_bittrex_currency_codes, code => {
                            let exists = tm_current_bittrex_currency_codes[code] || false;
                            //console.log('code', code);
                            //console.log('code exists', exists);

                            if (!exists) {
                                delisted_codes.push(code);
                            }
                        });

                        //console.log('currency_codes_missing_from_db', currency_codes_missing_from_db);
                        //console.log('delisted_codes', delisted_codes);

                        if (currency_codes_missing_from_db.length > 0) {
                            obs_problems.raise('next', ['currency codes missing from db', currency_codes_missing_from_db]);
                        }


                        // Also need to check for duplicate currency IDs.
                        //  This may have hapenned if one currency id has overwritten another.
                        //   This could make the numerical data that has gone into the system corrupt.

                        // It seems like a lot of data will be recoverable, but some not as it has been corrupted.
                        //  Probably hapenned because the incrementor was not set up to start properly or correct itself.
                        //   I think the causative buf is fixed now, but there has been some corruption in data written.

                        // Still, would be possible to fix the bugs by changing the data to what it should be where possible.
                        //  Then can later scan for and possibly fix misaligned records.

                        // Maybe keeping track of the misalignments will help.
                        //  That way the number of prospective value serieses can be limited further.


                        // need to put together a map of the currencies by id to test if any are already there.
                        //  shared id, currencies sharing the id.
                        //   then work out which currency needs to have its id changed.

                        // Would be fine to change the id to what it should be, then 


                        // Just look through the connected bittrex currencies, see if any two share the same ID.
                        //  I don't see how that is possible though???
                        //  They only have one id field.
                        //   Possibly it's doing a corrupted index lookup.
                        //    Validation of index on safer start makes sense.
                        //     Could check the structural tables.
                        //      That is any table in the core or with any other table fks pointing towards it.

















                        // Then check for currency codes which have got unexpected / wrong IDs.

                        //each(db_bittrex_currency_codes, code => {

                        //})

                        this.get_bittrex_currencies_map_by_id((err, db_currencies_by_id) => {
                            if (err) {
                                throw err;
                            } else {
                                //console.log('db_currencies_by_id', db_currencies_by_id);

                                // Then iterate these while checking if any of them conflict with values that should be there.

                                // currencies in wrong place in db
                                // currencies that are not in the db in a set position (and possibly not in the db at all)
                                // currencies that are not in the db but should be.

                                // has overwritten fixed currency

                                let mismatches = [];


                                each(db_currencies_by_id, db_currency => {

                                    let currency_id = db_currency[0][1];

                                    if (map_list_correct_bittrex_currency_ids[currency_id]) {

                                        let correct_item = map_list_correct_bittrex_currency_ids[currency_id];
                                        //console.log('');
                                        //console.log('correct_item', correct_item);
                                        //console.log('db_currency', db_currency);
                                        //let db_currency_id = db_currency[0][1];

                                        let matches = db_currency[1][0] === correct_item;

                                        if (!matches) {
                                            // Say which is missing, at what id, and what has replaced it.
                                            //console.log('db_currencies_by_id', db_currencies_by_id);
                                            //throw 'stop';

                                            //console.log('db_currencies_by_id[currency_id]', db_currencies_by_id[currency_id]);
                                            //throw 'stop';

                                            //console.log('record', db_currencies_by_id[currency_id]);

                                            //throw 'stop';
                                            let obj_mismatch = {
                                                'should_be': correct_item,
                                                'at_id': currency_id,
                                                'code_present': db_currency[1][0],
                                                'record': db_currencies_by_id[currency_id]
                                            }
                                            mismatches.push(obj_mismatch);

                                        }

                                        //console.log('currency_id', currency_id);
                                        //console.log('map_list_correct_bittrex_currency_ids[currency_id]', map_list_correct_bittrex_currency_ids[currency_id]);

                                        // see what 

                                        //if (map_list_correct_bittrex_currency_ids[currency_id][1][0] !== currency_id) {

                                        //}
                                    }



                                });

                                //console.log('mismatches', mismatches);
                                obs_problems.raise('next', ['currencies overwritten', mismatches]);





                                // Look for malformed currency indexes
                                //  


                                let malformed_table_records = [];




                                //

                                let obs_scan = this.error_scan_table('bittrex markets');
                                obs_scan.on('next', data => {


                                    //console.log('bittrex markets scan mal-formed record data', data);

                                    let decoded_data_value = Binary_Encoding.decode_buffer(data[1]);
                                    //console.log('decoded_data_value', decoded_data_value);





                                    let market_code = decoded_data_value[1];
                                    let [base_code, item_code] = market_code.split('-');

                                    //console.log('[base_code, item_code] ', [base_code, item_code]);

                                    if (!map_dodgy_item_codes[item_code]) {
                                        map_dodgy_item_codes[item_code] = true;
                                        dodgy_item_codes.push(item_code);
                                    }

                                    // as well as the dodgy item codes, need to know the malformed records themselves

                                    // market data records with broken indexes (won't decode)

                                    malformed_table_records.push({
                                        'arr_bufs': data,
                                        'value': decoded_data_value,
                                        'table': 'bittrex markets'
                                    });



                                    // Then check for each of these currencies being in the currencies table.
                                    //  Since we don't know their IDs, could scan all currency records for them.

                                    //  Worth doing a full table scan for the record, as well as for any index record?
                                    //   Better to use the Model to construct what that record will be.
                                    //  It may be something like validate_record.


                                });

                                obs_scan.on('complete', () => {
                                    obs_problems.raise('next', ['malformd table records', malformed_table_records]);
                                    obs_problems.raise('complete');
                                })

                            }
                        })






                        // Could be worth first identifying what is missing from the DB, and working out what place it should be in
                        //  Then see what is in its place, and move that to where it should be (having allocated free space)

                        // Probably won't be all that many currencies that have mismatches.
                        //  Want to methodically work through them on all servers and download all data I can get, verify it as it comes in.
                        //   May well be worth syncing this way to a new server that has been set up there.






                        // Moving towards shading will be nice with an 'offloading' system where a node is told which key ranges it should not have.
                        // May be worth giving it a map of all key ranges that other nodes accept
                        //  Even keeping that fully synced between servers.


                        // With syncing, need mismatch detection and fixing in the right way.
                        //  Just need to leave the server system there gathering data for a while.

                        // Sharding by subdivision or subdivision and time could more.
                        //  More recent data maintained in more places.

                        // Want a few live moving average systems running ASAP.
                        //  Look at a few strong indicators for trading signals, then make the trades accordingly.

                        // Will also judge very short-term movements, and have readouts of that.








                        // Moving the errant record seems like a good first step.
                        //  Or do full diagnose and then movement of records.













                        // Seems OK here.

                        // Making a version that does not change db 1, but a safer db tool that opens the db and scans through all records (core first)
                        //  looking for mal-formed rows.
                        //   maybe could cross-reference all data, using appropriate lookups, to see if it works as expected.

                        // Getting it so we can get the full run-down on erroneous data in 



                        // And should be able to identify what the index should be.
                        //  Could use hard-coded values based on existing and working data.
                        //   Difficulty with accounting for removed coins, may be necessary to find them, if poss, and incorporate them.
                        //    Could look at the existing data for coins that have since been removed from bittrex.






                    }
                })




                // 

                // we get the error with the currency there by scanning the markets table.
                //  finding all currencies referred to in bittrex markets.



                // assets client get bittrex currency codes.
                //  will go through the bittrex currency records, extracting their codes.








                /*

                let obs_scan = this.error_scan_table('bittrex markets');
                obs_scan.on('next', data => {
                    console.log('scan data', data);

                    let decoded_data_value = Binary_Encoding.decode_buffer(data[1]);
                    console.log('decoded_data_value', decoded_data_value);

                    let market_code = decoded_data_value[1];
                    let [base_code, item_code] = market_code.split('-');

                    console.log('[base_code, item_code] ', [base_code, item_code]);

                    if (!map_dodgy_item_codes[item_code]) {
                        map_dodgy_item_codes[item_code] = true;
                        dodgy_item_codes.push(item_code);
                    }


                    // Then check for each of these currencies being in the currencies table.
                    //  Since we don't know their IDs, could scan all currency records for them.



                    //  Worth doing a full table scan for the record, as well as for any index record?
                    //   Better to use the Model to construct what that record will be.
                    //  It may be something like validate_record.




                });
                //obs_scan.on('complete', obs_problems.raise('complete'))


                obs_scan.on('complete', () => {
                    console.log('dodgy_item_codes', dodgy_item_codes);


                    // Should call this in sequence and get the results together.
                    //  resolve a group of promises together seems like the best approach.

                    / *
        
                    //let obs_currency_problems = this.diagnose_bittrex_currency(dodgy_item_codes[0]);
        
                    obs_currency_problems.on('next', data => {
                        obs_problems.raise('next', {
                            'table': 'bittrex currencies',
                            'arr_record': data
                        })
                    })
        
                    obs_currency_problems.on('complete', data => {
                        obs_problems.raise('complete');
                    })
         
                    * /


                    // Need to apply this to each of the currencies with problems.
                    //  It seems some earlier currency records were overwritten on various servers.

                    // Making a fixed version of the software and then deploying it to data1 and onwards will work best.

                    // Want to begin some sharding behaviour where a server configured to do so offloads some of its key range, or specific key ranges onto another server.
                    //  Offloading could be a way of doing it where it sends the data to servers that allow it, logs where it has sent it in a local table, then deletes the local data.
                    //   If ever asked for any of that local data, will retrieve it from the DBs that it considers responsible for it.
                    //    Would need to stay updated with what other DBs advertise as the data ranges they hold.

                    // Not so sure about a huge variety of different data ranges.
                    //  Would be able to define data ranges according to rules.

                    // Anyway, need a client-side function to scan the currencies and find missing currencies.
                    //  Maybe do that after correcting records with invalid keys.

                    // Should work out what to correct other records to.


                    // Interesting idea: a server that has operations to fix data on another server / the other servers.
                    //  Knows some tables need to be syncronised to match its own in some cases.

                    // Right now, best to progress with some data retrieval and fixing, def get data 8 working.
                    //  Data 9 and 10 could be worth starting up.
                    //   Data11 to get data out of other existing servers, and to compare tables for disgnostic purposes.

                    // May need to change some market records, and therefore change lots of snapshot records that refer to them and have been put in the db wrong.


                    // A diagnosis asset clien to assess the damage.
                    //  A scan when the db starts to scan all records to see if they are mal-formed.
                    //   Just alert if they are.

                    // A server-side record to get all mal-formed keys or records in range.
                    //  This could possibly be useful for upgrading record structures too.

                    // Would help to identify which of the records are not mal-formed.
                    //  Many records presumably will be OK.

                    // db malformed record count, percentage.




                    this.diagnose_bittrex_currency(dodgy_item_codes[0]).then(res => {
                        console.log('diagnose_bittrex_currency res', res);

                        // these are the dodcgy records found.

                        // Should find out what it's index should be.

                        // Seems strange that adding the new currency did not use the incrementor to assign its id correctly.

                        // An insert that first checks for the same primary key would help.
                        //  insert table record.

                        // Check if the db has_key

                        // should have loaded the model.
                        //  don't know why we are still stuck at incrementor problems.

                        obs_problems.raise('next', {
                            'table': 'bittrex currencies',
                            'arr_row': res,
                            'problem': 'id 0 when not BTC'
                        });
                        obs_problems.raise('complete');




                    }, err => {
                        console.log('err', err);
                    })

                    // diagnose the problems with those currencies.

                    //  see if it is recorded at all.

                    // I think that making some things more OO will help.
                    //  Have a Currency class.
                    //   Or data strcuture really, it holds basic data about that currency.
                    //   The Currency class could then be a place from which to load currency data.
                    //    Though it would best if there was little data retrieval code actually in currency, but it uses a data loading mechanism from elsewhere.
                    //     Some kind of data provision API that is brought together at the level of the currency.

                })

            */



            }
        })

        if (callback) {
            let arr_all = [];
            obs_problems.on('next', data => {
                //console.log('data', data);
                arr_all.push(data);
            });
            obs_problems.on('error', err => {

                callback(err);

            });
            obs_problems.on('complete', () => {
                //throw 'stop';
                callback(null, arr_all)
            });
        } else {
            return obs_problems;
        }
    }


    // Will get the diagnosis, then apply the fixes
    diagnose_fix() {


        // Logging fix operations could be useful for data recovery,
        //  If some records have been mixed up, knowing what swaps were made would be useful.
        //  Would require a sub-db.

        // With only a few mismatches, it won't be so hard to fix.
        //  Need to get the DB into the correct structure.
        //  If some data has been corrupted, we would notice it on import with consistency checks
        //   Consistency with itself... is it one time series? 2 distinct ones? How many distinct time series values.








        /*

        let obs_diagnosis = this.diagnose();
        let problems = [];

        let res = new Evented_Class();

        obs_diagnosis.on('next', problem => {
            problems.push(problem);
        });

        obs_diagnosis.on('complete', () => {
            console.log('problems', problems);

            throw 'stop';

            each(problems, problem => {
                if (problem.problem === 'id 0 when not BTC' && problem.table === 'bittrex currencies') {
                    // change the record to use a new id.

                    let record = problem.arr_row;

                    // remake it with the right ID.

                    let new_id = this.model.map_tables[problem.table].pk_incrementor.increment();

                    console.log('new_id', new_id);

                    let new_record = clone(record);
                    new_record[0][new_record[0].length - 1] = new_id;

                    console.log('record', record);
                    console.log('new_record', new_record);

                    this.cs_update_record_update_indexes(record, new_record);



                    //this.cs_update_record_update_indexes


                }
            })

            // fix those problem records. 


            / *
            let new_id = this.model.map_tables['bittrex currencies'].pk_incrementor.increment();
            console.log('new_id', new_id);

            if (new_id > 1) {

            }
            * /

        });

        return res;

        */

        let res = new Evented_Class;

        let obs_through = (source, target) => {
            source.on('next', data => target.raise('next', data));
            source.on('error', err => target.raise('error', err));
            source.on('complete', () => target.raise('complete'));
        }

        this.diagnose((err, arr_problems) => {
            if (err) {
                res.raise('error', err);
            } else {
                console.log('arr_problems', JSON.stringify(arr_problems));

                //throw 'stop';

                let map_problems_by_type = {};
                each(arr_problems, problem => {
                    console.log('problem', problem);

                    map_problems_by_type[problem[0]] = map_problems_by_type[problem[0]] || [];

                    each(problem[1], individual_problem => {
                        map_problems_by_type[problem[0]].push(individual_problem);
                    });
                    //map_problems_by_type[problem[0]].push(problem[1]);

                });

                console.log('map_problems_by_type', JSON.stringify(map_problems_by_type));

                //throw 'stop'


                // Check for malformed records in the currency table.

                let map_malformed_records_by_table = {};


                if (map_problems_by_type['malformd table records']) {
                    each(map_problems_by_type['malformd table records'], malformed_record_set => {
                        // not just in the tables table.

                        console.log('malformed_record_set', malformed_record_set);

                        each(malformed_record_set, malformed_record => {
                            console.log('malformed_record', malformed_record);
                            //throw 'stop';

                            map_malformed_records_by_table[malformed_record.table] = map_malformed_records_by_table[malformed_record.table] || [];
                            map_malformed_records_by_table[malformed_record.table].push(malformed_record);
                        })
                    })
                } else {
                    console.log('no malformed table records');
                }

                console.log('map_malformed_records_by_table', map_malformed_records_by_table);

                // Then if there are no malformed currency records...

                let malformed_currency_records = map_malformed_records_by_table['bittrex currencies'];

                if (malformed_currency_records && malformed_currency_records.length > 0) {
                    throw 'Not yet able to fix malformed currency records';
                } else {
                    console.log('no malformed currency records');


                    //deal with the currencies overwritten.
                    // will move whatever has overwritten early values, having found where it should be.
                    //  need to update the incrementor as part of such a move (or just after it).

                    // Then put the correct record back in place.

                    //download_put_currency_at_id

                    // Then when data8 is passing validation, get the data from it.
                    // Proceed to attempt to recover data from other servers too - at least run diagnose.


                    // Just need to get the current issues solved on multiple PCs.
                    //  Would be a different diagnosis on different machines, more of them will be wrong on the older machines.

                    // Just need to get the data collection working reliably and properly.
                    //  A few days referring back will be enough in many cases.

                    // What about putting together all of the DB changes, allow them to be confirmed before doing them.

                    // Could see the incrementor changes that way too.

                    // currencies overwritten'

                    let currencies_overwritten = map_problems_by_type['currencies overwritten'];
                    console.log('currencies_overwritten', currencies_overwritten);
                    console.log('map_problems_by_type', map_problems_by_type);
                    //throw 'stop';

                    // Could try to make all of the fixes at once, putting them together.

                    let obs_fix = this.fix_currencies_overwritten(currencies_overwritten);

                    // then when that is done, fix the malformed markets records.
                    //  will use the values from the new currency values.
                    // curreny codes missing from db
                    let c_missing = map_problems_by_type['currency codes missing from db'];

                    console.log('c_missing', c_missing);


                    if (c_missing.length > 0) {

                        this.ensure_bittrex_currencies((err, res) => {
                            console.log('currencies ensured');
                        })

                    }

                    // then add those missing currency records.
                    //  Could do the standard ensure bittrex currencies.

                    //this.ensure_




                    // Could put together put instructions.









                }



                // deal with some problems before others.

                // come up with a solution plan.

                // move overwritten records to the end, freeing space.
                //  start with the first overwritten record.

                // Move it from its current position to the end of its autoincrementing table.







                //let obs_fix = this.fix_problems(arr_problems);
                //obs_through(this.fix_problems(arr_problems), res);
            }
        })
        return res;
    }


    // Get currencies overwritten fixes


    // get curency overwritten fix

    // Modelling broken records could work.
    //  Invalid key records - would store the key value, but not attempt to render it as JS objects.



    fix_currency_overwritten(currency_overwritten_problem, new_id) {
        let res = new Evented_Class();


        // and have an id to assign it.

        console.log('fix_currency_overwritten currency_overwritten_problem', currency_overwritten_problem);

        // modify the record.
        let decoded_record = currency_overwritten_problem.record;

        //let decoded_record = database_encoding.decode_model_row(currency_overwritten_problem.record);

        //console.log('decoded_record', decoded_record);

        let record_for_deletion = clone(decoded_record);
        let record_for_update = clone(decoded_record);
        let record_for_put = clone(decoded_record);
        record_for_put[0][1] = new_id;

        console.log('record_for_deletion', record_for_deletion);
        // get the index records for the record for deletion

        console.log('record_for_put', record_for_put);


        // don't make the change yet, but now the logic is clearer.

        // And create the currency record that should be in place.
        //  Get the downloaded info for whatever currency it is.

        // only for bittrex currencies so far.
        //  probably best to change to a unified currencies API, where the exchange name or id gets provided each time.

        // Indexing trades by timestamp as well as trades will probably prove useful.
        //  Need to get out of this complication to do with wrong bittrex records.

        this.bittrex_watcher.get_map_currencies_info((err, map_currencies_info) => {
            if (err) {
                res.raise('error', err);
            } else {
                //console.log('map_currencies_info', map_currencies_info);


                let code_overwritten = currency_overwritten_problem.should_be;

                let code_overwritten_info = map_currencies_info[code_overwritten];
                //console.log('code_overwritten_info', code_overwritten_info);

                record_for_update[1][0] = code_overwritten_info[0];
                record_for_update[1][1] = code_overwritten_info[1];
                record_for_update[1][2] = code_overwritten_info[2];
                record_for_update[1][3] = code_overwritten_info[3];
                record_for_update[1][4] = code_overwritten_info[4];
                record_for_update[1][5] = code_overwritten_info[5];
                record_for_update[1][6] = code_overwritten_info[6];
                record_for_update[1][7] = code_overwritten_info[7];



                //console.log('record_for_update', record_for_update);

                // then could use the model Table to create the index records.

                // however, there is core DB functionality that should be able to do this easily.

                //this.model.arr_records_to_records_with_index_records
                //  creates the Record objects with the right Table objects, then returns them all as an array.
                //   then will be able to put them into the DB relatively easily.


                // Also need to fix the malformed market records (later, after fixing currency records)

                let no_kp_put = clone(record_for_put);
                //no_kp_put[0].shift();
                let no_kp_update = clone(record_for_update);
                //no_kp_update[0].shift();
                //let no_kp_put = clone(record_for_put)[0].shift();
                //let no_kp_update = clone(record_for_update)[0].shift();

                let all_keys_to_delete = this.model.create_index_records_by_record(record_for_deletion);
                console.log('all_keys_to_delete', all_keys_to_delete);

                let all_records_to_put = this.model.arr_records_to_records_with_index_records([no_kp_put, no_kp_update])
                console.log('all_records_to_put', all_records_to_put);

                // Need a new server-side function to handle this.
                //  In fact, need the full path for doing it.

                //  This looks like one of the things where we need both the ll version and the hl version.

                // Possibly the keys should be encoded as keys on the way to the server?
                //  Right now we are using somewhat more general purpose, simpler to use, encoding.

                // Possibly keys should be encoded as keys before going to the server.
                //  Could maybe have a Key_Set class, to help its easy / efficient encoding and decoding.
                //  More of the tricky, repeated functionality is getting put in the Model.

                // Key_Set would just have an inner buffer, and have functions that can split them up.

                //  Would be used in the background much of the time.
                //  Also Record_Set
                //      Buffered_Record
                //       Like Record, but backed by a Buffer.

                // Should have a system that automatically encodes the keys on the way to the DB.
                //  Later, can use key specific encoding, or buffer_list encoding.

                // Need to delete the records successfully.
                //  For the moment, will download a backup of the full data8 db file.





                this.ll_delete_records_by_keys(all_keys_to_delete, (err, res_delete) => {
                    if (err) {
                        response.raise('error', err);
                    } else {

                        console.log('records deleted (by key): ', all_keys_to_delete);

                        // then batch put the ones we want.


                        // Will do an upgrade to put records.
                        //  A Record_List object may help here.
                        //  Put together the record list easily, then get its buffer value.
                        //   Will be encoded as an array of buffers.
                        //   Or array of arrays?
                        //    Could have records encoded on the server as arrays.
                        //   Or as a list of buffers that is interpreted as key value pairs.

                        // And server-side, could decode this using the Record_List.

                        //this.

                        let rl = new Record_List(all_records_to_put);

                        // Then should be able to send this record list easily.
                        //  It should be understandable by put records.

                        // May be best to test this separately.

                        // Being able to create and use a table without defining any fields would be useful (for testing too).

                        // and then get the decoded values from the record list.

                        console.log('rl.decoded', rl.decoded);

                        // can make a 'put' command that uses a Record_List, it would be decoded into a buffer on the server.
                        //  however - we may already know it's a buffer.
                        //   best to have it typed as a buffer for when it get put into other arrays.



                        this.put(rl, (err, res_put) => {
                            if (err) {
                                throw err;
                            } else {
                                console.log('cb put');

                                console.log('rl', rl);



                                process.nextTick(() => {
                                    res.raise('complete');
                                });




                            }
                        });






                    }
                });



                /*
                this.put(all_records_to_put, (err, res_put) => {
                    // Want a simple put function.
                    //  Puts the records as an array.
                    //  Using an OO system for the record-list will be useful.
                    //  Will be able to get them all as a buffer easily.
                    //   Would be stored internally as a buffer.




                });
                */



                //throw 'stop';




                // delete the keys












                // put 2 records (more with indexes)
                //  it will overwrite the overwritten record. no need to delete a record.

                // Need to put these records with their appropriate indexing.
                //  Could have something on the server side to create a record index whenever it is put.
                //  However here, want to use client-side functionality.







                //record_for_update[0][1] = currency_overwritten_problem.at_id;
                // already is.



                // 


            }
        })













        return res;

    }



    fix_currencies_overwritten(arr_currencies_overwritten_problems) {

        let res = new Evented_Class();

        let model = this.model;
        let table = model.map_tables['bittrex currencies'];


        if (arr_currencies_overwritten_problems.length > 0) {
            console.log('pre get_table_last_id');
            this.get_table_last_id('bittrex currencies', (err, last_bittrex_currency_id) => {
                if (err) {
                    console.trace();
                    throw err;
                } else {
                    console.log('last_bittrex_currency_id', last_bittrex_currency_id);

                    // OK, now this is working.

                    // Observable sequencer.

                    // Best to do it one at a time. maybe


                    // Generating the changes would work nicely.
                    //  Replace rows by keys.

                    // Then also need to replace the malfomrmed bittrex markets table records later on.
                    //  Then need to keep the data together and verify it too.





                    // here is the place to create the db record changes.
                    //  It may be worth making a number of commands in the model

                    // Definitely use observable sequencer
                    //  But the new observable API needs to have a .subscribe in order to start it and get the data.

                    // Having fns so it can process observables...

                    //let fns = new Fns();


                    // put together the observables list

                    let observables = [];

                    let sequence_observable_calls = (calls) => {

                        let c = 0,
                            l = calls.length;
                        let res = new Evented_Class();

                        let go = () => {
                            console.log('c', c);
                            if (c < l) {
                                //let obs = observables[c];
                                //console.log('calls[c]', calls[c]);

                                let [context, fn, args] = calls[c];
                                let obs_fn = fn.apply(context, args);
                                obs_fn.on('next', data => {
                                    res.raise('next', {
                                        args: args,
                                        data: data
                                    })
                                });
                                obs_fn.on('error', err => {
                                    res.raise('error', err);
                                })
                                obs_fn.on('complete', () => {
                                    console.log('has raised complete');
                                    res.raise('next', {
                                        args: args,
                                        complete: true
                                    })
                                    c++;
                                    go();
                                });
                            } else {
                                console.log('done');
                                res.raise('complete');
                            }

                        }

                        process.nextTick(go);
                        //go();

                        return res;

                    }


                    each(arr_currencies_overwritten_problems, currency_overwritten_problem => {
                        console.log('currency_overwritten_problem', currency_overwritten_problem);
                        // need to do it one by one.
                        observables.push([this, this.fix_currency_overwritten, [currency_overwritten_problem, last_bittrex_currency_id++]]);
                        // Generate the fix, apply it now?

                        // Need to come up with the correct records / IDs for both of them
                        //  get the next available ID.
                        //   that could be done with incrementing in the model.

                        // Could maybe use verified_increment
                        //  where it gives the starting increment value, checks it, and then gets the new incrementor value back.

                        // Or verify the incrementor is where it should be
                        //  Then increment the next one.

                        // Doing this in the model is a bit too longwinded, the model does not have these tables loaded...
                        //  But could load them. Hard to get the model to load invalid records though.

                        // call function to work out what these records should be.
                        // do it one by one...
                        // or get the last id value from the db table


                    })

                    let obs_all = sequence_observable_calls(observables);
                    obs_all.on('next', data => {
                        //console.log('data', data);
                    });
                    obs_all.on('complete', () => {
                        console.log('fix_currencies_overwritten inner obs complete');
                        res.raise('complete')
                    });

                }

            })
        } else {
            res.raise('complete');
        }







        return res;

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


    var server_data2 = config.nextleveldb_connections.data2;
    server_data2.access_token = access_token;
    var server_data3 = config.nextleveldb_connections.data3;
    server_data3.access_token = access_token;
    var server_data4 = config.nextleveldb_connections.data4;
    server_data4.access_token = access_token;
    var server_data5 = config.nextleveldb_connections.data5;
    server_data5.access_token = access_token;
    var server_data6 = config.nextleveldb_connections.data6;
    server_data6.access_token = access_token;
    var server_data7 = config.nextleveldb_connections.data7;
    server_data7.access_token = access_token;
    var server_data8 = config.nextleveldb_connections.data8;
    server_data8.access_token = access_token;


    local_info.access_token = access_token;
    //var server_data1 = config.nextleveldb_connections.localhost;

    // The table field (for info on the fields themselves) rows are wrong on the remote database which has got approx 12 days of data.
    //  Can still extract the data, I expect.

    // Don't want to replace the code on the server quite yet.

    // May be possible to edit the fields, possibly validate the fields?

    var client = new Assets_Client(local_info);

    // Some of the clients have now got corrupted data.

    // Seems fine to copy the data of clients 5 and 6.

    //  Want a way to assess the validity of data, can work back from known values.
    //  Could pinpoint which dataset has which values that correspond with known ones.

    // Definitely need data syncing from the servers to local.
    //  Getting the data out of the older servers could prove tricky / impossible / too time consuming.
    //  Better to save / keep that data.

    // Maybe would be easier to set up and use CockroachDB to have a well sharded, reliable and performant data set.

    // Downloading and storing the data here as needed.
    //  Seems somewhat tricky to turn this system into a fully sharded one. Replication is proving tricky enough.
    //  














    // Looks like some currencies were overwritten or stored wrong.
    //  Tricky since most of the markets reference bitcoin.
    //  Seems that a combination of problems caused this.





    //var client = new Assets_Client(local_info);

    client.start((err, res_start) => {
        if (err) {
            throw err;
        } else {
            // Because the incrementor was loaded wrong on the server.
            //  I thought that got corrected before.

            // The first part of diagnosis is looking up the incrementor value for that table.
            //  The model will have been loaded, so it should be high.



            // client.fix_table_incrementor_value
            //  make it one more than the maximum id.

            // get table maximum id...?







            console.log('Assets Client connected to', server_data8);

            // Model should have been loaded.


            // 22/03/2018 - Nice to see this still works.
            //  Could retrieve data as more directly encoded TypedArray recordsets.
            //   Would not retrieve them as records, but using Binary_Encoding, with Binary_Encoding more advanced than it is now.

            // Possibly coins have been removed, so that could mean the keys on different servers are different.
            //  Would be somewhat difficult to change the usage of one value to another.
            //  Would need to download the normalised records, or get values changed on one of the (live) servers.

            // Could increase the amount a DB can deal with unnormalised data.
            //  When it recognises some differences, it can download unnormalised data, or provide it.

            // Changing record values when changing preceeding data - should not be impossible.
            //  Will need to investigate these discrepencies further.
            //  When syncing, would be able to detect differences in the structural tables.

            // Do diffs between a variety of table records.
            //  Downloading with mapping the results looks like one of the best ways of doing it.
            //  Probably best experimenting with servers 2, 3 and 4. See if we can get the data back to the local db, and test that data.

            // Then work on data retrieval from server 1.
            //  Before long, we need to have the full set of data available to use.












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
            //test_get_bittrex_snapshot_records();

            let test_get_bittrex_currencies_records = () => {
                let decode = true;
                let table_name = 'bittrex currencies';

                let obs_table_records = client.get_table_records(table_name, decode);

                let page_number = 0;
                obs_table_records.on('next', data => {
                    console.log('data.length', data.length);
                    console.log('page_number', page_number++);
                    //console.log('data', data);


                    each(data, item => console.log('item', item));
                });
                obs_table_records.on('complete', last_data_page => {
                    //console.log('data', data);
                    console.log('complete');
                });
            }
            //test_get_bittrex_currencies_records();


            // get bittrex currencies records




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


            // bittrex currencies

            let test_get_table_records = () => {
                client.get_table_records('bittrex currencies', (err, res) => {
                    if (err) {
                        throw err;
                    } else {
                        //console.log('bittrex currencies records', res);
                        //console.log('bittrex currencies')
                        each(res, item => console.log('item' + item[0][0] + ', ' + item[1][0]));
                    }
                })
            }
            //test_get_table_records();

            let diagnose = () => {
                let obs_problems = client.diagnose();

                obs_problems.on('next', problem => {
                    console.log('problem', problem);
                });
            }

            //diagnose();

            let diagnose_fix = () => {
                let obs_fixes = client.diagnose_fix();

                obs_fixes.on('next', fix => {
                    console.log('obs_fixes', obs_fixes);
                });
            }

            diagnose_fix();


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