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
    fs.mkdir(path, mask, function(err) {
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















    ensure_bittrex_structure_current(callback) {
        // .load_core_plus_tables(['bittrex markets', 'bittrex currencies']

        // Assume the core is loaded?
        //  No harm in reloading it?
        //let p_bw = promisify(this.bittrex_watcher);

        let bw = this.bittrex_watcher;
        let that = this;

        this.load_core_plus_tables(['bittrex currencies', 'bittrex markets'], (err, model) => {
            if (err) { callback(err); } else {
                // Get the currencies and markets (together) from bittrex-watcher
                // Check that each of the currencies has a record in the DB.
                //  Could use the model for this.
                //  However, directly checking in the DB itself may be better.

                // Seems to be how Bluebird promisify works.
                let p_bittrex_downloaded_structure = promisify(bw.download_bittrex_structure, {context: bw});

                // using await and destructuring?
                // download_bittrex_structure returns [at_currencies, at_markets]
                //  or it should do.

                console.log('p_bittrex_downloaded_structure', p_bittrex_downloaded_structure);

                p_bittrex_downloaded_structure().then(([at_currencies, at_markets]) => {
                    console.log('at_currencies', at_currencies);
                    console.log('at_markets', at_markets);

                    console.log('at_currencies.length', at_currencies.length);
                    console.log('at_markets.length', at_markets.length);


                    // Here it may be worth doing checks to see if we have those records.

                    // client.check_records
                    //  would check those records exist on the server.
                    //   we may not know what the assigned key is.
                    //   so it may check the records against the fields that the records are indexed by.

                    // Also want a systematic way to transform records.
                    console.log('at_currencies.keys', at_currencies.keys);

                    // So we can use just one field from this array_table, 'Currency', and then search by indexed value within the DB.

                    var currency_0 = at_currencies.values[0];
                    console.log('currency_0', currency_0);

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
                        console.log('pre check_table_records_exist');
                        that.check_table_records_exist('bittrex currencies', at_currencies.values, (err, res_records_exist) => {
                            if (err) { callback(err); } else {
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

                                            

                                            

                                            // Check the market records exist

                                            // The market records get transformed somewhat.
                                            console.log('at_markets.values', at_markets.values);

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

                                            that.check_table_records_exist('bittrex currencies', at_currencies.values, (err, res_records_exist) => {
                                                if (err) {
                                                    callback(err);
                                                } else {
                                                    console.log('res_records_exist', res_records_exist);

                                                    throw 'stop';
                                                }
                                            });


                                            // Then check that these records are in the database.


                                            


                                            
                                        }

                                        

                                    }))









                                    

                                    /*

                                    that.get_table_kv_field_names('bittrex currencies', (err, field_names) => {
                                        if (err) {
                                            throw err;
                                        } else {
                                            console.log('field_names', field_names);

                                        }
                                    })
                                    */







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

                        /*


                        that.check_table_record_index_lookup('bittrex currencies', currency_0, (err, res_lookup) => {
                            if (err) { callback(err); } else {
                                console.log('res_lookup', res_lookup);

                                // Want to check a whole number of records.
                                //  Would also be interested in adding any records that don't already exist.

                                // Then could ensure all markets are there.
                                // Would have the correct structure to add more data to the db on an ongoing basis.





                                //throw 'stop';


                            }
                        });

                        */

                    });


                    // Then want to look up the fields in the bittrex currencies table

                    // get_table_kv_field_names    get_table_field_names    get_map_table_field_names_by_id

                    // It only has values, but no keys.

                    // Get table indexes info

                    // This should be usable for getting the info about fields, alongside information about if they are indexed
                    //  One index could index multiple fields at once, but they must be within an order.
                    //   So could use the first part of an index for a query... but would possibly want to sort the results as needed.

                    /*
                    that.get_table_indexes_info('bittrex currencies', (err, indexes_info) => {
                        if (err) { 
                            callback(err);
                        } else {
                            console.log('indexes_info', indexes_info);

                            // Then we will know which fields are indexed
                            //  Use those indexes to be able to carry out a search using indexed data.


                            // Before long, definitely want to get server side indexing working for records such as bittrex currencies and markets.




                        }
                    })
                    */


                    /*


                    that.get_map_table_field_names_by_id('bittrex currencies', (err, field_names) => {
                        if (err) { callback(err); } else {
                            console.log('field_names', field_names);



                        }
                    });
                    */


                    /*

                    that.get_table_kv_field_names('bittrex currencies', (err, field_names) => {
                        if (err) {
                            callback(err);
                        } else {
                            console.log('field_names', field_names);

                            // These table index records could maybe do with some interpretation.
                            //  

                            // *

                            that.get_table_records('table indexes', (err, records) => {
                                console.log('records', records);
                            });

                            //* /

                            // Basically want to ensure the currency records are in the database

                            // Do a record_exists_check_indexes
                            //  this way we look up which indexes could be used to search for the record.
                            //  Then we search for it based on the indexes
                            //   Could return the record itself or its PK.

                            // Then before too long could move the index checking onto the server side.
                            //  Possibly making use of the model as well.
                            //  Or having a batch way of doing it that's efficient with lookups, maybe not using server-side model.
                            //   Seems best to avoid using that model when it's fairly simple logical processes. Would need to update the model as necessary later.
                            //    Possibly indicate that the model is 'dirty'.



                            // Want to add the functionality that makes it easy to add/define data.
                            //  Will want to add more bittrex market data, as well as data from other exchanges.

                            // Could work out to be a large amount of data.
                            //  Seems worth setting it up so that a lot gets downloaded on the residential connection, as well as in the cloud.

                            // May be worth using some cheaper specific storage services for longer term saving / backups.
                            //  Could store large documents / trade data blocks as large documents within Amazon.

                            // Could do some kinds of find record by...


                            





                            // 

                            // get_table_field_names

                            that.get_table_indexes_info('bittrex currencies', (err, indexes_info) => {
                                if (err) { 
                                    callback(err);
                                } else {
                                    console.log('indexes_info', indexes_info);

                                    // Then we will know which fields are indexed
                                    //  Use those indexes to be able to carry out a search using indexed data.


                                    // Before long, definitely want to get server side indexing working for records such as bittrex currencies and markets.




                                }
                            })

                            
                        }
                    })

                    */
                    







                    

                    //console.log('res', res);
                })



                // Can try PUT_NO_OVERWRITE operations.

                // For all of the currencies, we want to find if they are in the remote DB or the local model.
                //  Mainly want to deal with querying the remote DB, efficiently too.

                // Can check single records to begin with.


                // Definitely need to do some DB put that involves creating the index records.



                // LL_FIND_TABLE_RECORDS
                // LL_COUNT_FIND_TABLE_RECORDS


                // Want to search for a bittrex currency record.
                //  LL_FIND would be useful.
                //   Would know which table
                //   Would then refer to the indexes
                //   Would put together that record's index records (using the model)
                //   Would search for those index records in the DB.



            }
        })

    }


    // ensure_current_bittrex_currencies
    // ensure_current_bittrex_markets

    // Should be easy enough to do this via the model too.
    //  Want to call simple, probably async functions from the assets client

    // Functions that index the data into the DB would be very useful for sending the currency data.

    // Could have a function where we send one set of data, and it creates new records as needed, and then sends back a report of which records were created.
    //  All using binary encoding to transmit the data.

    // Attempting to add a single record at a time would be useful.







    // Downloads them with the watcher.
    // Transforms the records if needed
    // Sends those records to the DB, to be indexed as well.






    // load model with structural tables.
    //  Can we replace everything in an existing Model with things from other tables?

    // A version to ensure bittrex currencies that actually looks them up.




    /*
    download_ensure_bittrex_currencies(callback) {
        // Find out which bittrex are missing?
        //  Ideally, want a put operation that also says which have been overwritten.
        //  Seems like a lower level server function would do this OK.
        // Get the original keys and values, go through them doing the replacements.

        console.log('download_ensure_bittrex_currencies');
        var that = this;

        var tbl_bittrex_currencies = this.model.map_tables['bittrex currencies'];
        console.log('tbl_bittrex_currencies.records.arr_records.length', tbl_bittrex_currencies.records.arr_records.length);

        / *
        var map_ids_by_currency = {};
        //var map_currencies = tbl_bittrex_currencies.get_obj_map_bittrex_currencies_ids_by_name
        each(tbl_bittrex_currencies.records.arr_records, (record) => {
            //console.log('record', record);
            map_ids_by_currency[record.value[0]] = record.key[0];
        });
        // May need to reset the iterator.
        console.log('map_ids_by_currency', map_ids_by_currency);
        * /

        //throw 'stop';

        this.bittrex_watcher.get_at_all_currencies_info((err, at_all_currencies) => {
            if (err) {
                callback(err);
            } else {
                console.log('at_all_currencies', JSON.stringify(at_all_currencies));
                //throw 'stop';
                that.ensure_bittrex_currencies(at_all_currencies.values, callback);

            }
        });
    }
    */

    /*
    download_ensure_bittrex_markets(callback) {
        console.log('download_ensure_bittrex_markets');




        var that = this;
        var tbl_bittrex_currencies = this.model.map_tables['bittrex currencies'];
        // get a map of these records, so that we have name -> id
        var map_ids_by_currency = {};
        //var map_currencies = tbl_bittrex_currencies.get_obj_map_bittrex_currencies_ids_by_name

        // We have got the currencies, but we need to ensure them into the database.
        //  Want an easier way to ensure that data that gets downloaded is in the DB.

        // Want this to specifically notice new coins and currencies.

        // Need multi-ensure for the client
        //  Ensure the model is loaded properly from remote
        //  Update the local model
        //  Persist those changes to remote.

        // The ensure stage could be multi-pronged.

        // A simple system to ensure data from online gets put into the DB effectively would be useful.
        //  Maybe a web interface to set them up.
        //  Define the API calls
        //   Define what happens with the result data.
        //   GUI map of the DB would be helpful to drag result sets to tables / icons


        // Getting the server to automatically index records that get put would be useful.
        //  Would not use simple ll_put, but would use index_put.


        // Also, a means on the server side to ensure data records would help, and would return info about if records were overwritten / put at all / not put.

        // Some ll_db helper functions / methods could be of use too.
        //  Such as server-side index maintenance.

        // 30/12/2017, this is my best chance to spend the time to make significant upgrades to the DB.

        // Server-side indexing of records becoming more routing with the client would help.
        //  Then that would help server side put without overwrites on key fields.

        // Some functions could start to use promisified interfaces.

        // 









        each(tbl_bittrex_currencies.records.arr_records, (record) => {
            //console.log('record', record);
            map_ids_by_currency[record.value[0]] = record.key[0];
        });
        // May need to reset the iterator.
        console.log('map_ids_by_currency', map_ids_by_currency);
        throw 'stop';
        //console.log('map_ids_by_currency["BTG"]', map_ids_by_currency["BTG"]);
        console.log('tbl_bittrex_currencies.records.arr_records.length', tbl_bittrex_currencies.records.arr_records.length);
        //throw 'stop';
        this.bittrex_watcher.get_at_all_markets_info((err, at_all_markets) => {
            if (err) {
                callback(err);
            } else {

                

                //console.log('at_all_markets', at_all_markets);
                // then get the array which contains the currency references
                var market_records = [];
                each(at_all_markets.values, v => {

                    // Do we already have a record for that market?



                    //console.log('v', v);
                    var str_market_currency = v[0];
                    var str_base_currency = v[1];
                    //arr_market_names.push(str_market_currency + '-' + str_base_currency);
                    //arr_market_names.push(str_base_currency + '-' + str_market_currency);

                    var market_currency_key = map_ids_by_currency[str_market_currency];
                    var base_currency_key = map_ids_by_currency[str_base_currency];

                    //console.log('str_market_currency', str_market_currency);
                    //console.log('str_base_currency', str_base_currency);

                    var market_currency_id = market_currency_key[0];
                    var base_currency_id = base_currency_key[0];
                    //console.log('market_currency_id', market_currency_id);
                    //console.log('base_currency_id', base_currency_id);
                    // then reconstruct the record. We need the right fields for the record.
                    var record_def = [
                        [market_currency_id, base_currency_id], v.slice(4)
                    ];


                    market_records.push(record_def);
                });
                //throw 'stop';
                // Then need to recreate the markets records.
                console.log('market_records', market_records);
                that.ensure_bittrex_markets(market_records, callback);
            }

        })
    }

    */



    // Downloading to the 

    /*

    download_ensure_bittrex_currencies_markets(callback) {

        console.log('download_ensure_bittrex_currencies_markets');
        / *
        Fns([
            [this.download_ensure_bittrex_currencies, this],
            [this.download_ensure_bittrex_markets, this]
        ]).go(callback);
        * /

        // Do it together...

        // Dont want to redownload all bittrex, as we already have keys for existing records.

        // Makes most sense to add the records into an existing model.
        //  Have the local model with Bittrex data from the server

        // Load the local model from the server
        //  Ensure that model has got the necessary records.
        //   (reindex if necessary)

        // We can use a new Model to download the current data from the server.

        var model = new Crypto_Model.Database();
        var client = this,
            bw = this.bittrex_watcher;

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
            }
        });



        /*

        client.load_core_plus_tables(['bittrex markets', 'bittrex currencies'], (err) => {
            // Seems like it has not set up pk_incrementor of the table.

            if (err) {
                callback(err);
            } else {
                
            }

            //download the data


            
            / *

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

            * /
        })

        * /


    }
    */



    // others will be download_ensure

    /**
     * 
     * 
     * @param {array} arr_currencies 
     * @param {any} callback 
     * @memberof Assets_Client
     */

    // Only ensures them into the model.
    // May be better to have this function just in the model.

    /*


    ensure_bittrex_currencies(arr_currencies, callback) {
        // need to get a map of all currencies by id.
        //  Notso sure about using new copies of the Model all of the time.
        //  May be best to ensure they are in the client-side model as well as the remote DB.
        //   Transfer data from the local model first?



        var that = this;
        var model = that.model;

        console.log('1) ensure_bittrex_currencies arr_currencies.length', arr_currencies.length);
        //console.log('Object.keys(model)', Object.keys(model));

        // The table should have a pk incrementor set up if the records require it.
        // 

        // No overwrite of indexed records.

        // Could get back the number that are added to the model.

        var arr_added = model.ensure_table_records_no_overwrite('bittrex currencies', arr_currencies);

        console.log('arr_added', arr_added);


        // This should do for the moment.
        //  Want to be able to run this on a server soon.
        //  Could do raspberry pis too.



        //throw 'stop';
        //console.log('arr_currencies', JSON.stringify(arr_currencies));

        // Ensures they are in the client-side model.
        //  Have not been ensured on the server yet though.




        //throw 'stop';

        callback(null, true);

        // Time to send these to the database?
        //console.trace();

        // Better to load the local / new crypto model with data from the db about these currencies.



        // load a new model with the data from the db server.

        // We have the currencies, so put them into the model.

        //var crypto_model = this.model || new Crypto_Model.Database();
        //console.log('crypto_model', crypto_model);

        / *
        crypto_model.download_ensure_bittrex_currencies((err, res_ensure) => {
            if (err) { callback(err); } else {
                //console.log('download_ensure_bittrex_currencies res_ensure', res_ensure);


                callback(null, res_ensure);
            }
        });

        * /




        // get the new records from a diff.








        / *

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
        * /

    }
    */

    /*
    ensure_bittrex_markets(arr_markets, callback) {
        // need to get a map of all currencies by id.
        //  Notso sure about using new copies of the Model all of the time.
        //  May be best to ensure they are in the client-side model as well as the remote DB.
        //   Transfer data from the local model first?

        var that = this;
        var model = that.model;

        console.log('1) ensure_bittrex_markets arr_markets.length', arr_markets.length);
        //console.log('Object.keys(model)', Object.keys(model));

        // The table should have a pk incrementor set up if the records require it.

        // should be able to see how many records were written.
        //  Have the indexes to the markets been set up properly?

        console.log('arr_markets', arr_markets);
        console.trace();
        throw 'stop';


        var arr_added = model.ensure_table_records_no_overwrite('bittrex markets', arr_markets);
        console.log('arr_added', arr_added);
        //console.log('arr_currencies', JSON.stringify(arr_currencies));
        throw 'stop';

        callback(null, true);
    }
    */

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
                            fns.push([that, that.get_bittrex_market_snapshots_time_range, [
                                [market_value[0], market_value[1]]
                            ]]);
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
                        if (err) { throw (err); } else {
                            //console.log('kv_fields', JSON.stringify(kv_fields, null, 2));
                            //console.log('kv_fields', (kv_fields[0]));
                            //console.log('kv_fields', (kv_fields[1]));


                            client.get_table_field_names('bittrex market summary snapshots', (err, fields) => {
                                if (err) { throw (err); } else {
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