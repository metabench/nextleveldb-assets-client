
const fin_data = require('fin-data');
const Exchange = fin_data.Exchange;
//const Coin = 

/*
Coin: require('./coin'),
Coin_On_Exchange: require('./coin-on-exchange'),
Exchange: require('./exchange'),
Exchange_Market: require('./exchange-market'),
Trade: require('./trade'),
Market_Snapshot: require('./market-snapshot'),
nextleveldb_def: require('./nextleveldb-def')
    */

//const

// Does not seem like much need for the collector with this available in the client.





const db_def = fin_data.nextleveldb_def;
const path = require('path');
const lang = require('lang-mini');
const each = lang.each;

const NextLevelDB_Client = require('nextleveldb-client');
const fnl = require('fnl');
const prom_or_cb = fnl.prom_or_cb;




const Bittrex_Watcher = require('bittrex-watcher');
//const Active = require('nextleveldb-active');

const Active_Database = require('nextleveldb-active');

// Then an active record would have the b_record attached.

class NextLevelDB_Fin_Client extends NextLevelDB_Client {
    constructor(spec) {
        super(spec);
    }


    // May as well have a function to collect bittrex snapshots.




    start(callback) {
        //console.log('this.super', this.super);


        return prom_or_cb((resolve, reject) => {
            (async () => {


                //console.log('pre super');
                await super.start();
                //console.log('post super');

                console.log('db_def', db_def);

                await this.ensure_tables(db_def);
                console.log('post ensure tables');

                // Model should be OK..

                await this.load_core();

                //throw 'stop';

                // Then functions to ensure various records.
                //  Could keep track of the table data using Active_Table

                // When it says its started, it needs to have ensured the structure.
                //  Much of the necessary code has been abstracted away.





                // Ensure structure from oo snapshots.



                /*

                let obs_bittrex_snapshots = Bittrex_Watcher.watch_bittrex_snaphots(10000);
                console.log('obs_bittrex_snapshots', obs_bittrex_snapshots);

                obs_bittrex_snapshots.on('next', data => {
                    console.log('data.length', data.length);
                });
                */

                resolve();

            })();
        }, callback);


    }

    // observable too

    collect_bittrex() {

        (async () => {

            let active = new Active_Database(this);
            let map_active_coins_by_code = {};


            // Wait until it is ready.
            //  Needs to load up its tables I think.
            //  Maybe something, not sure.


            console.log('!!active', !!active);
            console.log('Object.keys(active)', Object.keys(active));

            // then the active table

            let at_coins = active['coins'];
            console.log('!!at_coins', !!at_coins);


            console.log('Object.keys(at_coins)', Object.keys(at_coins));

            console.log('at_coins.model.name', at_coins.model.name);
            console.log('at_coins kv_fields', at_coins.kv_fields);

            //throw 'stop';

            // then use the active coins table to put records.
            //  how about active records with validation?
            //throw 'stop';

            let at_exchanges = active['exchanges'];

            // count the records in that table...
            //console.log()
            console.log('pre count exchanges');
            let count_exchanges = await at_exchanges.count();
            console.log('count_exchanges', count_exchanges);

            // 

            const bittrex = new Exchange({
                name: 'Bittrex'
            });



            //at_exchanges.ensure({}'bittrex');


            /*
            // Could put this into fnl.
            let delay = async (ms) => {
                return new Promise((resolve, reject) => {
                    setTimeout(() => {
                        resolve();
                    }, ms)
                });
    
            }
            await delay(1000);
            */

            // get the bittrex active record.

            // just get the only property that is indexed.

            let ar_bittrrex_exchange = await at_exchanges.ensure(bittrex);
            console.log('ar_bittrrex_exchange', ar_bittrrex_exchange);

            // then the ar could have a key
            console.log('ar_bittrrex_exchange.key', ar_bittrrex_exchange.key);
            console.log('ar_bittrrex_exchange.key.decoded', ar_bittrrex_exchange.key.decoded);
            console.log('ar_bittrrex_exchange.decoded', ar_bittrrex_exchange.decoded);

            // then with the coins on exchange
            //  

            // .id
            //  that is a property lookup on the Active_Record.

            //throw 'stop';


            let at_exchange_coins = active['exchange coins'];
            let at_exchange_markets = active['exchange markets'];

            // exchange market summary snapshots
            //let at_market_snapshots = active['market snapshots'];
            let at_market_snapshots = active['exchange market summary snapshots'];
            //console.log('!!at_exchange_coins', !!at_exchange_coins);

            //throw 'stop';

            let obs_snapshots = Bittrex_Watcher.watch_bittrex_snaphots(1000);
            // pause it once we have the first results?

            // Ideally want to get the initial snapshotts, save them, and then proceed with the interval.
            //  Also, batching of records to put could help.



            // Need to see if we have got these necessary data objects.
            //  Would be useful to have arrays of the oo objects with the market snapshots.
            //   But likely we would need to get them elsewhere.
            // obs_snapshots.on('supplemental');
            // data.market_snapshots
            // data.data
            // data.basis



            // map of exchange coins by market name...

            let map_bittrex_exchange_coins_by_code = {};
            let map_bittrex_markets_by_name = {};

            let ensure_coins = async (map_exchange_coins) => {

                for (let item of Object.entries(map_exchange_coins)) {
                    //console.log('exchange_coin item', item);
                    let coin_code = item[0];
                    if (!map_bittrex_exchange_coins_by_code[coin_code]) {
                        let exchange_coin = item[1];
                        let coin = exchange_coin.coin;
                        //console.log('pre ensure item');
                        // Not working quite right.
                        let active_coin_record = await at_coins.ensure(coin);
                        // No, it should have the key in the returned record.
                        map_active_coins_by_code[coin_code] = active_coin_record;
                        exchange_coin.coin_id = active_coin_record.key.decoded[1];
                        exchange_coin.exchange_id = ar_bittrrex_exchange.key.decoded[1];
                        //console.log
                        //console.log('item', item);
                        // copy the exchange coin properties over?
                        let ar_exchange_coin = await at_exchange_coins.ensure(exchange_coin);
                        //console.log('ar_exchange_coin.decoded', ar_exchange_coin.decoded);
                        map_bittrex_exchange_coins_by_code[exchange_coin.coin.code] = ar_exchange_coin;
                    }

                }
                console.log('coins have been ensured');

                return true;


            }

            // Won't need to create new ARs if we keep them cached.

            let ensure_markets = async (map_exchange_markets) => {
                for (let market of Object.entries(map_exchange_markets)) {
                    //console.log('market', market);
                    let [code, exchange_market] = market;
                    if (map_bittrex_markets_by_name[code]) {

                    } else {
                        exchange_market.exchange_id = ar_bittrrex_exchange.key.decoded[1];
                        exchange_market.market_exchange_coin_id = map_bittrex_exchange_coins_by_code[exchange_market.market_exchange_coin.coin.code].key.decoded[2];
                        exchange_market.base_exchange_coin_id = map_bittrex_exchange_coins_by_code[exchange_market.base_exchange_coin.coin.code].key.decoded[2];
                        //console.log('exchange_market', exchange_market);
                        //console.log('     -----     ');
                        let ar_exchange_market = await at_exchange_markets.ensure(exchange_market);
                        //console.log('ar_exchange_market', ar_exchange_market);
                        //console.log('ar_exchange_market.decoded', ar_exchange_market.decoded);
                        map_bittrex_markets_by_name[exchange_market.name] = ar_exchange_market;
                    }

                }
                console.log('markets have been ensured');
                return true;
            }





            obs_snapshots.on('next', async data => {
                //console.log('data', data);
                //throw 'stop';

                // The data could also come with neecessary arrays of objects.
                //  All exchanges (mentioned)
                //  All coins (mentioned)
                //  All exchange coins (mentioned)
                //  All exchange markets (mentioned)

                //console.log('Object.keys(data)', Object.keys(data));

                let { market_snapshots, map_exchange_coins, map_exchange_markets } = data;

                //console.log('Object.keys(map_exchange_coins)', Object.keys(map_exchange_coins));
                //console.log('Object.keys(map_exchange_markets)', Object.keys(map_exchange_markets));

                // Ensure coins
                // Ensure exchange coins
                // Ensure exchange markets
                // Put market snapshot records

                // Want the core operations to be done in a very small amount of code on this level.

                console.log('Object.keys(map_exchange_coins).length', Object.keys(map_exchange_coins).length);
                console.log('Object.keys(map_exchange_markets).length', Object.keys(map_exchange_markets).length);

                // Then, looks like we need an Active_Record for each of them.

                // Active_Record will get the record id if it already exists
                //  If it does not exist, it will create the record.

                // Would need to query an Active_Table to see if the record exists.
                //  Maybe do some more advances with active-record / nextleveldb and bringing it to the client.

                // Really want a simple procedure in the fin client or collector to get and ensure active records for all of these.

                // ensure all of these...

                // Want to be able to check for a wide variety of indexes / keys at once.
                // But can we do a for of loop?

                //console.log('map_exchange_coins', map_exchange_coins);

                // an async each?
                //  Will want to ensure all of these at once.

                // async iterator of object?




                // The put loop is quite quicj anyway/

                let ensure_market_snapshots = async () => {


                    //map_bittrex_exchanges_by_name

                    // 
                    //console.log('at_exchange_markets.id', at_exchange_markets.id);

                    for (let market_snapshot of market_snapshots) {
                        // access the active exchange market
                        let ar_em = map_bittrex_markets_by_name[market_snapshot.exchange_market.name];
                        //console.log('ar_em', ar_em);
                        //console.log('ar_em.decoded', ar_em.decoded);
                        let decoded = ar_em.decoded;
                        // then the id from that.
                        //console.log('market_snapshot', market_snapshot);
                        // But the markets don't themselves have IDs.
                        //  Maybe they should.
                        // it's exchange_market_key ?
                        //  but not the whole key, not the kp
                        // decoded_key_no_kp
                        // decoded_no_kp
                        market_snapshot.exchange_market_key = ar_em.decoded_key_no_kp;
                        //console.log('!!at_market_snapshots.put', !!at_market_snapshots.put);

                        // Batch create active records?

                        // Or debounce the put calls?
                        //  Send them in a batch to be processed in a batch.
                        //  Or could put multiple records.

                        // put_multi
                        // put(arr of records or record_list)

                        // Don't want it to take 1 to 2 s to save the bittrex records.
                        //console.log('market_snapshot', market_snapshot);

                        // Putting these snapshots all at once would be fastest.
                        //  queue_put(record)
                        //  await queue();


                        // Batches of normal operations will be useful.
                        //  These are no a standard batch_put operation.

                        // Active table could have a buffer of operations to run.
                        //  Will send them all as one message.

                        // It's at the stage now where it can run on a machine.
                        //  Batching of the put operations would definitely save on resources.
                        //   

                        let ar_snapshot = await at_market_snapshots.put(market_snapshot);
                        //console.log('ar_snapshot.decoded', ar_snapshot.decoded);
                        // Seems to actually put the data now.
                        //  Primary keys with multiple fields can be put in an array so they count as a single field.
                        //throw 'stop';
                    }
                    return true;
                }


                // Will have some kind of cache to skip this quicker - 
                //  however always need to check in case a new coin / market has just been added.

                await ensure_coins(map_exchange_coins);
                //console.log('map_bittrex_exchange_coins_by_name', map_bittrex_exchange_coins_by_code);
                await ensure_markets(map_exchange_markets);
                await ensure_market_snapshots();

                // do this once, finish before setting interval.

                console.log('market snapshots put');
                // await ensure_snapshot_records();

                // Then will need to ensure the snapshot records
                //  After this is's likely going to be worth doing more client-side work.

                // then ensure the markets
                // then ensure the snapshots



                //console.log('data.length', data.length);
                // Then for each data item, we have the oo records available.
            });

        })();




    }

    // Basically want to add table records in a simple way.
    //  Maybe references to the table would help.


    // May go more of an active table / active record route to putting the data into place.
    //  Now seems like outputting a standard row format will be easy enough...

    // ensure_snapshots({
    //   ensure_table_records('snapshots', snapshots.to_records())
    //})

}


module.exports = NextLevelDB_Fin_Client;
if (require.main === module) {
    (async () => {
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
        // give a db_name
        //  

        var client = new NextLevelDB_Fin_Client(local_info);
        console.log('pre client start');
        await client.start();

        // have client reload the model?
        // Should have ensured the db structure.

        // ensure the right table structure at one time?
        console.log('client started');

        client.collect_bittrex();
        //let tbl_coins = client.model

        // Model tables, and active tables.
        // Lets get the active db.

        // Ensuring the bittrex / fin structure? Fin client will do that automatically.


        //let client = new NextLevelDB_Fin_Client()
    })();


}