
// fin data table

// normal client
//  on start, ensure that db def
//   ie ensures all the fin tables

const fin_data = require('fin-data');
const db_def = fin_data.nextleveldb_def;
const path = require('path');
const lang = require('lang-mini');
const each = lang.each;

const NextLevelDB_Client = require('nextleveldb-client');
const fnl = require('fnl');
const prom_or_cb = fnl.prom_or_cb;

const Bittrex_Watcher = require('bittrex-watcher');
//const Active = require('nextleveldb-active');

const Active_Database = require('../nextleveldb-active/active-database');

class NextLevelDB_Fin_Client extends NextLevelDB_Client {
    constructor(spec) {
        super(spec);
    }

    start(callback) {
        //console.log('this.super', this.super);


        return prom_or_cb((resolve, reject) => {
            (async () => {


                console.log('pre super');
                await super.start();
                console.log('post super');

                console.log('db_def', db_def);

                await this.ensure_tables(db_def);
                console.log('post ensure tables');

                // Then functions to ensure various records.
                //  Could keep track of the table data using Active_Table





                // When it says its started, it needs to have ensured the structure.



                // Ensure structure from oo snapshots.



                let obs_bittrex_snapshots = Bittrex_Watcher.watch_bittrex_snaphots(10000);
                //console.log('obs_bittrex_snapshots', obs_bittrex_snapshots);
                obs_bittrex_snapshots.on('next', data => {
                    console.log('data.length', data.length);
                });

                resolve();





            })();
        }, callback);


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
        // Should have ensured the db structure.

        // ensure the right table structure at one time?
        console.log('client started');
        //let tbl_coins = client.model

        // Model tables, and active tables.
        // Lets get the active db.

        // Ensuring the bittrex / fin structure? Fin client will do that automatically.

        let active = new Active_Database(client);

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
        let obs_snapshots = Bittrex_Watcher.watch_bittrex_snaphots(10000);
        // Need to see if we have got these necessary data objects.
        //  Would be useful to have arrays of the oo objects with the market snapshots.
        //   But likely we would need to get them elsewhere.
        // obs_snapshots.on('supplemental');
        // data.market_snapshots
        // data.data
        // data.basis

        obs_snapshots.on('next', async data => {
            console.log('data', data);
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


            for (let item of Object.entries(map_exchange_coins)) {
                //console.log('item', item);
                let coin_code = item[0];
                let coin = item[1].coin;

                let res_ensure = await at_coins.ensure(coin);
                console.log('res_ensure', res_ensure);

            }

            console.log('coins have been ensured');

            // Then ensure the exchange coins.


            /*



            each(map_exchange_coins, (exchange_coin, i, stop) => {
                //console.log('exchange_coin', exchange_coin);
                let coin = exchange_coin.coin;
                //console.log('coin', coin);

                // ensure that exchange coin.
                //  we could cache 'ensure' results.
                //  maybe want to keep a local version of the table?
                //   ok with coins and some others.

                // Get the db record from the coin.
                //  A buffer-backed pure record rather than row could help.
                //  Then we do equivalent model functionality using the buffered data.

                // active table put record / ensure record
                //  where that record comes from...

                // Would like active_table to be capable of doing what it needs.

                // Should maybe use a server-side ensure.
                //  This way we don't get multiple clients creating the same record

                // Be able to send the record to the server, have the server generate the new ID.
                //  Or send it an 'undefined' id.
                //  Have the server generate that ID.
                //  Need to avoid race conditions this way.
                //   Thinking about possibility of record locks too....

                // Best to send the coin record, with a placeholder (undefined) saying it needs a new ID if it's not got one.

                // active-table should handle interface between object and db data.

                console.log('pre at ensure coin');


                (async () => {
                    let res_ensure = await at_coins.ensure(coin);
                    console.log('res_ensure', res_ensure);


                    console.log('post at ensure coin');
                })();


                stop();

                //let res_put = at_coins.put(exchange_coin)
                // Should not be all that hard to use active records.
                // Need to determine if such a coin exists on the server.
            });

            */
            //console.log('data.length', data.length);
            // Then for each data item, we have the oo records available.
        });
        //let client = new NextLevelDB_Fin_Client()
    })();


}