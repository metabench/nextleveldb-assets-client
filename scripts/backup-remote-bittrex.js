const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;
const path = require('path');

//console.log(path.resolve('../../../config/config.json'));

var config_path = path.resolve('../../../config/config.json');
var config = require('my-config').init({
	path : config_path//,
	//env : process.env['NODE_ENV']
	//env : process.env
});
console.log('config_path', config_path);

const Assets_Client = require('../nextleveldb-assets-client');


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

            var test_live_btc_eth = () => {
                var live_hist_btc_eth = client.live_bittrex_snapshots('BTC-ETH');
                // Will return an object which itself processes events, as it gets the data back from the server.
                //  Server-side subscriptions are a good way to do this.

            }
            //test_live_btc_eth();
            // Loading the core does not work (any longer) because the fields get loaded wrong.
            //  Could have bug fix swap, now the bug has been found and fixed.
            //console.log('pre load core');
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
        }
    });
} else {
    //console.log('required as a module');
}

//module.exports = Assets_Client;
