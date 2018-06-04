// May well need a way to add fields to records.
//  For example more info on specific coins as new info becmes available
//  Coin type
//   Different exchanges have different ways of expressing that.

// Snapshots
// Streaming history
// Get recent trades


// Kucoin - no streaming
//  Combining snapshots with get recent trades.
//   Get recent trades is just for one exchange
//  "_comment": "arr[0]   Timestamp arr[1]   Order Type arr[2]   Price arr[3]   Amount arr[4]   Volume",
//  

// Snapshot every second?

// Snapshot from a number of different exchanges (2 for the moment)

// Could have standardised OO trade data.
//  Or subclasses of an Active_Record or Active_Table.

// OO classes would definitely help to structure the data.
//  Convert data from the exchanges to the format for the DB. Holding in an intermediate format too.
//   Would possibly create/output the DB records for the trades / snapshot data.


// kucoin, bittrex, binance

// then trades
//  some exchanges will provide more data about individual trades
//   trade_id
//   we may need to make a new hash key for each trade
//    that would ensure uniqueness.
//   We get trades as an array, just presume each trade sent by the exchange is unique.

// Then streamed trades - a different way to get the trades.
//  All all trade info is to be deemed to be 


// Assets db

// .info could often be unstructured data in a JS obj.

// exchange_trade object
//  makes sense to have one.


// Should be able to get the snapshots, and the trades.
//  Snapshots provide some info about the last trade, but its not the trading history.

// Need data strucures to hold the assets data.
//  When there is no exchange-supplied ID, for trades, it will need to generate one.
//   Could look out for identical trades together, but for the moment, ignore it. Could apply batch number variables.


//  have them all in the key, but they can be null. Need at least one of them?
// exchange market trade id?
// exchange trade id?
// hash (generated) id?

// Exchange market and timestamp should be fine.


// exchange_trade_id
//  exchange_market_trade_id ~(seems like this on bittrex)
//   if the exchanges have trade ids per market.

// just a trade id field.
//  one will be fine.
//  we get that from the server, combined with exchange_market it will be unique

// fill_type
//  partial fill, fill
//  

// Could even store the means to translate the data from exchanges to our format.
//  However, making the asset classes capable of accepting the raw crypto data would be fine.


// Need to be able to store candlestick data.
// Economic data - readings at times.
//  Could be relatively simple time series.
//  just a time-series class would be cool. Time, then a value.
//  Or multiple values for each of them. Can be flexible.
//  Worth having a timeserieses table
//   


// then info field
//  begin timestamp
//  end timestamp

// This almost looks fine to be the basis on which a lot of data can be collected.


// Getting the fields definition more like SQL will be helpful.



// Possibility of storing HTTP requests / responses / domains here?
//  Not worth it, just stick with the time serieses and timed value data from exchanges


// Candlesticks though

// look at bittrex candlesticks.



// candlestick data
//  exchange_market, start_time, (timespan), end_time



//  exchange_market, timespan, start_time, (end_time), open, low, high, close, volume
//  exchange_market, timespan, start_time, open, low, high, close, volume




//  index by exchange_market, timespan


// Possibly tagging data flaws
//  eg if data was estimated / interpolated?

// Probably best for the moment to make this a capable logging system, be sure to log all of the data.
//  May still need to do some error correction on data from some servers.
//  Would be worth putting that data into a db of this structure.


// Estimated / indirectly sourced data?
//  Would definitely be worth thinking of how this could be incorporated.
//  Different layer would likely be best.

// probably move to just the financial data module?
//  Want it so that the 


// ! together make a PK

// want indexed rows
// indexed rows with enforced uniqueness

// Seems like we need a way to enforce uniqueness with coins, including their codes and names
//  Individual uniqueness
//  This feature seems relatively important.
//  Reliability somewhat depends on it.

// pks are always unique anyway

// Storing and enforcing unique constraints / table record constraints makes sense.
//  Definitely seems like the way to do this.

// Have a table constraints table, save it and load it as before.
//  Will be useful with put and ensure

// Will definitely help in ensuring and using records corresponding to coins and markets on exchanges.
//  Seems like an ommission so far not to have these features.
//  Worth putting them in before the next iteration of the db.
//   Will wind up doing checks in the DB before actually putting data. Will avoid overwriting records.


const tables = [


    // standard info on the coin types.

    // Coins being renamed would have multiple records.

    //['coins', ['+id', 'code', 'name']],
    ['coins', ['+id', 'code(unique)', 'name(unique)']],


    ['exchanges', ['+id', 'name']],

    // cointype?

    // unstructures further info about the coin.
    //  an object for info would be a plain JS object with fields and values. Not compressed or normalised here.
    //  will help to keep the data, then view that data as obj.
    ['exchange coins', ['!exchange_id fk=> exchanges', '!coin_id', 'info']],

    // keep reference to the coins on that exchange
    ['exchange markets', ['!market_exchange_coin fk=> exchange coins', '!base_exchange_coin fk=> exchange coins']],

    // IsActive, Created (from bittrex)
    // summary snapshots

    // timestamp has to be unique?
    //  or combined, they are unique, part of the key.
    ['exchange market summary snapshots', ['!exchange_market_id fk=> exchange markets', '!timestamp', 'last', 'bid', 'ask', 'volume', 'base_volume', 'open_buy_orders', 'open_sell_orders']],

    // different types of volume
    //  volume in that currency (amount) and volume in the base currency
    ['exchange trades', ['!exchange_market_id fk=> exchange markets', '!timestamp', 'id', 'price', 'amount', 'volume', 'is_buy', 'is_partial_fill']],


    // Timespan in milliseconds. There will be a variety of values, but not all that many will be used, eg 60000 for 1 minute, 5! for one day.
    ['exchange candlesticks', ['!exchange_market_id fk=> exchange markets', 'timespan', 'start_time'], ['open', 'low', 'high', 'close', 'volume']],

    // and data from indexes could just go into timeserieses.
    //  more meant for misc timeseries data

    ['timeserieses', ['+id', 'name', 'begin', 'end'], ['info']],
    // want to index by name though.

    // No fields specified by default, can have any fields that are relevant to the time series.
    ['timeseries values', ['!timeserieses_id fk=> timeserieses', '!timestamp'], []]

];