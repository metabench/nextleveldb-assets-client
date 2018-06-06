// Giving data types could be useful here.
//  eg ['market_currency_id fk=> bittrex currencies', 'base_currency_id fk=> bittrex currencies'],  maybe would not need to be specified because the FK types match the referenced PK types

// + will be made to firmly mean autoincrementing 0 based integer. This will be made as as type within the model.
//  Need to ensure that the model sets that data automatically, and that it's an intrinsic (native) type within the DB.

// Getting closer to being able to (more) automatically map one record type to another.
//  Still need this in order to most effectively add the Bittrex Markets records.

// A new version of assets client may well be best.
//  Make it work for all exchanges.
//   Indexing records is harder if they don't have a unique assigned key???
//    Well, every record has a key
//   Could index the snapshot or trade records.


// Seems like getting a new version up and running very soon would be best.
//  Run it on a new DB, import the data from elsewhere.
//   Saved backups could be imported. Import => transpose. 

// Very soon, want it handling lots of data from multiple exchanges.
//  Running means per second would be of great use.



// Likely seems best to make the new multi-exchange (multi-data-source too) version and use that.
//  Some data sources maybe are not exchanges, eg CMC.
//  Data provider, with 'tag' or option of 'Exchange'.

// Maybe record tags would be a useful feature. Or flags. Quick reading to see which flags are there, and to add and delete them.
// A new version for data from all exchanges / sources would be best.

const tables = [
    [
        'market providers', [
            [
                ['+id'],
                ['name']
            ],
            [
                [
                    ['name'],
                    ['id']
                ]
            ]
        ]
    ],
    [
        'bittrex currencies', [
            '+id',
            '!Currency',
            '!CurrencyLong',
            'MinConfirmation',
            'TxFee',
            'IsActive',
            'CoinType',
            'BaseAddress',
            'Notice'
        ]
    ],
    [
        'bittrex markets', [
            ['market_currency_id fk=> bittrex currencies', 'base_currency_id fk=> bittrex currencies'],
            ['MinTradeSize',
                '!MarketName',
                'IsActive',
                'Created',
                'Notice',
                'IsSponsored',
                'LogoUrl'
            ]
        ]
    ],
    [
        'bittrex market summary snapshots', [

            // This is a bit tricky, being a composite field.
            //  A single field here refers to the whole primary key of another table.
            //  That could be a composite field.
            //  This will take thought and testing.
            //   Need to be able to look at the field value and notice it refers to two fields.
            //   Know that the foreign key refers to two fields - be aware of that possibility in all relevant places.

            // They type of the market_id field here is directly the type of the pk of bittrex markets
            //  Get the markets working right, be able to refer to records there by their PKs.

            // Condidering a lower level row timestamp too
            //  making it possible to retrieve all records added after time x.

            ['market_id fk=> bittrex markets',
                'timestamp'
            ],
            ['last',
                'bid',
                'ask',
                'volume',
                'base_volume',
                'open_buy_orders',
                'open_sell_orders'
            ]
        ]
    ]
]

module.exports = tables;