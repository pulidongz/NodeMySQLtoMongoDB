import mysql from 'mysql2';
import moment from 'moment';
import { MongoClient }from 'mongodb';

let options = {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  }

const mongo_url = 'mongodb://localhost:27017/';

let promisePool = null;

let tableList = [];

const migrate_table = async(table_name, database) => {

    await promisePool.execute(`SELECT * FROM ${table_name};`)
    .then(([rows, fields]) => {
        MongoClient.connect(mongo_url, options, (err, db) => {
            if(err) throw err;
            console.log(`\nConnected to MongoDB: ${database}`);
            let dbo = db.db(database);
            console.log(`Migrating data from table: ${table_name}`);
            rows.forEach(async(row) => {
                await dbo.collection(table_name).updateOne({_id: row._id}, {$set: row}, {upsert: true}, (err, task) => {
                    if (err) {
                        console.log(err);
                    }
                });
            });
        });
    })
    .catch((err) => { return err; });
}

const setValue = (value) => {
    tableList = value;
  }

const get_table_list = async() => {
    await promisePool.execute("SHOW TABLES;")
    .then(([rows, fields]) => {
        rows = rows.map(row => row[Object.keys(row)[0]]);
        setValue(rows);
    })
    .catch((err) => { return err; });
}

async function migrate(db){
    const pool = mysql.createPool({
        host: 'localhost',
        user: 'pul',
        password: 'admin',
        database: db,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0
    });
    promisePool = pool.promise();

    // Get MySQL db table lists
    await get_table_list();

    // Extract rows from table and insert to MongoDB
    tableList.forEach(async (table) => {
        await migrate_table(table, db);
    });
}

async function main(){
    const dbList = [
        // 'cbewsl_commons_db',
        // 'cbewsl_mar_collections', 
        // 'cbewsl_umi_collections', 
        'comms_db', 
        // 'senslopedb'
    ];

    const begin_time = await moment();
    console.log(`Script started at ${begin_time}`);

    dbList.forEach(async (db) => {
        await migrate(db);
    });

    const end_time = await moment();
    console.log(`Script completed at ${end_time}`);
    console.log(`Total time: ${end_time.diff(begin_time, 'seconds')} seconds`);
}

main();