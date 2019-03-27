const https = require("https")
const parser = require('xml2js').parseString;
const Database = require('better-sqlite3');
//const db = new Database('./database/ft.oda.sqlite.db');

var types = { "Edm.Int32": "INTEGER", "Edm.Int16": "INTEGER", "Edm.String": "TEXT", "Edm.DateTime": "INTEGER", "Edm.Boolean": "INTEGER" }
var dbs = [] 
function createDatabase() {
    return new Promise((resolve, reject) => {

        query("https://oda.ft.dk/api/$metadata").then(parseXML).then((data) => {
            var entities = data['edmx:Edmx']['edmx:DataServices'][0]['Schema'][0]['EntityType']
            for (var i = 0, n = entities.length; i < n; i++) {
                var tableName = entities[i]['$'].Name
                var properties = entities[i]['Property']
                var propertySQLs = []
                for (var j = 0, k = properties.length; j < k; j++) {
                    var property = properties[j]['$'].Name
                    var type = properties[j]['$'].Type
                    var nullable = properties[j]['$']["Nullable"]
                    var sql = property + " " + types[type];

                    if (property === "id") {
                        sql += " PRIMARY KEY"
                    }

                    if (nullable) {
                        sql += " NOT NULL"
                    }

                    propertySQLs.push(sql)
                }
                var db = new Database(`./databases/ft.oda.${tableName}.sqlite.db`);
                var sql = `CREATE TABLE IF NOT EXISTS ${tableName}(`;
                sql += propertySQLs.join(",")
                sql += ");"
                var stmt = db.prepare(sql);
                stmt.run()
                db.close()
                dbs.push(tableName)
            }
            resolve()
        }).catch(reject)
    })
}

async function updateDatabase() {


    for (var i = 0, n = dbs.length; i < n; i++) {
        var table = dbs[i]
        var db = new Database(`./databases/ft.oda.${table}.sqlite.db`);
        var dbid = db.prepare(`SELECT id FROM ${table} ORDER BY id DESC LIMIT 1;`).get()
        var dbCount = db.prepare(`SELECT count(*) FROM ${table};`).get()
        
        dbid = !dbid ? 0 : dbid.id
        dbCount = !dbCount ? 0 : dbCount['count(*)']
        console.log(`Checking if ${table} data is up to date!`)
        await updateTable(table, dbid, dbCount, db)
        db.close()
    }
}


async function updateTable(table, dbID, dbCount, db) {

    var remoteID, remoteCount;
    try {
        data = await query(`https://oda.ft.dk/api/${table}?$top=1&$orderby=id%20desc&$inlinecount=allpages`)
        data = JSON.parse(data)
        remoteID = data['value'][0].id
        remoteCount = data['odata.count']
    } catch (err) {
        console.log(err)
        process.exit(1)
    }
    await sleep(Math.floor(Math.random() * 1000) + 2000)
    
    var skip = 0
    console.log(dbID, remoteID)
    console.log(dbCount, remoteCount)
    while (dbID < remoteID) {
        try {
            var remaining = remoteCount-dbCount
            console.log(`Fetching ${table} data. Only ${remaining} records to go!`)
            result = await query(`https://oda.ft.dk/api/${table}?$filter=id%20gt%20${dbID}`)
            result = JSON.parse(result)
            var objs = result.value
            for(var i = 0, n = objs.length; i < n; i++){
                var obj = objs[i]
                var keys = [], values = [];
 
                for (var k in obj) {
                    var val = obj[k]
                    val = typeof val === "string" ? val.replace(new RegExp("'", 'g'), "''") : val
                    keys.push(k)
                    values.push(val)
                }

                const stmt = db.prepare(`INSERT OR REPLACE INTO ${table}(${keys.join(",")}) VALUES ('${values.join("','")}');`)
                const info = stmt.run()
                dbID = obj.id
            }
            dbCount+=100;
        } catch (err) {
            console.log(err)
            process.exit(1)
        }
        await sleep(Math.floor(Math.random() * 10000) + 10000)
    }
    console.log(`${table} data up to date.`)
    await sleep(Math.floor(Math.random() * 3000) + 2000)
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function query(url) {
    return new Promise((resolve, reject) => {

        var request = https.get(url, res => {
            res.setEncoding("utf8");
            let body = "";
            res.on("data", chunk => {
                body += chunk;
            });

            res.on("end", () => {
                try {
                    resolve(body)
                } catch (e) {
                    reject(e)
                }
            });

            res.on("error", (err) => {
                reject(err)
            })
        });

        request.setTimeout(10000, function () {
            reject("Timeout")
        });

        request.on("error", (err) => {
            reject(err)
        })
    })
}

function queryAll(url, result, count) {
    result = result || [];
    count = count || 0;
    return new Promise((resolve, reject) => {
        query(url).then((data) => {
            var data = JSON.parse(data)
            result = result.concat(data.value)
            if (data.hasOwnProperty("odata.nextLink") && count < maxCount) {
                setTimeout(() => {
                    console.log("Fetching next " + data["odata.nextLink"])
                    resolve(queryAll(data["odata.nextLink"], result, count + 1))
                }, Math.floor(Math.random() * 5000) + 2000)
            } else {
                resolve(result)
            }
        }).catch((err) => {
            setTimeout(() => {
                console.log("Error: trying again " + url)
                return queryAll(url, result, count)
            }, Math.floor(Math.random() * 5000) + 8000)
        })
    })
}

function parseXML(xml) {
    return new Promise((resolve, reject) => {
        parser(xml, (err, data) => {
            if (err) {
                reject(err)
            } else {
                resolve(data)
            }
        });
    })
}


module.exports = {
    CreateDatabase: function () {
        return createDatabase()
    },
    UpdateDatabase: function () {
        return updateDatabase()
    }
}