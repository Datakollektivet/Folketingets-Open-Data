console.log("running")

var db = require("./lib/sqllite")
console.log(db)
var dbpath = "./database/ft.oda.sqlite.db"
db.CreateDatabase().then(db.UpdateDatabase).catch((err)=>{
  console.log("rejected")
  console.log(err)
})




/*
const electron = require('electron')

const { app, BrowserWindow } = require('electron')

function createWindow () {
  let win = new BrowserWindow({ width: 800, height: 600 })
  win.loadFile('./views/index.html')
}

//database info
//https://www.ft.dk/~/media/sites/ft/pdf/dokumenter/aabne-data/oda-browser_brugervejledning.ashx?la=da


app.on('ready', createWindow)
*/