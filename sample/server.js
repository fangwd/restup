'use strict'

const restup = require('../'),
      fs = require('fs')

let argv = process.argv,
    schemaFile = argv.length > 2 ? argv[2] : 'mydb.json',
    dataDir = argv.length > 3 ? argv[3] : 'data'

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir)
}

restup.createServer({
  db: {
    user : 'me',
    password : 'secret',
    database : 'mydb',
    schema : schemaFile,
    fieldHandler : {
      url : {
        response : (row, callback) => {
          let fileName = `${dataDir}/${row.id}`,
          data = row.response
          if (data instanceof Buffer) {
            fs.writeFile(fileName, data, 'binary', function(err) {
              row['response'] = fileName
              callback(err)
            })
          }
          else {
            fs.readFile(fileName, 'binary', (err, data) => {
              if (!err) {
                row['response'] = data
              }
              callback(err)
            })
          }
        }
      }
    },
    maxRunning: 2
  }
}).listen(1337)
