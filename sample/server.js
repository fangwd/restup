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
    user : 'root',
    password : '',
    database : 'mydb',
    schema : schemaFile,
    fieldHandler : {
      url : {
        response : (row, callback) => {
          let fileName = `${dataDir}/${row.id}`,
          data = row.response
          if (typeof data === 'object') {
            fs.writeFile(fileName, JSON.stringify(data), 'binary', function(err) {
              row['response'] = fileName
              callback(err)
            })
          }
          else {
            fs.readFile(fileName, 'binary', (err, data) => {
              row['response'] = JSON.parse(data)
              callback(err)
            })
          }
        }
      }
    },
    maxRunning: 2
  }
}).listen(1337)
