'use strict'

const http   = require('http'),
      URL    = require('url'),
      mydb   = require('./lib/mydb'),
      Query  = require('./lib/query'),
      extend = require('util')._extend

exports.createServer = function(options) {
  options = extend({}, options)

  let db = mydb(options.db)

  return http.createServer((req, res) => {
    let url = URL.parse(req.url, true),
        query = Query(url)

    function finish(error, result) {
      if (error) {
        res.writeHead(500)
        res.end(error.toString())
      }
      else {
        res.end(JSON.stringify(result))
      }
    }

    function receive(req, next) {
      let chunks = []
      req.on('data', (chunk) => {
        chunks.push(chunk)
      })
      req.on('end', () => {
        let data = Buffer.concat(chunks)
        if (next) next(null, data)
      })
      req.on('error', (err) => {
        if (next) next(err)
      })
    }

    if (req.method == 'GET') {
      db.get(query, finish)
    }
    else if (req.method == 'POST') {
      receive(req, (err, data) => {
        if (err) return finish(err)
        if (query.attached) {
          let field = Object.keys(query.attached)[0],
              index = data.indexOf(0)
          try {
            let row = index > 0 ? JSON.parse(data.slice(0, index).toString()) : {}
            row[field] = data.slice(index + 1)
            query.rows = [row]
          }
          catch(err) {
            return finish(err)
          }
        }
        else {
          try {
            data = JSON.parse(data.toString())
          }
          catch(err) {
            return finish(err)
          }
          query.rows = Array.isArray(data) ? data : [data]
          if (query.rows.length == 0) {
            return finish(null, [])
          }
        }
        db.update(query, finish)
      })
    }
    else {
      finish(`Unsupported method ${req.method}`)
    }
  })
}
