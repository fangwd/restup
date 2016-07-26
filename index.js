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
    else {
      receive(req, (err, data) => {
        if (err) return finish(err)
        try {
          data = JSON.parse(data.toString())
        }
        catch(err) {
          return finish(err)
        }
        if (req.method == 'PATCH') {
          query.update = data
          db.claim(query, finish)
        }
        else {
          // req.method == 'POST'
          query.rows = Array.isArray(data) ? data : [data]
          db.update(query, finish)
        }
      })
    }
  })
}
