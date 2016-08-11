'use strict'

const URL = require('url')

function Query(url) {
  if (!(this instanceof Query)) {
    return new Query(url)
  }

  this.table = null
  this.columns = null
  this.rowId = null
  this.where = {}
  this.limit = 1
  this.sort = null
  this.update = null
  this.attached = null

  if (typeof(url) === 'string') {
    url = URL.parse(url, true)
  }

  let parts = url.pathname.split('/')

  if (parts[1].indexOf('.') == -1) {
    this.table = parts[1]
    this.columns = '*'
  }
  else {
    // GET /url.id,url?...
    let names = parts[1].split('.')
    this.table = names[0]
    this.columns = names[1].split(',')
  }

  this.rowId = parts.length > 2 ? parts[2] : null

  for (let key in url.query) {

    if (key.indexOf(':') == -1) {

      // key => name[-gt/ge/lt/le/...]
      function decodeOperator(op) {
        if (!op) return '='
        if (op == 'gt') {
          return '>'
        }
        else if (op == 'ge') {
          return '>='
        }
        if (op == 'lt') {
          return '<'
        }
        else if (op == 'le') {
          return '<='
        }
        else if (op == 'like') {
          return 'LIKE'
        }
        else {
          throw `Unknown operator '${op}'`
        }
      }

      let name = key.split('-')
      let value = url.query[key]
      this.where[name[0]] = [decodeOperator(name[1]), value]

    }

    else {
      let pair = key.split(':')

      if (pair.length > 1) {
        if (pair[0] == 'limit') {
          this.limit = parseInt(pair[1])
        }
        else if (pair[0] == 'sort') {
          this.sort = pair[1]
        }
        else if (pair[0] == 'where') {
          this.where = pair[1]
        }
        else if (pair[0] == 'update') {
          // update:status=2
          this.update = this.update || {}
          this.update[pair[1]] = url.query[key]
        }
        else if (pair[0] == 'attached') {
          // attached:response[=980]
          this.attached = this.attached || {}
          this.attached[pair[1]] = parseInt(url.query[key])
        }
        else {
          throw 'Unknown keyword ' + pair[0]
        }
      }
      else {
        throw 'Unknown query ' + key
      }

    }
  }
}

module.exports = Query
