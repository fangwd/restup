'use strict'

const fs     = require('fs'),
      crypto = require('crypto'),
      assert = require('assert'),
      MySQL  = require('mysql'),
      extend = require('util')._extend,
      Query  = require('./query')

const MAX_QUERY_SIZE = 512 * 1024

function Table(data) {
  extend(this, data)

  this.primaryKey = null        // array of column names
  this.uniqueConstraint = null  // array of column names

  for (let i = 0; i < data.indexes.length; i++) {
    let index = data.indexes[i]
    if (index.primaryKey) {
      this.primaryKey = index.columns
    }
    else if (index.unique) {
      this.uniqueConstraint = index.columns
    }
  }

  if (!this.uniqueConstraint) {
    this.uniqueConstraint = this.primaryKey
  }

  let _columnMap = {}
  this.columns.forEach(column => {
    _columnMap[column.name] = column
  })

  this.column = function(name) {
    return _columnMap[name]
  }
}

function Database(data) {
  if (!(this instanceof Database)) {
    return new Database(data);
  }
  this._data = JSON.parse(data)

  this._tableMap = {}
  this._data.tables.forEach(table => {
    this._tableMap[table.name] = new Table(table)
  })
}

Database.prototype.table = function(name) {
  return this._tableMap[name]
}

function MyDB(options) {
  if (!(this instanceof MyDB)) {
    return new MyDB(options);
  }

  this.options = extend({
    host         : 'localhost',
    user         : 'root',
    password     : '',
    database     : '',
    port         : 3306,
    multipleStatements: true,
    insecureAuth : true,
    connectionLimit : 8,
    fieldHandler : {},
    maxRunning: 8
  }, options)

  this.pool = MySQL.createPool(this.options)
  this.db = Database(fs.readFileSync(this.options.schema, 'utf8'))
}

function escapeKeys(keys) {
  return keys.map(key => {
    return '(' + key.map(MySQL.escape).join(',') + ')'
  }).join(',')
}

MyDB.prototype._handleFields = function(table, rows, next) {
  let tableName = typeof table === 'string' ? table : table.name,
      fieldHandler = this.options.fieldHandler[tableName]

  if (!fieldHandler) {
    return next(null, rows)
  }

  let running = 0,
      total = 0,
      pending = [],
      hasError = null

  function done(err) {
    if (!hasError) {
      if (err) {
        hasError = true
        next(err)
      }
      else {
        running--
        if (pending.length > 0) {
          let entry = pending.shift()
          entry[0](entry[1], done)
          running++
        }
        else if (running == 0) {
          next(null, rows)
        }
      }
    }
  }

  for (let i = 0; i < rows.length; i++) {
    let row = rows[i]
    for (let fieldName in fieldHandler) {
      if (fieldName in row) {
        total++;
        if (running < this.options.maxRunning) {
          running++;
          fieldHandler[fieldName](row, done)
        }
        else {
          pending.push([fieldHandler[fieldName], row])
        }
      }
    }
  }

  if (total == 0) next(null, rows)

}

MyDB.prototype.get = function(args, next) {
  if (!(args instanceof Query)) {
    return next('Instance of Query expected')
  }

  if (args.update) return this.claim(args, next)

  let columns = '*'
  if (args.columns && args.columns !== '*') {
    columns = args.columns.map(MySQL.escapeId).join(',')
  }

  let query = `SELECT ${columns} FROM \`${args.table}\``

  if (args.rowId) {
    let table = this.db.table(args.table)
    if (!table) {
      return next(`Table ${args.table} does not exist`)
    }
    query += ` WHERE ${table.primaryKey[0]}=${MySQL.escape(args.rowId)}`
  }
  else {
    if (typeof args.where === 'string') {
      query += ` WHERE ${args.where}`
    }
    else {
      let keys = Object.keys(args.where)
      for (let i = 0; i < keys.length; i++) {
        if (i == 0) {
          query += ' WHERE'
        }
        else {
          query += ' AND'
        }
        let op = args.where[keys[i]][0], value = args.where[keys[i]][1]
        query += `${MySQL.escapeId(keys[i])}${op}${MySQL.escape(value)}`
      }
    }

    if (args.sort) {
      query += ` ORDER BY ${MySQL.escapeId(args.sort)}`
    }

    if (args.limit) {
      query += ` LIMIT ${args.limit}`
    }
  }

  let self = this

  this.pool.getConnection(function(err, conn) {
    if (err) return next(err)

    conn.query(query, (err, results) => {
      conn.release()
      if (err || results.length == 0) {
        next(err, results)
      }
      else {
        self._handleFields(args.table, results, next)
      }
    })
  })
}

MyDB.prototype.claim = function(args, next) {
  let tableName  = args.table,
      rowCount   = args.limit,
      table      = this.db.table(tableName)

  if (!table) {
    return next(`Table ${tableName} does not exist`)
  }

  let keys = Object.keys(args.where), where = ''
  for (let i = 0; i < keys.length; i++) {
    if (i > 0) where += ' AND '
    let op = args.where[keys[i]][0], value = args.where[keys[i]][1]
    where += `${MySQL.escapeId(keys[i])}${op}${MySQL.escape(value)}`
  }

  this.pool.getConnection(function(err, conn) {
    if (err) return next(err)

    conn.query(`LOCK TABLES \`${tableName}\` WRITE`, (err, results) => {
      function abort(err) {
        conn.release()
        conn.destroy()
        next(err)
      }
      if (err) return abort(err)

      let columns = '*'
      if (args.columns && args.columns !== '*') {
        columns = args.columns.map(MySQL.escapeId).join(',')
      }
      conn.query(`SELECT ${columns} FROM \`${tableName}\`
                  WHERE ${where}
                  LIMIT ${rowCount}`, (err, rows) => {
        if (err) return abort(err)

        let keyValues = []
        rows.forEach(row => {
          let keyValue = []
          table.primaryKey.forEach(keyName => {
            keyValue.push(row[keyName])
          })
          keyValues.push(keyValue)
        })

        if (keyValues.length > 0) {
          let pkCols = table.primaryKey.map(MySQL.escapeId),
              pkRange = escapeKeys(keyValues),
              values = ''
          for (let key in args.update) {
            if (values.length > 0) values += ','
            values += MySQL.escapeId(key) + '=' + MySQL.escape(args.update[key])
          }
          conn.query(`UPDATE \`${tableName}\` SET ${values}
                      WHERE (${pkCols}) IN (${pkRange})`, (err) => {
            if (err) return abort(err)
            conn.query("UNLOCK TABLES", (err) => {
              if (err)
                abort(err)
              else {
                conn.release()
                next(null, rows)
              }
            })
          })
        }
        else {
          conn.query('UNLOCK TABLES', (err) => {
            if (err) {
              abort(err)
            }
            else {
              conn.release()
              next(null, [])
            }
          })
        }
      })
    })
  })
}

MyDB.prototype.close = function(next) {
  this.pool.end(next)
}

function _execStmts(conn, stmts, next) {
  var subs = [], i = 0, results = []

  function split() {
    let len = 0;
    while (i < stmts.length) {
      if (len + stmts[i].length + subs.length >= MAX_QUERY_SIZE) {
        if (i == 0) {
          next('Huge statement!')
        }
        else {
          return run()
        }
      }
      else {
        subs.push(stmts[i])
        len += stmts[i].length
        i++;
      }
    }
    if (subs.length > 0) {
      return run()
    }
  }

  function run() {
    conn.query(subs.join(';'), (err, res) => {
      if (err) {
        next(err)
      }
      else {
        results = results.concat(res)
        if (i == stmts.length) {
          next(null, results)
        }
        else {
          subs = []
          split()
        }
      }
    })
  }

  split()
}

MyDB.prototype.update = function(args, next) {
  let table = this.db.table(args.table)

  if (!this.db.table(args.table)) {
    return next(`Table ${args.table} does not exist`)
  }

  if (args.rowId) {
      if (table.primaryKey.length != 1) {
        return next(`Primary key error: ${args.table}`)
      }
      if (args.rows.length != 1) {
        return next(`Multiple rows for the same primary key`)
      }
      args.rows[0][table.primaryKey[0]] = args.rowId
  }

  this._handleFields(table, args.rows, (err, rows) => {
    let pkAll = [],  // Field value(s) in table's primary key
        ucAll = [],  // Field value(s) in table's unique constraint
        pkMap = {},  // PK => rowId
        ucMap = {}   // UC => rowId

    if (err) return next(err)

    function _hashRow(row, cols) {
      let hash = crypto.createHash('sha256')
      for (let i = 0; i < cols.length; i++) {
        let col = cols[i]
        if (!(col in row)) {
          return null
        }
        // 1-23 != 12-3
        let value
        if (table.column(col).type.toLowerCase() === 'date') {
          value = new Date(row[col]).toISOString().replace(/T.+$/, '')
        }
        else {
          // FIXME: Handle types other than string and integer
          // FIXME: Handle case sensitive (binary) comparisons
          value = (row[col] + '').toLowerCase()
        }
        hash.update(value.length + ':', 'binary')
        hash.update(value, 'binary')
      }
      return hash.digest('hex')
    }

    // Returns [sha256(pk), sha256(uc)]
    function hashRow(row) {
      let hash = _hashRow(row, table.primaryKey)
      if (hash) {
        return [hash, undefined]
      }

      if (table.uniqueConstraint.length == 0) {
        return null
      }

      hash = _hashRow(row, table.uniqueConstraint)
      if (hash) {
        return [null, hash]
      }

      return null
    }

    function getRowId(hash) {
      if (hash[0]) {
        return hash[0] in pkMap ? pkMap[hash[0]] : -1
      }
      else {
        return hash[1] in ucMap ? ucMap[hash[1]] : -1
      }
    }

    // Rows are merged before they are committed. For each row this map keeps
    // the index of another row, if there is one, which the data of the current
    // row is merged into.
    let dataMap = []
    for (let i = 0; i < args.rows.length; i++) {
      let hash = hashRow(args.rows[i])

      if (!hash) {
        return next("Can't update incomplete row(s).")
      }

      let rowId = getRowId(hash)

      if (rowId == -1) {
        if (hash[0]) {
          pkMap[hash[0]] = i
        }
        else {
          ucMap[hash[1]] = i
        }
      }
      else {
        let src = args.rows[i], dst = args.rows[rowId]
        for (let k in src) {
          dst[k] = src[k]
        }
      }

      dataMap.push(rowId)
    }

    for (let i = 0; i < args.rows.length; i++) {
      if (dataMap[i] != -1) continue

      let row = args.rows[i],
          pk  = [],
          uc  = []

      for (let j = 0; j < table.primaryKey.length; j++) {
        if (table.primaryKey[j] in row) {
          pk.push(row[table.primaryKey[j]])
        }
      }

      if (pk.length == table.primaryKey.length) {
        pkAll.push(pk)
      }
      else {
        for (let j = 0; j < table.uniqueConstraint.length; j++) {
          if (table.uniqueConstraint[j] in row) {
            uc.push(row[table.uniqueConstraint[j]])
          }
        }

        if (uc.length == table.uniqueConstraint.length) {
          ucAll.push(uc)
        }
        else {
          let error = "Can't update incomplete row: " + JSON.stringify(row)
          return next(error)
        }
      }
    }

    function escapeKeys2(keys, names) {
      return keys.map(key => {
        return `(${names})=(${key.map(MySQL.escape)})`
      })
    }

    function selectRows(conn, next) {
      let cols = table.primaryKey.concat(table.uniqueConstraint)
                 .map(MySQL.escapeId).join(','),
          pkCols = table.primaryKey.map(MySQL.escapeId),
          ucCols = table.uniqueConstraint.map(MySQL.escapeId),
          range = escapeKeys2(pkAll, pkCols),
          nextIndex = 0, results = []

      range = range.concat(escapeKeys2(ucAll, ucCols))

      let stmtBase = `SELECT ${cols} FROM \`${table.name}\` WHERE `

      function query() {
        let subRange = [], length = stmtBase.length
        for (; nextIndex < range.length; nextIndex++) {
          if (length + range[nextIndex].length + 4 < MAX_QUERY_SIZE) {
            subRange.push(range[nextIndex])
            length += range[nextIndex].length + 4
            continue
          }
          break
        }

        if (subRange.length == 0) {
          return next('Query size too big.')
        }

        conn.query(`${stmtBase} ${subRange.join(' OR ')}`, (err, rows) => {
          if (err) return next(err)
          results = results.concat(rows)
          if (nextIndex == range.length) {
            return next(null, results)
          }
          query()
        })
      }

      query()
    }

    this.pool.getConnection(function(err, conn) {
      if (err) return next(err)

      conn.query(`SET autocommit=0, foreign_key_checks=0`, (err, res) => {
        if (err) return next(err)

        conn.query(`LOCK TABLES \`${table.name}\` WRITE`, (err, res) => {
          function abort(err) {
            conn.release()
            conn.destroy()
            next(err)
          }
          if (err) return abort(err)

          selectRows(conn, (err, rows) => {
            if (err) return abort(err)

            // `name` = 'value'
            function escapePair(name, value) {
              return MySQL.escapeId(name) + '=' + MySQL.escape(value)
            }

            // `key1`="value1" AND `key2`="value2"
            function createConstraint(row, keys) {
              let result = ''
              for (let i = 0; i < keys.length; i++) {
                let key = keys[i]
                if (result.length > 0) {
                  result += ' AND '
                }
                result += escapePair(key, row[key])
              }
              return result
            }

            // UPDATE `t1` SET `name`='value'[,...] WHERE `key`="value"[ AND ...]
            function createUpdate(userRow, keys) {
              let stmt = 'UPDATE ' + MySQL.escapeId(table.name) + ' SET ',
                  fieldCount = 0

              for (let key in userRow) {
                if (keys.indexOf(key) == -1) {
                  if (fieldCount++ > 0) {
                    stmt += ', '
                  }
                  stmt += escapePair(key, userRow[key])
                }
              }

              if (fieldCount == 0) {
                // Placeholder
                return 'SELECT 0'
              }

              return stmt + ' WHERE ' + createConstraint(userRow, keys)
            }

            function createInsert(userRow) {
              let keys = [], values = []

              for (let key in userRow) {
                keys.push(key)
                values.push(userRow[key])
              }

              return 'INSERT INTO ' + MySQL.escapeId(table.name) + ' ('
                                    + keys.map(MySQL.escapeId).join(',')
                                    + ') VALUES ('
                                    + values.map(MySQL.escape).join(',')
                                    + ')'
            }

            let pkMap2 = {}, ucMap2 = {}
            for (let i = 0; i < rows.length; i++) {
              let row = rows[i]
              pkMap2[_hashRow(row, table.primaryKey)] = row
              if (table.uniqueConstraint.length > 0) {
                ucMap2[_hashRow(row, table.uniqueConstraint)] = row
              }
            }

            let stmts = []

            for (let i = 0; i < args.rows.length; i++) {
              let userRow = args.rows[i], stmt = null

              if (dataMap[i] != -1) continue

              let hash = hashRow(userRow),
                  diskRow = hash[0] ? pkMap2[hash[0]] : ucMap2[hash[1]]
              if (diskRow) {
                if (hash[0]) {
                  // UPDATE ... WHERE pk=...
                  stmt = createUpdate(userRow, table.primaryKey)
                }
                else {
                  // UPDATE ... WHERE uc=...
                  stmt = createUpdate(userRow, table.uniqueConstraint)
                  for (let k = 0; k < table.primaryKey.length; k++) {
                    userRow[table.primaryKey[k]] = diskRow[table.primaryKey[k]]
                  }
                }
              }
              else {
                // INSERT INTO ...
                stmt = createInsert(userRow)
              }

              stmts.push(stmt)
            }

            if (stmts.length > 0) {
              _execStmts(conn, stmts, (err, results) => {
                if (err) return conn.rollback(() => { abort(err) })

                let rowKeys = []

                conn.commit(err => {
                  if (err) return conn.rollback(() => { abort(err) })

                  if (table.primaryKey.length > 1) {
                    for (let i = 0; i < args.rows.length; i++) {
                      rowKeys.push(0)
                    }
                  }
                  else {
                    let j = 0
                    if (!Array.isArray(results)) {
                      // assert(data.rows.length == 1)
                      results = [results]
                    }
                    for (let i = 0; i < args.rows.length; i++) {
                      if (dataMap[i] == -1) {
                        let keyValue = args.rows[i][table.primaryKey[0]],
                            result = results[j++],
                            insertId = result.insertId ? result.insertId : 0,
                            rowId = keyValue ? keyValue : insertId
                        assert (rowId)
                        rowKeys.push(rowId)
                      }
                      else {
                        rowKeys.push(rowKeys[dataMap[i]])
                      }
                    }
                  }
                })

                conn.query("UNLOCK TABLES", (err) => {
                  if (err) {
                    return abort(err)
                  }
                  else {
                    conn.release()
                    next(null, rowKeys)
                  }
                })
              })
            }
            else {
              conn.query("UNLOCK TABLES", (err) => {
                if (err) return abort(err)
                conn.release()
                next(null, [])
              })
            }
          })
        }) // LOCK TABLES
      }) // SET autocommit=0
    }) // getConnection
  })
}

module.exports = MyDB
