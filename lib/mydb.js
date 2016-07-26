'use strict'

const fs     = require('fs'),
      MySQL  = require('mysql'),
      extend = require('util')._extend,
      Query  = require('./query')

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

}

MyDB.prototype.get = function(args, next) {
  if (!(args instanceof Query)) {
    return next('Instance of Query expected')
  }

  let columns = '*'
  if (args.columns && args.columns !== '*') {
    columns = args.columns.map(MySQL.escapeId).join(',')
  }

  let query = `SELECT ${columns} FROM ${args.table}`

  if (args.rowId) {
    let table = this.db.table(args.table)
    if (!table) {
      return next(`Table ${tableName} does not exist`)
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
      if (err) {
        next(err)
      }
      else {
        self._handleFields(args.table, results, next)
      }
    })
  })
}

MyDB.prototype.claim = function(args, next) {
  let flagName = Object.keys(args.where)[0]

  if (!(flagName in args.update)) {
    return next(`${flagName} not updated after claim`)
  }

  let tableName  = args.table,
      flagValue  = args.where[flagName][1],
      flagAfter  = args.update[flagName],
      rowCount   = args.limit,
      table      = this.db.table(tableName)

  if (!table) {
    return next(`Table ${tableName} does not exist`)
  }

  this.pool.getConnection(function(err, conn) {
    if (err) return next(err)

    conn.query(`LOCK TABLES ${tableName} WRITE`, (err, results) => {
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

      conn.query(`SELECT ${columns} FROM ${tableName}
                  WHERE ${flagName}=${flagValue}
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
          conn.query(`UPDATE ${tableName} SET ${values}
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

MyDB.prototype.update = function(args, next) {
  let table = this.db.table(args.table)

  if (!this.db.table(args.table)) {
    return next(`Table ${args.table} does not exist`)
  }

  this._handleFields(table, args.rows, (err) => {
    let pkAll = [],  // Field value(s) in table's primary key
        ucAll = []   // Field value(s) in table's unique constraint
  
    // Compares the values of the primary key and unique constraint of the given
    // rows. Returns 0 if they differ, 1/2 if their PK/UC key values are equal,
    // and -1 if they share the same PK but have different UCs.
    function compareRows(r1, r2) {
      let m = 0
      for (let i = 0; i < table.primaryKey.length; i++) {
        let k = table.primaryKey[i]
        if (!(k in r1) || !(k in r2)) {
          // PK not present in any of the rows
          break
        }
        if (r1[k] == r2[k]) {
          m++
        }
      }
  
      if (m == table.primaryKey.length) {
        for (let i = 0; i < table.uniqueConstraint.length; i++) {
          let k = table.uniqueConstraint[i]
          if (k in r1 && k in r2 && r1[k] != r2[k]) {
            // Same PK, different UCs
            return -1
          }
        }
        return 1
      }
  
      let n = 0
      for (let i = 0; i < table.uniqueConstraint.length; i++) {
        let k = table.uniqueConstraint[i]
        if (!(k in r1) || !(k in r2)) {
          break
        }
        if (r1[k] == r2[k]) {
          n++
        }
      }
  
      if (n == table.uniqueConstraint.length) {
        for (let i = 0; i < table.primaryKey.length; i++) {
          let k = table.primaryKey[i]
          if (k in r1 && k in r2 && r1[k] != r2[k]) {
            // Same UC, different PKs
            return -2
          }
        }
        return 2
      }
  
      return 0
    }
  
    // Rows are merged before they are committed. For each row this map keeps
    // the index of another row, if there is one, which the data of the current
    // row is merged into.
    let dataMap = []
    for (let i = 0; i < args.rows.length; i++) {
      dataMap.push(-1)
    }
  
    for (let i = 0; i < args.rows.length; i++) {
      if (dataMap[i] != -1) continue
      let r1 = args.rows[i]
      for (let j = i + 1; j < args.rows.length; j++) {
        if (dataMap[j] != -1) continue
        let r2 = args.rows[j], n = compareRows(r1, r2)
        if (n < 0) {
          return next("Inconsistent PK and unique constraint values")
        }
        else if (n > 0) {
          for (let k in r2) {
            r1[k] = r2[k]
          }
          dataMap[j] = i
        }
      }
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
        // continue to force PK/UC consistency in user rows
      }
  
      for (let j = 0; j < table.uniqueConstraint.length; j++) {
        if (table.uniqueConstraint[j] in row) {
          uc.push(row[table.uniqueConstraint[j]])
        }
      }
  
      if (uc.length == table.uniqueConstraint.length) {
        ucAll.push(uc)
      }
      else if (pk.length != table.primaryKey.length) {
        let error = "Can't update incomplete row: " + JSON.stringify(row)
        return next(error)
      }
    }
  
    function escapeKeys(keys) {
      return keys.map(key => {
        return '(' + key.map(MySQL.escape).join(',') + ')'
      }).join(',')
    }
  
    let cols = table.primaryKey.concat(table.uniqueConstraint)
               .map(MySQL.escapeId).join(','),
        pkCols = table.primaryKey.map(MySQL.escapeId),
        ucCols = table.uniqueConstraint.map(MySQL.escapeId),
        pkRange = escapeKeys(pkAll),
        ucRange = escapeKeys(ucAll)
  
    this.pool.getConnection(function(err, conn) {
      if (err) return next(err)
  
      conn.query(`LOCK TABLES ${table.name} WRITE`, (err, res) => {
        function abort(err) {
          conn.release()
          conn.destroy()
          next(err)
        }
        if (err) return abort(err)
  
        let where = '';
        if (pkRange.length > 0) {
          where = `(${pkCols}) IN (${pkRange})`
        }
        if (ucRange.length > 0) {
          if (where.length > 0) where += ' OR '
          where += `(${ucCols}) IN (${ucRange})`
        }
  
        conn.query(`SELECT ${cols} FROM ${table.name}
                    WHERE ${where}`, (err, rows) => {
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
  
          let stmts = []
  
          for (let i = 0; i < args.rows.length; i++) {
            let userRow = args.rows[i], stmt = null
  
            if (dataMap[i] != -1) continue
  
            for (let j = 0; j < rows.length; j++) {
              let diskRow = rows[j], equality = compareRows(diskRow, userRow)
              if (equality != 0) {
                if (equality == 1 || equality == -1) {
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
                rows.splice(j, 1)
                break;
              }
            }
  
            if (stmt == null) {
              // INSERT INTO ...
              stmt = createInsert(userRow)
            }
            stmts.push(stmt)
          }
  
          if (stmts.length > 0) {
            conn.beginTransaction(err => {
              if (err) return abort(err)
  
              conn.query(stmts.join(';'), (err, results) => {
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
                            insertId = result.insertId ? result.insertId : 0
                        rowKeys.push(keyValue ? keyValue : insertId)
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
    }) // getConnection
  })
}

module.exports = MyDB
