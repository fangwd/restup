# restup

Restup is a Node.js database proxy which provides a simple HTTP based API for distributed applications to query and update a shared database. It was originally designed for distributed web crawling but the API itself does not assume any domain specific knowledge about the database schema.

Restup currently supports MySQL as the backend database engine. Support for other databases will be added shortly as needed.

## Basic operations

The following examples are assumed that we have a table "url" which has been created with SQL statement:

```sql
CREATE TABLE url2 (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  url varchar(4096) DEFAULT NULL,
  url_hash char(32) DEFAULT NULL,
  status tinyint(4) DEFAULT '0',
  PRIMARY KEY (id),
  UNIQUE KEY url_hash (url_hash)
)
```

### Selecting rows from a table

Restup supports simple `SQL SELECT` queries to the database via `HTTP GET`. For example:

SQL:
```sql
SELECT * FROM url WHERE id=1
```

HTTP:
```
GET /url/1
```

SQL:
```sql
SELECT * FROM url WHERE status=0 LIMIT 1
```

HTTP:
```
GET /url?status=0
```

SQL:
```sql
SELECT id, url FROM url WHERE id > 10 AND status < 2 ORDER BY url LIMIT 5
```

HTTP:
```
GET /url.id,url?id-gt=10&status-lt=2&limit:5&sort:url
```

### Claiming rows from a table

A common requirement in distributed applications is the ability to select rows exclusively, i.e. to select one or more rows from a table and update a flag field of the selected row(s) atomically so that they will not be selected again by other processes.

With restup, this is achieved by using `HTTP PATCH` requests. For example, to get 5 URLs with flags equal to 0 and update their flags to 1 for the selected:

```js
PATCH /url?status=0&limit:5
{status: 1}
```

### Creating/updating rows

Rows can be updated using `HTTP POST`. For example:

```js
POST /url
[{"id": 1, "status":2}, {"id": 2, "status":-1}]
```

## Advanced usage

Field handlers can be provided via the `fieldHandler` option. The following example creates a restup server which writes `url.response` to disk on updating and reads them back when requested (field handler calls are parallelised behind the scenes for better performance).

```js
restup.createServer({
  db: {
    user : 'root',
    password : '',
    database : 'mydb',
    schema : 'mydb.json',
    fieldHandler : {
      url : {
        response : (row, callback) => {
          let fileName = `${dataDir}/${row.id}.json`,
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
})
```

