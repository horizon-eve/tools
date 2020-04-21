const { Client } = require('pg')
const argv = require('yargs').argv
const fs = require('fs')

/**
 * USAGE EXAMPLE
 * node db2api.js --user=postgres --host=localhost --db=horizon --passwd=postgres --port=5432 --schema=horizon --roles=authenticated,pubapi --output=horizon.json
 *
 Json Structure:
 api
	name
	path
	description
	table
	fields
		name
		description
		type
		column
	operations [crud]
 */
const client = new Client({
    user: argv.user,
    host: argv.host,
    database: argv.db,
    password: argv.passwd,
    port: argv.port,
})
const authenticated = argv.roles.split(',')[0]
const public = argv.roles.split(',')[1]



client.connect()
client.query('with table_with_pk as (\n' +
    '\tselect cu.table_name, cu.constraint_name, count(cu.constraint_name) as cnt\n' +
    '\tfrom information_schema.constraint_column_usage cu\n' +
    '\tjoin information_schema.table_constraints tc\n' +
    '\t  on tc.constraint_name = cu.constraint_name\n' +
    '\t and tc.table_name = cu.table_name\n' +
    '\t and tc.table_schema = cu.table_schema\n' +
    '\twhere tc.constraint_type = \'PRIMARY KEY\'\n' +
    '\t  and cu.table_schema = $1 \n' +
    '\tgroup by cu.table_name, cu.constraint_name\n' +
    '\thaving count(cu.constraint_name) = 1\n' +
    '\t)\n' +
    'select c.table_name, c.column_name, c.is_nullable, c.data_type, c.character_octet_length, (CASE WHEN cu.constraint_name is not null THEN \'1\' ELSE \'0\' END) as is_pk, (CASE WHEN c.column_default is not null THEN \'1\' ELSE \'0\' END) as has_default\n' +
    'from information_schema.columns c\n' +
    'left join table_with_pk tpk\n' +
    '  on tpk.table_name = c.table_name\n' +
    'left join information_schema.constraint_column_usage cu\n' +
    '  on cu.table_schema = c.table_schema\n' +
    ' and cu.table_name = c.table_name\n' +
    ' and cu.column_name = c.column_name\n' +
    ' and cu.constraint_name = tpk.constraint_name\n' +
    'where c.table_schema = $1 \n' +
    'order by c.table_name, is_pk desc, c.column_name'
    , [argv.schema] , (err, res) => {
      console.error(err ? err.stack : 'fetched ' + res.rows.length + ' columns')
      if (!err && res.rows.length > 0) {
          client.query('select grantee, table_name, privilege_type from information_schema.role_table_grants\n' +
            'where table_schema = $1', [argv.schema], (err, auth) => {
              if (err) {
                  console.error(`error fetching role_table_grants: ${err}`)
              } else {
                  var paths = {}
                  var ddls = res.rows.reduce(function (rv, x) {
                      (rv[x['table_name']] = rv[x['table_name']] || []).push(x);
                      return rv;
                  }, {});
                  Object.keys(ddls).forEach(function (tn) {
                      var fields = {}
                      ddls[tn].forEach(function(c) {
                          var name = c.is_pk === '1' ? 'id' : toFieldName(c.column_name)
                          fields[name] = {
                              name: name,
                              type: toFieldType(c.data_type),
                              is_required: c.is_nullable === 'NO' && c.has_default !== '1',
                              column_name: toDbName(c.column_name)
                          }
                      })
                      var path = toPath(tn)
                      let protected = isProtected(tn, auth.rows, authenticated)
                      let role = protected ? authenticated : public
                      paths[path] = {
                          name: toName(tn),
                          path: path,
                          protected: protected,
                          db_table: toDbName(tn),
                          db_schema: argv.schema,
                          operations: getOperations(tn, auth.rows, role),
                          fields: fields,
                      }
                  });
                  var schema = {
                      name: 'Horizon API',
                      version: '0.0.1',
                      created: new Date(),
                      description: 'Auto generated horizon api from ' + argv.schema + ' db schema using db2api',
                      paths: paths
                  }
                  if (argv.output) {
                      fs.writeFile(argv.output, JSON.stringify(schema), (err) => {
                          // throws an error, you could also catch it here
                          if (err) throw err;
                          // success case, the file was saved
                          console.log(`Saved to file: ${argv.output}`);
                      });
                  }
                  else
                      console.log(JSON.stringify(schema))
              }
              client.end()
          });
      }
      else {
          client.end()
      }
    });

function toDbName(n) {
    return n.match(/[A-Z]/) ? '"' + n + '"' : n
}

function toName(n) {
    return n.charAt(0).toUpperCase() + n.slice(1)
}

function toPath(n) {
    return n.toLowerCase()
}

function isProtected(tn, auth, role) {
    if (auth) {
        return auth.find(a => a.table_name === tn && a.grantee === role) !== undefined;
    }
    return false;
}

function getOperations(tn, auth, role) {
    if (auth) {
        return auth
          .filter(a => a.table_name === tn && a.grantee === role)
          .map(a => {
              switch (a.privilege_type) {
                  case("INSERT"): return 'C'
                  case("SELECT"): return 'R'
                  case("UPDATE"): return 'U'
                  case("DELETE"): return 'D'
              }
          })
          .join('')
    }
}


function toFieldType(t) {
    switch(t.toLowerCase()) {
        case 'integer' : return 'number'
        case 'double precision': return 'number'
        case 'numeric': return 'number'
        case 'bigint': return 'number'
        case 'real': return 'number'
        case 'character varying': return 'string'
        case 'character': return 'string'
        case 'text': return 'string'
        case 'boolean': return 'boolean'
        case 'timestamp without time zone': return 'string'
        case 'date': return 'string'
        case 'jsonb': return 'string'
        default: throw "Please add mapping to field type for '" + t + "'"
    }
}

function toFieldName(n) {
    return n.replace(/\.?([A-Z]+)/g, function (x,y){return "_" + y.toLowerCase()}).replace(/^_/, "")
}
