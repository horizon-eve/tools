const { Client } = require('pg')
const argv = require('yargs').argv
const fs = require('fs')
const request = require('request')

function main() {
  if (argv.file) {
    const f = fs.readFileSync(argv.file);
    run(JSON.parse(f))
  } else if (argv.url) {
    request({
      url: argv.url,
      json: true
    }, function (error, response, body) {
      if (error) {
        console.log(error)
        process.exit(-1)
        run(body)
      }
    })
  }
} main()

function run(spec) {
  let mapping = {
    title: spec.info.title,
    version: spec.info.version,
    description: spec.info.description,
    schema: argv.schema,
    protected_role: argv.roles.split(',')[0],
    public_role: argv.roles.split(',')[1],
    esi_roles: new Set(),
    tables: []
  }
  // Create Tables from paths
  Object.keys(spec.paths).forEach(p => {
    const path = spec.paths[p].get
    if (!path)
      return
    let table = {
      name: path2table(p),
      description: path.summary,
      operation: path.operationId,
      required_roles: expandRequiredRoles(path['x-required-roles'], mapping.esi_roles),
      columns: {}
    }
    mapping.tables.push(table)
    // params columns
    path.parameters.forEach(param => {
      let schema, name
      if (param.$ref) {
        if (param.$ref === '#/parameters/token') {
          table.protected = true
          if (!table.required_roles) { // This table is for authenticated character, add auth_character_id
            name = 'auth_character_id'
            let copy_from = spec.parameters['character_id']
            schema = {
              type: copy_from.type,
              format: copy_from.format,
              required: true,
              description: "Authenticated Character Id"
            }
          }
        }
        else {
          name = param.$ref.replace('#/parameters/', '')
          schema = spec.parameters[name]
        }
      }
      else {
        schema = param
        name = param.name
      }
      if (schema && !table.columns[name] && schema.required) {
        addColumn(schema, table, name)
      }
    })
    // Primary Key
    let schema = path.responses['200'].schema
    if (schema.type !== 'array') {
      let pk = Object.values(table.columns).filter(c => c.path)
      if (pk.length === 1) {
        pk[0].primary = true
      }
    }
    fillColumnsFromSchema(schema, table)
  })

  if (argv.out) {
    fs.writeFile(argv.out, toSql(mapping), (err) => {
      if (err) throw err;
      console.log('The file has been saved!');
    });
  } else {
    console.log(JSON.stringify(toSql(mapping)))
  }
}

function fillColumnsFromSchema(schema, table, prefix) {
  if (!prefix) prefix = ''
  switch (schema.type) {
    case 'object': {
      Object.keys(schema.properties).forEach(prop => {
        const property = schema.properties[prop]
        if (property.type === 'object')
          fillColumnsFromSchema(property, table, `${prefix}${prop}`)
        else
          addColumn(property, table, `${prefix}${prop}`, schema.required && schema.required.find(p => prop === p) !== undefined)
      })
      break;
    }
    case 'array': return fillColumnsFromSchema(schema.items, table, prefix)
    case 'integer':
    case 'number': {
      table.type = 'primitive'
      return addColumn(schema, table, `${prefix}${table.name.replace(/.+_/g, '')}_id`)
    }
    default: throw `Unknown model type: ${schema.type} for ${schema.title}`
  }
}

function addColumn(schema, table, name, required) {
  if (!table.columns[name]) {
    let c = {
      name: name,
      cname: to31Char(name),
      type: esi2dbtype(schema.type, schema.format),
      required: schema.required || required,
      description: schema.description,
    }
    if (schema.in && schema.in === 'path') {
      c.path= true
    }
    table.columns[name] = c
  }
}

function esi2dbtype(t, f) {
  switch (t) {
    case 'string':
      switch (f) {
        case 'date': return 'date'
        case 'date-time': return 'timestamp'
        case undefined: return 'varchar'
        default: throw `unknown format: ${f}, type: ${t}`
      }
    case 'integer':
      switch (f) {
        case 'int32': return 'integer'
        case undefined: return 'integer'
        case 'int64': return 'bigint'
        default: throw `unknown format: ${f}, type: ${t}`
      }
    case 'boolean': return 'boolean'
    case 'number':
      switch (f) {
        case 'float': return 'float'
        case 'double': return 'double precision'
        default: throw `unknown format: ${f}, type: ${t}`
      }
    case 'array': return 'varchar'
  }
  let err = new Error(`unknown type: ${t}, format: ${f}`)
  Error.captureStackTrace(err, esi2dbtype)
  throw err
}

function path2table(path) {
  let res = ''
  let tks = path.replace(/^\s*\/+|\s*\/+$/g, '').split('/')
  tks.forEach((s, i) => {
    let tk = s.replace(/\{|\}|_id/g, '')
      .replace('division', 'div')
      .replace(/ies$/, 'y')
    if (!tk.endsWith('us'))
      tk = tk.replace(/s$/, '')
    if (tks.length === 1) {
      res += s
    } else if (i === 0) { // first word becomes a prefix
      if (tks.length == 2 && tks[1].startsWith('{') && tks[1].includes(tk)) {
        res += tk
      }
      else
        res += tk.replace('alliance', 'alli')
          .replace('calendar', 'cal')
          .replace('character', 'chr')
          .replace('corporation', 'crp')
          .replace('dogma', 'dgm')
          .replace('fleet', 'flt')
          .replace('incursions', 'inc')
          .replace('industry', 'ind')
          .replace('insurance', 'ins')
          .replace('killmail', 'km')
          .replace('loyalty', 'loy')
          .replace('market', 'mkt')
          .replace('opportunity', 'opp')
          .replace('search', 'srch')
          .replace('sovereignty', 'sov')
          .replace('universe', 'uv')
          .replace('contract', 'ctr')
          + '_'
    } else if (i === tks.length - 1) { // last word may indicate details
      res += tks[i-1].includes(tk) ? res.endsWith('_') ? 'dtl' : '' : tk
    } else if (!tks[i-1].includes(tk)) // skip repetitions
      res += tk + '_'
  })
  if (res.length > 30) throw `table name > 31: ${res}, tks: ${tks}`
  return res
}

function toSql(mapping) {
  let sql = []
  sql.push(`-- ${mapping.title} v${mapping.version}`)
  sql.push(`-- ${mapping.description}`)
  sql.push(`--`)
  sql.push(`CREATE USER ${mapping.schema} with password '${mapping.schema}';`)
  sql.push(`GRANT CONNECT ON DATABASE horizon TO ${mapping.schema};`)
  sql.push(`CREATE SCHEMA AUTHORIZATION ${mapping.schema};`)
  sql.push(`DROP ROLE IF EXISTS ${[...mapping.esi_roles].join(',')};`)
  mapping.esi_roles.forEach(r => {
    sql.push(`CREATE ROLE ${r};`)
  })
  sql.push('--')
  sql.push(`SET SEARCH_PATH TO ${mapping.schema};`)
  sql.push(`--`)
  mapping.tables.forEach(t => {
    sql.push(`-- ${t.description}`)
    sql.push(`-- operation id: ${t.operation}`)
    sql.push(`CREATE TABLE ${t.name}`)
    sql.push(`(`)
    let columns = Object.keys(t.columns)
    columns.forEach((cn, i) => {
        const c = t.columns[cn]
        sql.push(`  ${c.cname} ${c.type}${c.type === 'varchar' ? '(4000)' : ''}${c.primary ? ' PRIMARY KEY' : c.required? ' NOT NULL': ''}${i < columns.length -1 ? ',':''}`)
    })
    sql.push(`);`)
    sql.push(`ALTER TABLE ${t.name} OWNER TO ${mapping.schema};`)
    sql.push(`GRANT SELECT ON TABLE ${t.name} TO ${t.required_roles? t.required_roles.join(',') : t.protected ? mapping.protected_role : mapping.public_role};`)
    if (t.protected) {
      sql.push(`ALTER TABLE ${t.name} ENABLE ROW LEVEL SECURITY;`)
      if (t.required_roles) {
        sql.push(`CREATE POLICY ${t.name} ON ${t.name} TO ${t.required_roles.join(',')} USING (corporation_id = current_setting('corporation_id')::INTEGER);`)
      }
      else {
        sql.push(`CREATE POLICY ${t.name} ON ${t.name} TO ${mapping.protected_role} USING (auth_character_id = current_setting('character_id')::INTEGER);`)
      }
    }
    sql.push('')
  })
  // SPEC
  sql.push('-- Swagger Mapping\n' +
    'CREATE TABLE swagger_mapping\n' +
    '(\n' +
    '  version varchar(50) NOT NULL,\n' +
    '  description varchar(255),\n' +
    '  mapping text not null\n' +
    ');\n' +
    `ALTER TABLE swagger_mapping OWNER TO ${mapping.schema};`)
  sql.push(`INSERT INTO swagger_mapping(version, description, mapping) VALUES ('${mapping.version}', '${mapping.description}', '${toSwaggerMapping(mapping)}');`)
  return sql.join('\n')
}

function toSwaggerMapping(mapping) {
  let res = {
    title: mapping.title,
    version: mapping.version,
    description: mapping.description,
    operations: {}
  }
  mapping.tables.forEach(t => {
    let op = {
      table: t.name,
      type: t.type,
      fields: {}
    }
    res.operations[t.operation] = op
    Object.values(t.columns).forEach(c => {
      if (c.path) {
        if (!op.key) {
          op.key = []
        }
        op.key.push(c.name)
      }
      op.fields[c.name] = c.cname
    })
  })
  return JSON.stringify(res).replace(/'/g,'\\\'')
}

function expandRequiredRoles(roles, esi_roles) {
  if (roles) {
    // role system is on the op, so at least CEO should have it
    if (roles.length === 0 ) {
      roles.push('CEO')
    }
    roles.forEach(r => esi_roles.add(r))
  }
  return roles
}

function to31Char(n) {
  if (n === 'from')
    n = `"${n}"`
  if (n.length <= 31)
    return n
  let h = '' + hashCode(n)
  let cut = n.length - 31 + h.length
  return n.substr(0, (n.length - cut) / 2) + h + n.substring((n.length + cut) / 2)
}

function hashCode(s) {
  let h;
  for(let i = 0; i < s.length; i++)
    h = Math.imul(31, h) + s.charCodeAt(i) | 0
  return Math.abs(h)
}
