module.exports = pgUpserter;

/**
 * Upserter.
 * Case study problem: you have two tables, table and Table, with columns k,k,c,c,c and K,C,C respectively, where the key columns are the k's;
 * you want to insert a lot of stuff in them.
 * 
 * code:
 * // create updater:
 * var upserter = new pgUpserter(require('knex'), 1000);
 * 
 * //define your tables:
 * upserter.setColumnNames('table', ['k', 'k', 'c', 'c', 'c'], ['k', 'k']);
 * upserter.setColumnNames('Table', ['K', 'C', 'C'], ['K'])
 * 
 * 
 * //insert stuff, flush when you are done.
 * 
 * upserter.upsert('table', [1,2,3,4,5])
 *  .then(() => upserter.upsert('Table', [1,2,3]))
 *  .then(() => upserter.upsert('table', [4,5,6,7,8]))
 *  .then(() => upserter.flush())
 * 
 * // -"I am sorry it was a 10, not a 7 in the last one" 
 * // - I got you:
 * upserter.upsert('table', [4,5,null,10,null]).then(upserter.flush) // new row: [4,5,6,10,8]
 * 
 * @param {*} knex 
 * @param {*} batchSize 
 * @param {*} defaultTableName 
 * @param {*} defaultColumnNames 
 * @param {*} defaultKeyColumns 
 */


function pgUpserter(knex, batchSize) {
  
  var buffer = {};
  var totalSize = 0;
  batchSize = batchSize || 1000;

  function flush() {
    
    var queries = [];

    for(var tableName in buffer) {
      var upd = buffer[tableName];
      var query = insertStatement(tableName, upd.columnNames, upd.rows) + upsertStatement(tableName, upd.columnNames, upd.keyColumns, true);
      // console.log(query, '===bindings===', upd.rows);
      queries.push(knex.raw(query, [].concat.apply([], upd.rows)));
      // console.log(knex.raw(query, [].concat.apply([], upd.rows)).toString())
      buffer[tableName].rows = [];
    }

    return Promise.all(queries);

  }

  function setColumnNames(tableName, columnNames, keyColumns) {
    if(!keyColumns) {
      throw new Error('you need to specify a key column(s) for upserting. If you don\'t need to upsert, just use knex\'s batchInsert()')
    }
    // console.log('inside setColumnNames')

    buffer[tableName] = buffer[tableName] || {rows: []};
    buffer[tableName].columnNames = columnNames;
    buffer[tableName].keyColumns = keyColumns;
  }


  return {
    setColumnNames: setColumnNames,
    upsert: function(table, row) {
      if(!buffer[table]) {
        throw new Error('call setColumnNames(tableName, columnNames, keyColumns) before attempting to upsert stuff in a table')
      }

      buffer[table].rows.push(row);
      totalSize++;
      
      if(totalSize >= batchSize) {
        return flush();
      }

      return Promise.resolve('buffered');
    },
    flush: flush,
    close: () => knex.destroy()
  }
}

function upsertStatement(tableName, columnNames, keyColumns, ignoreNull) {
  var query = [' ON CONFLICT (', keyColumns.join(','), ') DO UPDATE SET '];
  // console.log('====>', columnNames)
  query.push(
    columnNames
    .filter(c => keyColumns.indexOf(c) === -1)
    .map(c => ignoreNull? `${c}=COALESCE(excluded.${c}, ${tableName}.${c})` : `${c}=excluded.${c}`)
    .join(',')
  );
  return query.join('');
}

function insertStatement(tableName, columnNames, rows) {
  var query = [`INSERT INTO ${tableName} (`, columnNames.map(c => '"'+c+'"').join(','), ') VALUES '];
  //var values = rows.map(r => `(${r.map(v => JSON.stringify(v).split("'").join("''").split('"').join("'"))})`);
  var values = rows.map(r => '(' + r.map(_ => '?').join(',') + ')');
  return query.join('') + values.join(',');
}