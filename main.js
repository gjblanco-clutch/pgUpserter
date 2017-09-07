var knex = require('knex');
var moment = require('moment');

var PgUpserter = require('./upserter');

var updater = new PgUpserter(knex({
  client: 'pg',
  connection: require('./config')
}), 1000);

updater.setColumnNames('persona', ['name', 'age', 'birth'], ['name', 'age'])

var queries = [];

for(var i = 0; i < 5; i++) {
  queries.push(updater.upsert('persona', ['name'+i, i + 20, i&1? moment().add(i*100, 'days').format('YYYY-MM-DD') : null]));
}
Promise.all(queries).then(() => updater.flush()).then(() => updater.close());