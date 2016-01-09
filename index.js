'use strict';

var co = require('co');
var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
var thenify = require('thenify');
var yaml = require('yaml-js');

var readFile = thenify(fs.readFile);

var url = process.argv[2];
var file = process.argv[3];

co(function*() {
  console.log('Parsing ' + file);
  var data = yield readFile(file, 'utf8');
  var cols = yaml.load(data);

  console.log('Connecting to ' + url);
  var db = yield MongoClient.connect(url);
  try {
    for (var colName in cols) {
      console.log('Loading ' + colName);
      var col = db.collection(colName);
      var batch = col.initializeUnorderedBulkOp();

      var docs = cols[colName];
      for (var _id in docs) {
        batch.find({_id: _id}).upsert().updateOne(docs[_id]);
      }

      var result = yield batch.execute();
      console.log({
          found: result.nMatched,
          updated: result.nModified,
          inserted: result.nUpserted,
      });
    }
  }
  finally {
    yield db.close();
  }
}).catch(function (err) {
  console.error(err.stack || err);
});
