var DB_CONFIG = require('../../config/default').connections;
var COLLECTION_NAME = require('../../config/default').queueCollectionName;
var MongoClient = require('mongodb').MongoClient;

var STATE = {
	READY: "READY",
	PROCESSING: "PROCESSING",
	DONE: "DONE",
	RETRY: "RETRY",
	FAIL: "FAIL"
};

var REQUEST_RESULT = {
	SUCCESS: "SUCCESS",
	FAIL: "FAIL"
};

var dbBase = {
	_getCollection: function() {
		return this.db.collection(COLLECTION_NAME);
	},
	_getDb: function(callback) {
		MongoClient.connect(DB_CONFIG.url, callback);
	},
}

exports.REQUEST_RESULT = REQUEST_RESULT;
exports.STATE = STATE;
exports.dbBase = dbBase;