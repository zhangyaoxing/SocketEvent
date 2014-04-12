var _ = require('underscore');
var testCase = require('nodeunit').testCase;
var util = require("util");
var Subscriber = require("../libs/message/subscriber").Subscriber;
var STATE = require("../libs/message/base").STATE;
var REQUEST_RESULT = require("../libs/message/base").REQUEST_RESULT;
var SUBSCRIBER_STATE = require("../libs/message/subscriber").SUBSCRIBER_STATE;

getLogger = function() {
	return {
		trace: function(msg, err) {
			console.info(msg);
		},
		debug: function(msg, err) {
			console.info(msg);
		},
		info: function(msg, err) {
			console.info(msg);
		},
		warn: function(msg, err) {
			console.info(msg);
		},
		error: function(msg, err) {
			console.info(msg);
		},
		fatal: function(msg, err) {
			console.info(msg);
		}
	};
};

module.exports = testCase({
	"NotifyTest.Timeout": function(assert) {
		var manager = {
			unsubscribe: function() {}
		};
		var config = {
			id: "testId",
			event: "testEvent",
			socket: {
				emit: function() {}
			}
		};
		s = new Subscriber(manager, config);
		s.notify({}, 10, function(data) {
			assert.equal(data.status, REQUEST_RESULT.FAIL, "Status should be fail.");
			assert.equal(data.subscriberId, config.id, "Incorrect subscriber ID.");
			assert.done();
		});
	},
	"NotifyTest.Normal": function(assert) {
		var config = {
			id: "testId",
			event: "testEvent",
			socket: {
				emit: function(event, args, callback) {
					callback({
						status: REQUEST_RESULT.SUCCESS
					});
				}
			}
		};
		s = new Subscriber({}, config);
		s.notify({}, 10, function(data) {
			assert.equal(data.subscriberId, config.id, "Incorrect subscriber ID.");
			assert.equal(data.status, REQUEST_RESULT.SUCCESS, "Status should be success.");
			assert.done();
		})
	},
	"NotifyTest.Error": function(assert) {
		var manager = {
			unsubscribe: function() {}
		};
		var config = {
			id: "testId",
			event: "testEvent",
			socket: {
				emit: function(event, args, callback) {
					throw new Error();
				}
			}
		};
		s = new Subscriber(manager, config);
		s.logger = getLogger();
		s.notify({}, 10, function(data) {
			assert.equal(data.subscriberId, config.id, "Incorrect subscriber ID.");
			assert.equal(data.status, REQUEST_RESULT.FAIL, "Status should be success.");
			assert.done();
		})
	},
	"DisposeTest": function(assert) {
		var manager = {};
		s = new Subscriber(manager, {
			id: "testId",
			event: "testEvent",
			socket: {
				disconnect: function() {
				}
			}
		});
		s.logger = getLogger();
		s.dispose();
		assert.equal(SUBSCRIBER_STATE.DEAD, s.state, "Subscriber state should be DEAD now");
		assert.done();
	}
});