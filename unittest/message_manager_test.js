var _ = require('underscore');
var testCase = require('nodeunit').testCase;
var util = require("util");
var MessageManager = require("../libs/message/message_manager").MessageManager;
var STATE = require("../libs/message/base").STATE;
var REQUEST_RESULT = require("../libs/message/base").REQUEST_RESULT;

function listenMock() {

}

function _getDbMock(callback) {
	callback(null, {});
}

loggerMock = {
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

module.exports = testCase({
	"SubscriptionTest": function(assert) {
		var m = new MessageManager();
		m.listen = listenMock;
		m._getDb = _getDbMock;

		var data = {
			"requestId": "testId",
			"senderId": "testSender",
			"event": "testEvent"
		};
		m.subscribe({}, data, function(returnData) {
			assert.equal("testId", returnData.requestId, "testId not OK.");
			assert.equal("SUCCESS", returnData.status, "Subscribe failed.");
			assert.equal("undefined", typeof(returnData.error), "error is not null.");
			assert.equal(returnData.requestId, data.requestId, "Request ID changed.");

			var subscribers = m.eventSubscribers["testEvent"];
			assert.ok(subscribers);
			assert.equal(1, subscribers.length);
			assert.equal("testSender", subscribers[0].subscriberId);

			assert.done();
		})
	},
	"UnsubscriptionTest": function(assert) {
		var m = new MessageManager();
		m.listen = listenMock;
		m._getDb = _getDbMock;
		m.logger = loggerMock;

		var data = {
			"requestId": "testId",
			"senderId": "testSender",
			"event": "testEvent"
		};
		m.subscribe({}, data, function(returnData) {
			var subscribers = m.eventSubscribers[data.event];
			assert.equal(subscribers.length, 1, "Subscribe failed.");
			m.unsubscribe(data.event, data.senderId);
			assert.equal(subscribers.length, 0, "Unsubscribe failed.");
			assert.done();
		});
	},
	"EnqueueTest": function(assert) {
		var data = {};
		var m = new MessageManager();
		m.enqueue(data, function(result) {
			assert.equal(result.status, REQUEST_RESULT.FAIL, "Unable to filter out bad requests.");
		});
		data.requestId = "testRequestId";
		m.enqueue(data, function(result) {
			assert.equal(result.status, REQUEST_RESULT.FAIL, "Unable to filter out bad requests.");
		});
		data.event = "testEvent";
		m.enqueue(data, function(result) {
			assert.equal(result.status, REQUEST_RESULT.FAIL, "Unable to filter out bad requests.");
		});
		_.extend(data, {
			"senderId": "testSender",
			"retryLimit": 1,
			"timeout": 60,
			"args": {},
			"subscribers": [{
				subscriberId: "id1",
				remainingRetryTimes: 4,
				state: STATE.READY,
				lastOperateTime: new Date()
			}]
		});
		assert.done();
	},
	"DispatchTest": function(assert) {
		var m = new MessageManager();
		m._getCollection = function() {
			return {
				insert: function(data, callback) {

				},
				update: function(cond, to, callback) {

				},
				findAndModify: function(cond, sort, to, flag, callback) {
					callback(null, {
						event: "testEvent",
						subscribers: ["testSubscriber"]
					});
				}
			};
		};
		m.dispatch();
	}
});