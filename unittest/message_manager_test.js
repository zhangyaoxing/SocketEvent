var MessageManager = require("../libs/message/message_manager").MessageManager;
var testCase = require('nodeunit').testCase;

module.exports = testCase({
	"SubscriptionTest": function(test) {
		var m = new MessageManager();
		m.listen = function() {

		};

		m._getDb = function(callback) {
			callback(null, null);
		};

		var data = {
			"requestId": "testId",
			"senderId": "testSender",
			"event": "testEvent"
		};
		m.subscribe({}, data, function(returnData) {
			test.equal("testId", returnData.requestId, "testId not OK.");
			test.equal("SUCCESS", returnData.status, "Subscribe failed.");
			test.equal("undefined", typeof(returnData.error), "error is not null.");
			test.equal(returnData.requestId, data.requestId, "Request ID changed.");

			var subscribers = m.eventSubscribers["testEvent"];
			test.ok(subscribers);
			test.equal(1, subscribers.length);
			test.equal("testSender", subscribers[0].subscriberId);

			test.done();
		})
	},
	"UnsubscriptionTest" : function(test) {
		var m = new MessageManager();
		m.listen = function() {

		};

		m._getDb = function(callback) {
			callback(null, null);
		};

		m.logger.info = function(msg) {
			console.log(msg);
		}

		var data = {
			"requestId": "testId",
			"senderId": "testSender",
			"event": "testEvent"
		};
		m.subscribe({}, data, function(returnData) {
			var subscribers = m.eventSubscribers[data.event];
			test.equal(subscribers.length, 1, "Subscribe failed.");
			m.unsubscribe(data.event, data.senderId);
			test.equal(subscribers.length, 0, "Unsubscribe failed.");
			test.done();
		});
	}
});