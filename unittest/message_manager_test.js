var MessageManager = require("../message_manager").MessageManager;
var testCase = require('nodeunit').testCase;

module.exports = testCase({
	"Subscription test": function(test) {
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
		m.subscribe({}, data, function(data) {
			debugger;
			test.equal("testId", data.requestId, "testId not OK");
			test.equal("SUCCESS", data.status, "Subscribe failed");
			test.equal("undefined", typeof(data.error), "error is not null.");

			var subscribers = m.eventSubscribers["testEvent"];
			test.ok(subscribers);
			test.equal(1, subscribers.length);
			test.equal("testSender", subscribers[0].subscriberId);

			test.done();
		})
	}
});