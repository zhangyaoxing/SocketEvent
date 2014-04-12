var DB_CONFIG = require('../../config/default').connections;
var COLLECTION_NAME = require('../../config/default').queueCollectionName;
var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var getLogger = require('../log/logger').getLogger;
var getError = require('../exception/exceptions').getError;
var async = require('async');
var util = require("util");
var dbBase = require("base").dbBase;
var STATE = require("./base").STATE;
var REQUEST_RESULT = require("./base").REQUEST_RESULT;
var Subscriber = require("./subscriber").Subscriber;
var SUBSCRIBER_STATE = require(".subscriber").SUBSCRIBER_STATE;

function MessageManager() {
	// sample data
	// eventSubscribers = {
	// 	"event1": [Subscriber1, Subscriber2 ...],
	// 	"event2": []
	// 		...
	// };
	this.logger = getLogger("MainLoop");
	this.eventSubscribers = {};
}

MessageManager.prototype = {
	/**
	 * Validate request data.
	 * @param  {Object} data [description]
	 * @return {Object}      
	 * {
	 * 	error: "if any", 
	 * 	status: REQUEST_RESULT
	 * }
	 */
	_validate: function(data) {
		var result = {
			requestId: data.requestId,
			status: REQUEST_RESULT.SUCCESS
		};
		if (!data.requestId) {
			result.error = getError("ArgumentError", "requestId");
			result.status = REQUEST_RESULT.FAIL;
		}

		if (!data.event) {
			result.error = getError("ArgumentError", "event");
			result.status = REQUEST_RESULT.FAIL;
		}

		if (!data.senderId) {
			result.error = getError("ArgumentError", "senderId");
			result.status = REQUEST_RESULT.FAIL;
		}

		return result;
	},
	/**
	 * Start Socket IO listening.
	 * @param  {int} port socket port
	 * @param  {string} host host name or IP address.
	 * @return {void}      void
	 */
	listen: function(port, host) {
		this._getDb(function(err, db) {
			if (err) {
				this.logger.fatal("Database not available.", err);
				return;
			}

			this.db = db;
			var server = require('http').createServer();
			server.listen(port, host ? host : "0.0.0.0");
			this.io = require('socket.io').listen(server);
			this.io.sockets.on('connection', function(socket) {
				// client subscribes an event
				socket.on("subscribe", function(data, callback) {
					this.subscribe(socket, data, callback);
				}.bind(this));

				// client enqueues an event
				socket.on("enqueue", function(data, callback) {
					this.enqueue(data, callback);
				}.bind(this));
			}.bind(this));

			// try to dispatch event every minutes
			setInterval(this.schedule.bind(this), 60000);
		}.bind(this));
	},
	/**
	 * request data sample
	 * data = {
	 * "requestId": "",	// mandatory. unique ID of each request.
	 * "senderId": "",	// mandatory. unique name of sender.
	 * "event": "" // mandatory. options: enqueue/ack/command.
	 * }
	 * @param  {socket}   socket   SocketIO socket.
	 * @param  {Object}   data     Data from socket client
	 * @param  {Function} callback 
	 * @return {void}            void
	 */
	subscribe: function(socket, data, callback) {
		var result = this._validate(data);
		if (result.status == REQUEST_RESULT.FAIL) {
			this.acknowledge(callback, result);
			return;
		}

		if (!this.eventSubscribers[data.event]) {
			this.eventSubscribers[data.event] = [];
		}

		var subscribers = this.eventSubscribers[data.event];
		// check if this client has subscribed before
		var existed = _.find(subscribers, function(s) {
			return s.id == data.senderId;
		});

		if (existed) {
			// client already subscribed. close previous connection, use current one instead.
			this.unsubscribe(data.event, data.senderId);
			this.logger.warn("Client already connected.", getError("AlreadyConnected", existed.id));
		} else {
			var newSubscriber = new Subscriber(this, {
				id: data.senderId,
				socket: socket,
				event: data.event
			});
			subscribers.push(newSubscriber);
		}
		this.acknowledge(callback, {
			requestId: data.requestId,
			status: REQUEST_RESULT.SUCCESS
		});
	},
	/**
	 * Unsubscribe by event and subscriber ID.
	 * @param  {string} event event name
	 * @param  {string} sId   subscriber ID
	 * @return {void}       void
	 */
	unsubscribe: function(event, sId) {
		var subscribers = this.eventSubscribers[event];
		var subscriber = null;
		for (var i = 0; i < subscribers.length; i++) {
			var subscriber = subscribers[i];
			if (subscriber.id == sId) {
				subscribers.splice(i, 1);
				this.logger.info(util.format("client [%s] unsubscribed from event [%s]", sId, event));
				subscriber.dispose();
				break;
			}
		}
	},
	// request sample
	// data = {
	// 	"requestId": "",	// mandatory. unique ID of each request.
	// 	"senderId": "",	// mandatory. unique name of sender.
	// 	"event": "enqueue", // mandatory.  event to trigger.
	//  "retryLimit": 1, 	// optional. defaults to 0. -1 = always.
	// 	"timeout": 60,	// optional. timeout in seconds. defaults to 60
	// 	"args": {},	// optional. only available when action=command
	// }
	enqueue: function(data, callback) {
		// data storage sample
		// {
		// 	"_id": "",	// mandatory. 
		// 	"requestId": "",	// mandatory. unique ID of each request.
		// 	"senderId": "",	// mandatory. unique name of sender.
		// 	"event": "",	// mandatory. event name.
		// 	"retryLimit": 1,	// mandatory. how many times should we retry if fails. -1 = always.
		// 	"timeout": 60,	// mandatory. timeout in seconds.
		// 	"args": {},	// optional.
		// 	"subscribers": [{
		// 		subscriberId: "id1",
		// 		remainingRetryTimes: 4,
		// 		state: STATE.READY,
		// 		lastOperateTime: new Date()
		// 	}] // target names
		// }
		var result = this._validate(data);
		if (result.status == REQUEST_RESULT.FAIL) {
			this.acknowledge(callback, result);
			return;
		}

		// find out all the subscribers and prepare basic data for them.
		var subscribers = _.map(this.eventSubscribers[data.event], function(elm) {
			return {
				subscriberId: elm.id,
				remainingRetryTimes: data.retryLimit,
				state: STATE.READY,
				lastOperateTime: null
			}
		});
		this._getCollection().insert({
			"requestId": data.requestId,
			"senderId": data.senderId,
			"retryLimit": data.retryLimit,
			"timeout": (data.timeout ? data.timeout : 60) * 1000,
			"event": data.event,
			"args": data.args,
			"createAt": new Date(),
			"state": STATE.READY,
			"subscribers": subscribers
		}, function(err, doc) {
			if (err) {
				this.logger.fatal("Database is not available to accept new requests.", err);
				this.acknowledge(callback, {
					requestId: data.requestId,
					status: REQUEST_RESULT.FAIL,
					error: err
				});
				return;
			}

			this.acknowledge(callback, {
				requestId: data.requestId,
				status: REQUEST_RESULT.SUCCESS
			});
			this.schedule();
		}.bind(this));
	},
	/**
	 * Acknowlege to client
	 * @param  {Function} callback this function will be invoked with parameter describing request status.
	 * @param  {Object}   data     Data describing request status.
	 * @return {void}            void
	 */
	acknowledge: function(callback, data) {
		var result = {
			requestId: data.requestId,
			status: data.status
		};
		if (data.error) {
			result.error = {
				name: data.error.name,
				message: data.error.message,
				stack: data.error.stack
			}
		}

		callback(result);
	},
	schedule: function() {
		Event.createInstance(this.eventSubscribers, function(eventObj) {
			if (eventObj) {
				setTimeout(this.schedule.bind(this), 10);
				eventObj.dispatch();
			}
		})
	}
};

_.extend(MessageManager.prototype, dbBase);

exports.MessageManager = MessageManager;