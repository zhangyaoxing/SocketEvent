var DB_CONFIG = require('../../config/default').connections;
var WAITING_CONFIG = require('../../config/default').waitings;
var COLLECTION_NAME = require('../../config/default').queueCollectionName;
var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var getLogger = require('../log/logger').getLogger;
var getError = require('../exception/exceptions').getError;
var async = require('async');
var util = require("util");
var dbBase = require("./base").dbBase;
var STATE = require("./base").STATE;
var REQUEST_RESULT = require("./base").REQUEST_RESULT;
var Subscriber = require("./subscriber").Subscriber;
var SUBSCRIBER_STATE = require("./subscriber").SUBSCRIBER_STATE;
var Event = require("./event").Event;
var levels = require("log4js").levels;

function MessageManager() {
	// sample data
	// eventSubscribers = {
	// 	"event1": [Subscriber1, Subscriber2 ...],
	// 	"event2": []
	// 		...
	// };
	this.logger = getLogger("MainLoop");
	this.eventSubscribers = {};
	this.waitingFor = {};
	_.each(WAITING_CONFIG, function(id) {
		this.waitingFor[id] = false;
	}.bind(this));
	this.allSubscribersReady = false;
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
			this.io = require('socket.io').listen(server, {
				log: false
			});
			this.io.sockets.on('connection', function(socket) {
				this.logger.info(util.format("[Connect]: Client connected: [%s].", socket.id));
				// client subscribes an event
				socket.on("subscribe", function(data, callback) {
					this.subscribe(socket, data, callback);

					// auto-unsubscribe when disconnected.
					socket.on("disconnect", function() {
						// debugger;
						this.logger.info(util.format("Client [%s] disconnected. Unsubscribing event [%s]", data.senderId, data.event));
						this.unsubscribe(data.event, data.senderId);
					}.bind(this));
				}.bind(this));

				// client enqueues an event
				socket.on("enqueue", function(data, callback) {
					this.enqueue(data, callback);
				}.bind(this));
			}.bind(this));

			// try to dispatch event every minutes
			setInterval(this.schedule.bind(this), 60000);
			var that = this;
			setInterval(function() {
				_.each(this.eventSubscribers, function(subscribers, event) {
					ids = _.pluck(subscribers, "id");
					that.logger.debug(util.format("Subscribers of [%s]: %s", event, ids.join(",")));
				})
				this.logger.debug(JSON.stringify(this.waitingFor));
			}.bind(this), 60000);
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
			this.logger.debug(util.format("Client [%s] already subscribed [%s].", data.senderId, data.event),
				getError("AlreadyConnected", existed.id));
		}
		var newSubscriber = new Subscriber(this, {
			id: data.senderId,
			socket: socket,
			event: data.event
		});
		subscribers.push(newSubscriber);
		// check if all the subscribers we are waiting are online.
		var allReady = true;
		if (typeof this.waitingFor[newSubscriber.id] != 'undefined') {
			this.waitingFor[newSubscriber.id] = true;
		}
		for (var key in this.waitingFor) {
			if (!this.waitingFor[key]) {
				allReady = false;
				break;
			}
		}
		this.allSubscribersReady = allReady;

		this.acknowledge(callback, {
			requestId: data.requestId,
			status: REQUEST_RESULT.SUCCESS
		});
		this.logger.info(util.format("[Subscribe]: Client \"%s\" subscribed event \"%s\".", data.senderId, data.event));
	},
	/**
	 * Unsubscribe by event and subscriber ID.
	 * @param  {string} event event name
	 * @param  {string} sId   subscriber ID
	 * @return {void}       void
	 */
	unsubscribe: function(event, sId) {
		var subscribers = this.eventSubscribers[event];
		for (var i = 0; i < subscribers.length; i++) {
			var subscriber = subscribers[i];
			if (subscriber.id == sId) {
				subscribers.splice(i, 1);
				this.logger.info(util.format("[Unsubscribe]: Client \"%s\" unsubscribed from event \"%s\"", sId, event));
				subscriber.dispose();
				break;
			}
		}
		if (this.waitingFor[sId]) {
			this.waitingFor[sId] = false;
			this.allSubscribersReady = false;
		}
	},
	// request sample
	// data = {
	// 	"requestId": "",	// mandatory. unique ID of each request.
	// 	"senderId": "",	// mandatory. unique name of sender.
	// 	"event": "enqueue", // mandatory.  event to trigger.
	//  "tryTimes": 1, 	// optional. defaults to 1. -1 = always.
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
		// 	"tryTimes": 1,	// mandatory. how many times should we try if fails. -1 = always.
		// 	"timeout": 60,	// mandatory. timeout in seconds.
		// 	"args": {},	// optional.
		// 	"subscribers": [{
		// 		subscriberId: "id1",
		// 		remainingTryTimes: 4,
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
				remainingTryTimes: data.tryTimes,
				state: STATE.READY,
				lastOperateTime: null
			}
		});
		this._getCollection().insert({
			"requestId": data.requestId,
			"senderId": data.senderId,
			"tryTimes": data.tryTimes,
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

			this.logger.info(util.format("[Event]: event \"%s\" submitted by sender \"%s\"", data.event, data.senderId));
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
		if (!this.allSubscribersReady) {
			// Still waiting for some subscriber to join. Do nothing.
			this.logger.info("Some subscriber(s) are not ready. Defer scheduling:\n" + JSON.stringify(this.waitingFor));
			return;
		}
		Event.createInstance(this.db, this.eventSubscribers, function(eventObj) {
			if (eventObj) {
				setTimeout(this.schedule.bind(this), 10);
				eventObj.dispatch();
			}
		}.bind(this))
	}
};

_.extend(MessageManager.prototype, dbBase);

exports.MessageManager = MessageManager;