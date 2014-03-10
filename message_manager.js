var DB_CONFIG = require('config').connections;
var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var getLogger = require('./logger').getLogger;
var getError = require('./exceptions').getError;
var async = require('async');

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

function MessageManager() {
	// sample data
	// eventSubscribers = {
	// 	"event1": [{
	// 		subscriberId: "id1",
	// 		socket: socket1
	// 	}, {
	// 		subscriberId: "id2",
	// 		socket: socket2
	// 	}, ...],
	// 	"event2": []
	// 		...
	// };
	this.logger = getLogger("MainLoop");
	this.eventSubscribers = {};
}

MessageManager.prototype = {
	// @return {status: <[true, false]>, error: (optional)}
	validate: function(data) {
		var result = {
			requestId: data.requestId,
			status: REQUEST_RESULT.SUCCESS
		};
		// 数据合法性检查
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
	listen: function(port, host) {
		var that = this;

		var client = MongoClient.connect(DB_CONFIG.url, function(err, db) {
			if (err) {
				var error = getError("DatabaseUnavailable");
				this.logger.fatal("无数连接到数据库。", error);
				return;
			}

			that.db = db;
			var server = http.createServer(port, host ? host : "0.0.0.0");
			this.io = require('socket.io').listen(server);
			this.io.sockets.on('connection', function(socket) {
				// 订阅事件
				socket.on("subscribe", function(data, callback) {
					that.subscribe(socket, data, callback);
				});

				// 增加事件到队列
				socket.on("enqueue", function(data, callback) {
					that.enqueue(socket, data, callback);
				});
			});

			// 每分钟尝试分发事件
			setInterval(function() {
				that.dispatch();
			}, 600000);
		});
	},
	// 请求示例
	// data = {
	// 	"requestId": "",	// mandatory. unique ID of each request.
	// 	"senderId": "",	// mandatory. unique name of sender.
	// 	"event": "enqueue", // mandatory. options: enqueue/ack/command.
	// 	"params": {},	// optional. only available when action=command
	// }
	subscribe: function(socket, data, callback) {
		var result = this.validate(data);
		if (result.status == REQUEST_RESULT.FAIL) {
			this.acknowledge(callback, result);
			return;
		}

		if (!this.eventSubscribers[data.event]) {
			this.eventSubscribers[data.event] = [];
		}

		var subscribers = this.eventSubscribers[data.event];
		// 这个应用是否已经订阅过
		var existed = _.find(subscribers, function(subscriber) {
			return subscriber.senderId = data.senderId;
		});

		if (existed) {
			// 已经订阅，关闭之前的连接，用现在的连接代替。
			var existedSocket = existed.socket;
			existed.socket = socket;
			if (existedSocket.connected) {
				existedSocket.disconnect();
				this.logger.warn("客户端和服务端已经存在连接。", getError("AlreadyConnected", senderId));
			}
		} else {
			subscribers.push({
				subscriberId: data.senderId,
				socket: socket
			});
		}
		this.acknowledge(callback, {
			reqeustId: data.requestId,
			status: REQUEST_RESULT.SUCCESS
		});
	},
	unsubscribe: function(event, subscriberId) {
		var subscribers = this.eventSubscribers[event];
		if (subscribers[subscriberId] && subscribers[subscriberId].socket.connected) {
			subscribers[subscriberId].socket.disconnect();
		}
		delete subscribers[subscriberId];
		this.logger.info(util.format("已退订[%s]", subscriberId));
	},
	// 请求示例
	// data = {
	// 	"requestId": "",	// mandatory. unique ID of each request.
	// 	"senderId": "",	// mandatory. unique name of sender.
	// 	"event": "enqueue", // mandatory. options: enqueue/ack/command.
	//  "retryLimit": 1 	// optional. defaults to 0. -1 = always.
	// 	"args": {},	// optional. only available when action=command
	// }
	enqueue: function(data, callback) {
		// 数据示例
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
		var result = this.validate(data);
		if (result.status == REQUEST_RESULT.FAIL) {
			this.acknowledge(callback, result);
			return;
		}

		var that = this;
		// 查找这个事件有多少订阅者
		var subscribers = _.map(this.eventSubscribers[data.event], function(elm) {
			return {
				subscriberId: elm.senderId,
				remainingRetryTimes: data.retryLimit,
				state: STATE.READY,
				lastOperateTime: null
			}
		});
		this.db.collection("queue").insert({
			"requestId": data.requestId,
			"senderId": data.senderId,
			"retryLimit": data.retryLimit,
			"timeout": data.timeout,
			"event": data.event,
			"args": data.args,
			"createAt": new Date(),
			"state": STATE.READY,
			"subscribers": subscribers
		}, function(err, doc) {
			if (err) {
				that.logger.fatal("无法将新请求添加到数据库。", err);
				that.acknowledge(callback, "fail", err);
			}

			that.acknowledge("success");
			that.dispatch();
		});
	},
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
	dispatch: function() {
		var that = this;

		// 找出READY状态的事件，进行处理。一次只处理一条数据。
		this.db.collection("queue").findAndModify({
			"$or": [{
				state: STATE.READY
			}, {
				state: STATE.RETRY
			}]
		}, {
			createAt: 1
		}, {
			"$set": {
				state: STATE.PROCESSING
			}
		}, {

		}, function(err, record) {
			if (err) {
				this.logger.fatal("无法更新请求状态READY/RETRY->PROCESSING。", err);
				return;
			}

			if (record) {
				// 发起下一个请求处理更多数据
				setTimeout(this.dispatch.bind(this), 10);
			} else {
				// 已没有更多需要处理的数据
				return;
			}

			var event = record.event;
			var subscribers = record.subscribers;
			// 查找所有需要处理的subscriber
			var readySubscribers = [];
			_.each(subscribers, function(s) {
				// TODO: 实现可控制的重试时间
				if ((s.remainingRetryTimes > 0 || s.remainingRetryTimes == -1) && (s.state == STATE.READY || s.state == STATE.RETRY) && (s.lastOperateTime == null || (new Date() - s.lastOperateTime) > 60000)) {
					readySubscribers.push(s);
					s.remainingRetryTimes -= s.remainingRetryTimes > 0 ? 1 : 0;
					s.state = STATE.PROCESSING;
					s.lastOperateTime = new Date();
				}
			});

			// 批量更新数据库，设置当前记录的subscribers为正在处理
			this.db.collection("queue").update({
				"_id": record["_id"]
			}, record, function(err) {
				if (err) {
					// 无法更新数据库，分发失败
					this.logger.fatal("无法更新subscribers的请求状态READY/RETRY->PROCESSING", err);
					// TODO: 尝试改回READY或RETRY状态
					return;
				}

				var subscriberSockets = _.indexBy(this.eventSubscribers[event], 'subscriberId');
				// 成功更新数据库，开始通知客户端
				// 并行处理一条记录中的所有订阅者
				async.map(readySubscribers, (function(subscriber, callback) {
					// 获取socket
					// TODO: 如果socket不存在的错误处理
					var socket = subscriberSockets[subscriber.subscriberId].socket;
					// 数据库更新成功，向客户端推送事件
					// 如果规定时间内没有返回任何信息，视为失败
					var timeoutHandler = setTimeout(function() {
						that.unsubscribe(event, subscriber.subscriberId);
						callback(null, {
							subscriberId: subscriber.subscriberId,
							status: REQUEST_RESULT.FAIL
						})
					}, record.timeout);
					socket.emit(event, doc.args, function(data) {
						clearTimeout(timeoutHandler);
						// 通知并行调用结果
						callback(null, {
							subscriberId: subscriber.subscriberId,
							status: data.status
						});
					});
				}).bind(that), function(err, results) {
					// 因为不会返回err，所以err一定不存在。通过判断results来判断执行是否成功
					_.each(subscribers, function(s) {
						var subscriberStatus = _.find(results, function(status) {
							return status.subscriberId == s.subscriberId;
						});
						if (subscriberStatus) {
							switch (subscriberStatus.status) {
								case REQUEST_RESULT.SUCCESS:
									s.state = STATE.DONE;
									break;
								case REQUEST_RESULT.FAIL:
									s.state = s.remainingRetryTimes == 0 ? STATE.FAIL : STATE.RETRY;
									break;
							}
						}
					})
				});
			});
		});
	}
};