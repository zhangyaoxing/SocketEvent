var DB_CONFIG = require('../../config/default').connections;
var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var getLogger = require('../log/logger').getLogger;
var getError = require('../exception/exceptions').getError;
var util = require("util");
var dbBase = require("./base").dbBase;
var STATE = require("./base").STATE;
var REQUEST_RESULT = require("./base").REQUEST_RESULT;
var COLLECTION_NAME = require('../../config/default').queueCollectionName;

// find the earliest event with status READY or RETRY.
// only one record is proceeded at one time.
function Event(db, record, subscribers) {
	this.db = db;
	this.originalRecord = record;
	this.originalRecord.state = STATE.PROCESSING;
	this.logger = getLogger("EventTrigger");

	// looking up ready subscribers
	var readySubscriberIds = [];
	_.each(record.subscribers, function(s) {
		if ((s.remainingTryTimes > 0 || s.remainingTryTimes == -1) && (s.state == STATE.READY || s.state == STATE.RETRY) && (s.lastOperateTime == null || (new Date() - s.lastOperateTime) > 60000)) {
			readySubscriberIds.push(s.subscriberId);
			s.remainingTryTimes -= s.remainingTryTimes > 0 ? 1 : 0;
			s.state = STATE.PROCESSING;
			s.lastOperateTime = new Date();
		}
	});

	// get subscribers to be notified
	this.subscribers = _.filter(subscribers, function(s) {
		return _.contains(readySubscriberIds, s.id);
	});
}

Event.prototype = {
	_updateSubscriber: function(result) {
		var subscriberFound = false;
		var subscriberToBeUpdated = _.find(this.originalRecord.subscribers, function(s) {
			return s.subscriberId == result.subscriberId;
		});
		subscriberFound = !!subscriberToBeUpdated;
		if (!subscriberFound) {
			// if it's not found in the original list
			// it means the subscriber comes later than "enqueu" operation
			// we need to add insert it to database
			subscriberToBeUpdated = {
				subscriberId: result.subscriberId,
				remainingTryTimes: this.originalRecord.tryTimes - 1,
				lastOperateTime: new Date()
			};
			this.originalRecord.subscribers.push(subscriberToBeUpdated);
		}
		switch (result.status) {
			case REQUEST_RESULT.SUCCESS:
				subscriberToBeUpdated.state = STATE.DONE;
				break;
			default:
			case REQUEST_RESULT.FAIL:
				subscriberToBeUpdated.state = subscriberToBeUpdated.remainingTryTimes == 0 ? STATE.FAIL : STATE.RETRY;
				break;
		}
		this._getCollection().update({
			"_id": this.originalRecord["_id"],
			subscribers: {
				"$elemMatch": {
					subscriberId: result.subscriberId
				}
			}
		}, {
			"$set": {
				"subscribers.$": subscriberToBeUpdated
			}
		}, function(err) {
			if (err) {
				this.logger.error("Failed to update subscriber state: PROCESSING->" + subscriberToBeUpdated.state + "\n" + JSON.stringify(subscriberToBeUpdated),
					getError("DatabaseUnavailable"));
			}
		}.bind(this));
	},
	dispatch: function() {
		// batch update all the subscriber status in current record to PROCESSING
		this._getCollection().update({
			"_id": this.originalRecord["_id"]
		}, {
			"$set": this.originalRecord.subscribers
		}, function(err) {
			if (err) {
				// unable to update subscriber state from READY/RETRY to PROCESSING 
				this.logger.fatal("Failed to update subscribers state: READY/RETRY->PROCESSING", err);
				// TODO: try to revert record state from PROCESSING back to READY/RETRY
				return;
			}

			var finalizeDatabase = function() {
				this._getCollection().update({
					"_id": this.originalRecord["_id"]
				}, {
					"$set": {
						state: this.originalRecord.state
					}
				}, function(err) {
					if (err) {
						this.logger.error("Failed to update record state PROCESSING->" + this.originalRecord.state + "\n" + this.originalRecord["_id"],
							getError("DatabaseUnavailable"));
					}
				}.bind(this));
			}.bind(this);
			var updated = 0;
			if (this.subscribers.length == 0) {
				// TODO: this is a bug
				this.originalRecord.state = STATE.DONE;
				finalizeDatabase();
				return;
			} else {
				_.each(this.subscribers, function(subscriber) {
					subscriber.notify(this.originalRecord.args, this.originalRecord.timeout, function(result) {
						this._updateSubscriber(result);
						if (++updated == this.subscribers.length) {
							var states = _.countBy(this.originalRecord.subscribers, function(s) {
								return s.state;
							});

							// normally there should be only RETRY/DONE/FAIL.
							// in rare situations there could be other states.
							var done = states[STATE.DONE];
							var retry = states[STATE.RETRY];
							var fail = states[STATE.FAIL];
							var ready = states[STATE.READY];
							var processing = states[STATE.PROCESSING];
							if (done && !retry && !fail && !ready && !processing) {
								// all done
								this.originalRecord.state = STATE.DONE;
							} else if (!retry && !ready && !processing) {
								// nothing else than DONE/FAIL. all DONE is filtered out so FAIL.
								this.originalRecord.state = STATE.FAIL;
							} else {
								// retriable
								this.originalRecord.state = STATE.RETRY;
							}
							finalizeDatabase();
						}
					}.bind(this));
				}.bind(this));
			}
		}.bind(this));
	}
};

/**
 * Create an instance of Event.
 * @param  {[type]}   db               [db connection used to access mongodb]
 * @param  {[type]}   eventSubscribers [subscriber objects]
 * @param  {Function} callback         [description]
 * @return {[type]}                    [Event object]
 */
Event.createInstance = function(manager, callback) {
	if (!manager.allSubscribersReady) {
		// Still waiting for some subscriber to join. Do nothing.
		this.logger.info("Some subscriber(s) are not ready. Defer scheduling:");
		this.logger.info(JSON.stringify(this.waitingFor));
		callback(null);
		return;
	}
	var logger = getLogger("EventTrigger");
	var db = manager.db;
	var eventSubscribers = manager.eventSubscribers;

	// TODO: if it's RETRY, there must be some subscriber available
	db.collection(COLLECTION_NAME).findAndModify({
		"$or": [{
			state: STATE.READY
		}, {
			state: STATE.RETRY
		}]
	}, {
		createAt: 1
	}, {
		"$set": {
			state: STATE.PROCESSING,
			lastOperateTime: new Date()
		}
	}, {
		new: false
	}, function(err, record) {
		if (err) {
			logger.fatal("Cannot update request state: READY/RETRY->PROCESSING. (No manual operation required)", err);
			callback(null);
			return;
		}
		if (!record) {
			callback(null);
			return;
		}

		// allSubscribersReady can change while waiting for database query
		if (!manager.allSubscribersReady) {
			logger.info("Critical subscriber(s) unsubscribed. Deferring event scheduling.");
			logger.info(JSON.stringify(this.waitingFor));

			// rollback state change
			db.collection(COLLECTION_NAME).update({
				_id: record._id
			}, {
				$set: {
					state: record.state
				}
			}, function(err) {
				if (err) {
					var msg = util.format("Deferring event scheduling failed because of database error.\n\
						This requires manual fixing. Record ID: %s", record._id);
					logger.fatal(msg, err);
				}
				callback(null);
				return;
			});
			return;
		}

		logger.debug(util.format("Going to dispatch event _id: %s", record._id));
		callback(new Event(db, record, subscribers));
	});
};

_.extend(Event.prototype, dbBase);

exports.Event = Event;