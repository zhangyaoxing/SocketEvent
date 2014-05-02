var log4js = require('log4js');
var util = require("util");

log4js.configure({
	appenders: [{
		type: 'console'
	}]
});

function Logger(logger) {
	this.logger = logger;
}

Logger.prototype = (function() {
	var generateMsg = function(msg, err) {
		var finalMsg = msg;
		if (util.isError(err)) {
			finalMsg = util.format("%s\n%s\n%s", msg, err.message, err.stack);
		}

		return finalMsg;
	};
	return {
		trace: function(msg, err) {
			this.logger.trace(generateMsg(msg, err));
		},
		debug: function(msg, err) {
			this.logger.debug(generateMsg(msg, err));
		},
		info: function(msg, err) {
			this.logger.info(generateMsg(msg, err));
		},
		warn: function(msg, err) {
			this.logger.warn(generateMsg(msg, err));
		},
		error: function(msg, err) {
			this.logger.error(generateMsg(msg, err));
		},
		fatal: function(msg, err) {
			this.logger.fatal(generateMsg(msg, err));
		}
	}
})();

exports.getLogger = function(category) {
	var log4jsLogger = log4js.getLogger(category);
	log4jsLogger.setLevel(log4js.levels.DEBUG);
	return new Logger(log4jsLogger);
};