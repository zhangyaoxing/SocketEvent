var getLogger = require('./logger').getLogger;
var logger = getLogger("MainLoop");

var errors = {
	"AlreadyConnected": {
		message: "The client [%s] which is already connected is trying to connect again. Old connection disconnected."
	},
	"ArgumentError": {
		message: "Lack of argument [%s]."
	},
	"UnknownError": {
		message: "Unknown error occured."
	},
	"UnexpectedErrorKey": {
		message: "Error key [%s] doesn't exist. Check your code."
	},
	"DatabaseUnavailable": {
		message: "Unable to connect to database."
	}
};

exports.getError = function(key) {
	if (errors[key]) {
		var msg = errors[key].message;
		for (var i = 1; i < arguments.length; i++) {
			msg = util.format(msg, arguments[i]);
		}
		var error = new Error(msg);
		error.name = key;
	} else {
		var msg = util.format(errors["UnknownError"].message, key);
		var error = new Error(msg);
		error.name = "UnknownError";
	}

	return error;
};