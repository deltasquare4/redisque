var assert = require('assert');
var crypto = require('crypto');
var fs = require('fs');
var path = require('path');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var _ = require('lodash');
var backoff = require('backoff');
var debug = require('debug')('redisque');
var moment = require('moment');
var redis = require('redis');
// var Promise = require("bluebird");

// // Promisify Redis API
// Promise.promisifyAll(redis.RedisClient.prototype);

// Scripts
var getMessages = fs.readFileSync(path.join(__dirname, '/scripts/getMessages.lua'));
var putMessages = fs.readFileSync(path.join(__dirname, '/scripts/putMessages.lua'));
var removeMessage = fs.readFileSync(path.join(__dirname, '/scripts/removeMessage.lua'));
var releaseMessage = fs.readFileSync(path.join(__dirname, '/scripts/releaseMessage.lua'));
var moveTimedoutMessages = fs.readFileSync(path.join(__dirname, '/scripts/moveTimedoutMessages.lua'));
var acquireLock = fs.readFileSync(path.join(__dirname, '/scripts/acquireLock.lua'));
var moveExpiredMessages = fs.readFileSync(path.join(__dirname, '/scripts/moveExpiredMessages.lua'));

// Helper functions
function noop() {}

var defaults = _.partialRight(_.assign, function (a, b) {
	return typeof a === 'undefined' ? b : a;
});

/*
 * Execute a Redis Lua `script` by trying `evalsha` first, falling
 * back to `eval` if the script hasn't been cached yet.
 */
function _evalScript(/* script ... args, callback */) {
	var args = [].slice.call(arguments);
	var script = args.shift();
	var callback = args.pop();
	var self = this;

	var sha = crypto.createHash('sha1').update(script).digest('hex');
	this.client.evalsha.apply(this.client, [sha, args.length].concat(args).concat([ function (error, result) {
		if (error) {
			// Retry using eval() if script is not cached
			if (error.toString().match(/NOSCRIPT/)) {
				/* jshint evil:true */
				self.client.eval.apply(self.client, [script, args.length].concat(args).concat([ function (error, result) {
					callback(error, result);
				}]));
			} else {
				callback(error);
			}
		} else {
			callback(null, result);
		}
	}]));
}

/*
 * MessageQueue: Scheduler Message Queue backed by Redis.
 *
 * Scheduler implemented using Sorted sets, where messages are ordered by timestamp (ms).
 * Main Queue is a list, listened to with blocking pop. Popped messages are moved to another list/set for failsafe.
 *
 */
var MessageQueue = exports.MessageQueue = function (name, config) {
	EventEmitter.call(this);
	var self = this;
	// Queue name
	// Create new client for every queue???

	// TODO: Initialize Redis client/pool
	assert.ok(name, 'Queue name must exist');
	config = _.defaults(config, {
		host: 'localhost',
		port: 6379,
	});

	self.client = redis.createClient(config.port, config.host);
	self.client.select(1, function () {
		// TODO: Handle?
		self.emit('ready');
	});

	// Names
	self.name = name;
	self.prefix = 'q:' + self.name;
	self.queueName = self.prefix + ':act';
	self.storageName = self.prefix + ':data';
	self.archiveName = self.prefix + ':del';

	// Defaults
	self.options = {
		timeout: 60
	};
	self.messagesPerCall = 1;
};

// Make MessageQueue an EventEmitter
util.inherits(MessageQueue, EventEmitter);

// Put message(s) into the queue
MessageQueue.prototype.put = function (messages, options, callback) {
	if (typeof(options) === 'function') {
		callback = options;
		options = {};
	}
	options = defaults(options, this.options);

	if (!Array.isArray(messages)) {
		messages = [ messages ];
	}

	messages = _.map(messages, function (message) {
		var res = {};
		if (typeof(message) === 'object') {
			// Validation
			if (!message.body || typeof(message.body) !== 'string') {
				return callback(new Error('Valid message body not found.'));
			}
			res = message;
		} else {
			res.body = message;
		}
		res = defaults(res, options);

		if (res.delay) {
			if (res.delay instanceof Date) {
				res.delay = res.delay.getTime();
			} else if (!isNaN(res.delay)) {
				res.delay = Number(res.delay);
				res.delay = moment().add(res.delay * 1000, 'ms').toDate().getTime();
			} else {
				debug('Delay should be a date or a number', res);
				return null;
			}

			// Convert delay in seconds
			res.delay = Math.round(res.delay / 1000);
		}

		return res;
	});

	_evalScript.call(this, putMessages, this.queueName, JSON.stringify(messages), callback);
};

// Get specified number of messages from the queue
MessageQueue.prototype.get = function (count, callback) {
	if (typeof(count) === 'function') {
		callback = count;
		count = this.messagesPerCall;
	}

	_evalScript.call(this, getMessages, JSON.stringify({
		queue: this.queueName,
		count: count,
		now: Math.round(Date.now() / 1000)
	}), function (error, messages) {
		if (error) { return callback(error); }

		try {
			messages = JSON.parse(messages);

			// Hack to turn empty message object into array.
			// This happens because lua doesn't differentiate between arrays and hashes.
			if (!Array.isArray(messages)) {
				messages = [];
			}
		} finally {
			callback(null, messages);
		}
	});
};

// Delete/archive specified message
MessageQueue.prototype.delete = function (id, callback) {
	var self = this;

	_evalScript.call(self, removeMessage, JSON.stringify({
		queue: self.queueName,
		id: id
	}), function (error) {
		if (callback) {
			callback(error);
		} else {
			if (error) {
				self.emit('error', error);
			} else {
				self.emit('delete', id);
			}
		}
	});
};

// Release specified message
// TODO: Add tests
MessageQueue.prototype.release = function (id, callback) {
	var self = this;

	_evalScript.call(self, releaseMessage, JSON.stringify({
		queue: self.queueName,
		id: id
	}), function (error) {
		if (callback) {
			callback(error);
		} else {
			if (error) {
				self.emit('error', error);
			} else {
				self.emit('release', id);
			}
		}
	});
};

// Update messages
// TODO: Add tests
MessageQueue.prototype.update = function (messages, options, callback) {
	if (!Array.isArray(messages)) {
		messages = [ messages ];
	}

	messages = _.map(messages, function (message) {
		var res = {};
		if (typeof(message) !== 'object' || !message.id) {
			// Validation
			return callback(new Error('Message id not found.'));
		}
		return res;
	});

	this.put(messages, options, callback);
};

exports.connect = function (name, config) {
	config = config || this._config;
	return new MessageQueue(name, config);
};

exports.close = function (queue) {
	queue.client.end();
	queue.client = null;
	queue = null;
};

exports.config = function (config) {
	this._config = config;
};


var Scheduler = function (config) {
	EventEmitter.call(this);
	var self = this;
	assert.ok(config, 'Config must exist');
	assert.ok(config.host, 'host must exist');
	assert.ok(config.port, 'port must exist');

	self.client = redis.createClient(config.port, config.host);
	self.client.select(1, function () {
		self.start();
	});

	self.running = false;
	self.gateKeeper = exports.getGateKeeper(config);
};

// Make Scheduler an EventEmitter
util.inherits(Scheduler, EventEmitter);

Scheduler.prototype.start = function () {
	// TODO: Hack for testing
	var initialDelay = 1000;
	if (process.env.NODE_ENV && process.env.NODE_ENV === 'testing') {
		initialDelay = 1;
	}

	var self = this;
	if (!self.running) {
		// Initialize polling for scheduled queue
		if (!self._schedulerBackoff) {
			self._schedulerBackoff = backoff.fibonacci({
				randomisationFactor: 0,
				initialDelay: initialDelay,
				maxDelay: 30000,
			});

			self._schedulerBackoff.on('backoff', function () {
				debug('Backing off on scheduler loop');
			});

			self._schedulerBackoff.on('ready', function (count, delay) {
				debug('Scheduler polling...');
				// Try to acquire polling lock
				self.gateKeeper.acquire('schedulerLock', delay, function (error, resp) {
					if (error) { return self.emit('error', error); }

					if (resp) {
						debug('Success acquiring scheduler lock');
						self.emit('startScheduler');
						if (!self._schedulerInterval) {
							self._schedulerInterval = setInterval(function () {
								self.pollSchedules();
							}, 1000);
						}
					} else {
						debug('Scheduler lock is already acquired by other process');
						clearInterval(self._schedulerInterval);
						self._schedulerInterval = null;
					}
					self.running = true;
					self._schedulerBackoff.backoff();
				});
			});
		}

		// Initialize polling for timeout queue
		if (!self._timeoutBackoff) {
			self._timeoutBackoff = backoff.fibonacci({
				randomisationFactor: 0,
				initialDelay: initialDelay,
				maxDelay: 30000,
			});

			self._timeoutBackoff.on('backoff', function () {
				debug('Backing off on timeout loop');
			});

			self._timeoutBackoff.on('ready', function (count, delay) {
				debug('Timeout polling...');
				// Try to acquire polling lock
				self.gateKeeper.acquire('timeoutLock', delay, function (error, resp) {
					if (error) { return self.emit('error', error); }

					if (resp) {
						debug('Success acquiring timeout lock');
						self.emit('startTimeout');
						if (!self._timeoutInterval) {
							self._timeoutInterval = setInterval(function () {
								self.pollProcessing();
							}, 2000);
						}
					} else {
						debug('Timeout lock is already acquired by other process');
						clearInterval(self._timeoutInterval);
						self._timeoutInterval = null;
					}
					self.running = true;
					self._timeoutBackoff.backoff();
				});
			});
		}

		self._schedulerBackoff.backoff();
		self._timeoutBackoff.backoff();
	}
};

Scheduler.prototype.stop = function () {
	var self = this;
	if (this.running) {
		clearInterval(self._schedulerInterval);
		clearInterval(self._timeoutInterval);
		self._schedulerInterval = null;
		self._timeoutInterval = null;

		this._schedulerBackoff.reset();
		this._timeoutBackoff.reset();
		self.gateKeeper.release('schedulerLock', function (error) {
			if (error) { return self.emit('error', error); }
			self.gateKeeper.release('timeoutLock', function (error) {
				if (error) { return self.emit('error', error); }
				self.running = false;
				self.emit('stop');
			});
		});
	}
};

Scheduler.prototype.pollSchedules = function () {
	var self = this;
	_evalScript.call(this, moveExpiredMessages, JSON.stringify({
		now: Math.round(Date.now() / 1000)
	}), function (error, remaining) {
		if (error) { return self.emit('error', error); }
		self.emit('expire', remaining);
	});
};

Scheduler.prototype.pollProcessing = function () {
	var self = this;
	_evalScript.call(this, moveTimedoutMessages, JSON.stringify({
		now: Math.round(Date.now() / 1000)
	}), function (error, remaining) {
		if (error) { return self.emit('error', error); }
		self.emit('timeout', remaining);
	});
};

exports.getScheduler = function (config) {
	config = config || this._config;
	if (!this._scheduler) {
		this._scheduler = new Scheduler(config);
	}
	return this._scheduler;
};

/*
 * GateKeeper: Maintains global locks backed by redis
 */
var GateKeeper = function (config) {
	assert.ok(config, 'Config must exist');
	assert.ok(config.host, 'host must exist');
	assert.ok(config.port, 'port must exist');

	this.client = redis.createClient(config.port, config.host);
	this.client.select(1, noop);

	// Defaults
	this.prefix = 'locks:';
};

GateKeeper.prototype.acquire = function (name, duration, callback) {
	if (typeof(duration) === 'function') {
		callback = duration;
		duration = 60000; // Default to 60 seconds
	}

	_evalScript.call(this, acquireLock, this.prefix + name, duration, String(process.pid), callback);
};

GateKeeper.prototype.release = function (name, callback) {
	this.client.del(this.prefix + name, callback);
};

exports.getGateKeeper = function (config) {
	config = config || this._config;
	if (!this._gateKepper) {
		this._gateKepper = new GateKeeper(config);
	}
	return this._gateKepper;
};
