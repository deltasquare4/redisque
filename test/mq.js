/* global describe, it, before, after, beforeEach, afterEach */

var async = require('async');
var chai = require('chai');
var should = chai.should();
var moment = require('moment');
var sinon = require('sinon');

var redisque = require('../index');


/**
* Test Redis MessageQueue Interface
*/
describe('redisque', function () {

	var testQueue, messageId, scheduler;

	var clearQueue = function (queue, done) {
		queue.get(100, function (error, messages) {
			var index = 1;
			async.eachSeries(messages, function (message, callback) {
				testQueue.delete(message.id, callback);
			}, done);
		});
	};

	before(function (done) {
		testQueue = redisque.connect('testQueue', config);
		scheduler = redisque.getScheduler(config);

		clearQueue(testQueue, done);
	});

	it('should be able to put a message', function (done) {
		testQueue.put('Message 1', done);
	});

	it('should be able to put multiple messages in a single call', function (done) {
		var messages = [
			'Message 2',
			'Message 3',
			'Message 4',
			'Message 5',
			'Message 6',
		];
		testQueue.put(messages, done);
	});

	it('should receive and delete a message', function (done) {
		testQueue.get(6, function (error, messages) {
			should.not.exist(error);
			should.exist(messages);
			var index = 1;
			async.eachSeries(messages, function (message, callback) {
				should.exist(message.body);
				message.body.should.equal('Message ' + index++);
				testQueue.delete(message.id, callback);
			}, done);
		});
	});

	it('should support custom message attributes', function (done) {
		var sample = {
			id: 'Best custom id is 123',
			body: 'Custom attributes test',
			timeout: 5
		};

		testQueue.put(sample, function (error) {
			should.not.exist(error);
			testQueue.get(function (error, messages) {
				should.not.exist(error);
				should.exist(messages);
				var message = messages[0];
				should.exist(message.body);
				message.body.should.equal('Custom attributes test');
				message.id.should.equal('Best custom id is 123');
				testQueue.delete(message.id, done);
			});
		});
	});

	describe('Poller', function () {
		var clock;
		before(function (done) {
			scheduler.stop();
			scheduler.once('stop', function () {
				clock = sinon.useFakeTimers();
				scheduler.start();
				clock.tick(1000);
				process.nextTick(function () {
					scheduler.once('startTimeout', done);
				});
			});
		});

		it('should push message with custom timeout', function (done) {
			testQueue.put('Timeout example', { timeout: 2 }, function (error) {
				should.not.exist(error);
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					var message = messages[0];
					should.exist(message.body);
					message.body.should.equal('Timeout example');
					done();
				});
			});
		});

		it('should not be able to get the same message before timeout', function (done) {
			testQueue.get(function (error, messages) {
				should.not.exist(error);
				should.exist(messages);
				messages.length.should.equal(0);
				done();
			});
		});

		it('should move timed-out messages back to queue', function (done) {
			clock.tick(2000);

			scheduler.once('timeout', function () {
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					var message = messages[0];
					should.exist(message.body);
					message.body.should.equal('Timeout example');
					testQueue.delete(message.id, done);
				});
			});
		});

		after(function () {
			clock.restore();
		});
	});

	describe('Scheduler', function () {
		var clock;
		before(function (done) {
			scheduler.stop();
			scheduler.once('stop', function () {
				clock = sinon.useFakeTimers();
				scheduler.start();
				clock.tick(1000);
				scheduler.once('startScheduler', done);
			});
		});

		it('should push message with delay as date', function (done) {
			clock.tick(1000);
			var after10sec = moment(Date.now()).add(9, 'seconds').toDate();
			testQueue.put('Delay example 1', { delay: after10sec }, function (error) {
				should.not.exist(error);
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					messages.length.should.equal(0);
					done();
				});
			});
		});

		it('should push message with delay in seconds', function (done) {
			clock.tick(1000);
			testQueue.put('Delay example 2', { delay: 3 }, function (error) {
				should.not.exist(error);
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					messages.length.should.equal(0);
					done();
				});
			});
		});

		it('should get delayed messsage 2 after 5 seconds', function (done) {
			clock.tick(3000);

			scheduler.once('expire', function () {
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					var message = messages[0];
					should.exist(message.body);
					message.body.should.equal('Delay example 2');
					testQueue.delete(message.id, done);
				});
			});
		});

		it('should get delayed messsage 1 after 10 seconds', function (done) {
			clock.tick(5000);

			scheduler.once('expire', function (remaining) {
				testQueue.get(function (error, messages) {
					should.not.exist(error);
					should.exist(messages);
					var message = messages[0];
					should.exist(message.body);
					message.body.should.equal('Delay example 1');
					testQueue.delete(message.id, done);
				});
			});
		});

		after(function (done) {
			clock.restore();
			scheduler.stop();
			scheduler.once('stop', done);
		});
	});
});

