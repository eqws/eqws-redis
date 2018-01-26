const Adapter = require('eqws-adapter');
const uid2    = require('uid2');
const msgpack = require('msgpack-lite');
const redis   = require('redis').createClient;

const debug = require('debug')('eqws:redis');
const error = require('debug')('eqws:redis:error');

const REQUEST_TYPES = {
	CLIENTS: 0,
	CLIENT_ROOMS: 1,
	ALL_ROOMS: 2,
	REMOTE_JOIN: 3,
	REMOTE_LEAVE: 4,
	REMOTE_DISCONNECT: 5,
	CUSTOM_REQUEST: 6
};

class RedisAdapter extends Adapter {
	constructor(wss, opts = {}) {
		super(wss, opts);

		this.uid = uid2(6);
		this._pub = opts.pubClient || createClient();
		this._sub = opts.subClient || createClient();
		this._prefix = opts.prefix || 'eqws';
		this._requests = {};
		this._requestsTimeout = opts.requestsTimeout || 1000;

		this._channel = this._prefix;
		this._requestChannel = this._channel + '-req#';
		this._responseChannel = this._channel + '-res#' + this.uid;

		this._sub.subscribe([this._channel, this._requestChannel, this._responseChannel], onError);
		this._sub.on('messageBuffer', this._onMessage.bind(this));

		this._pub.on('error', onError);
		this._sub.on('error', onError);

		const self = this;

		function createClient() {
			if (opts.uri) {
				return redis(opts.uri);
			} else {
				return redis(opts);
			}
		}

		function onError(err) {
			if (err) self.emit('error', err);
		}

		this.on('error', error);
	}

	broadcast(packet, rooms, remote = false) {
		if (!remote) {
			const data = packet.encode();
			const msg = msgpack.encode([this.uid, data, rooms]);

			this._pub.publish(this._channel, msg);
		}

		Adapter.prototype.broadcast.call(this, packet, rooms);
	}

	clients(rooms) {
		return new Promise((resolve, reject) => {
			this._pub.send_command('pubsub', ['numsub', this._requestChannel], (err, numsub) => {
				if (err) {
					this.emit('error', err);
					return reject(err);
				}

				numsub = parseInt(numsub[1], 10);

				const request = {
					id: uid2(6),
					type: REQUEST_TYPES.CLIENTS,
					rooms
				};

				this._request(request, {
					resCount: 0,
					clients: [],
					numsub
				}).then(resolve).catch(reject);
			});
		});
	}

	remoteJoin(id, rooms) {
		return this._remoteRoom(id, rooms, REQUEST_TYPES.REMOTE_JOIN);
	}

	remoteLeave(id, rooms) {
		return this._remoteRoom(id, rooms, REQUEST_TYPES.REMOTE_LEAVE);
	}

	_remoteRoom(id, rooms, type) {
		// Adapt rooms array
		if (!Array.isArray(rooms)) rooms = [rooms];

		const socket = this._wss._connected[id];

		if (socket) {
			return socket.join(rooms);
		}

		const request = {
			id: uid2(6),
			type: type,
			sid: id,
			rooms: rooms
		};

		return this._request(request);
	}

	_request(req, meta) {
		return new Promise((resolve, reject) => {
			const cb = reject.bind(null, new Error('timeout for response'));
			const timeout = setTimeout(cb, this._requestsTimeout);

			this._requests[req.id] = {
				type: req.type,
				promise: {resolve, reject},
				timeout,
				meta
			};

			req.channel = this._responseChannel;
			req.uid = this.uid;

			const data = msgpack.encode(req);

			debug('publish request=%j', req);
			this._pub.publish(this._requestChannel, data);
		});
	}

	_response(id, channel, result) {
		const response = {id, result};
		const encoded = msgpack.encode(response); 

		debug('publish response=%j channel=%s', response, channel);
		return this._pub.publish(channel, encoded);
	}

	_onMessage(channel, msg) {
		channel = channel.toString();

		if (this._channelMatches(channel, this._requestChannel)) {
			return this._onRequest(msg);
		} else if (this._channelMatches(channel, this._responseChannel)) {
			return this._onResponse(msg);
		} else if (!this._channelMatches(channel, this._channel)) {
			return debug('ignore unkown channel');
		}

		const args = msgpack.decode(msg);

		if (this.uid == args.shift()) return debug('ignore same uid');

		args.push(true);
		this.broadcast.apply(this, args);
	}

	_onResponse(msg) {
		const res = msgpack.decode(msg);
		const req = res.id ? this._requests[res.id] : null;
		const self = this;

		if (!req) return debug('ignoring unknown request');

		debug('received response %j', res);

		switch (req.type) {
			case REQUEST_TYPES.CLIENTS:
				req.meta.resCount++;
				if (!Array.isArray(res.result)) return;
				req.meta.clients = req.meta.clients.concat(res.result);

				if (req.meta.resCount === req.meta.numsub) {
					return done(req.meta.clients);
				}

			case REQUEST_TYPES.CLIENT_ROOMS:
				return done(res.result);

			case REQUEST_TYPES.ALL_ROOMS:
				// TOO-DOO
				return done(res.result);

			case REQUEST_TYPES.REMOTE_JOIN:
			case REQUEST_TYPES.REMOTE_LEAVE:
			case REQUEST_TYPES.REMOTE_DISCONNECT:
				return done(res.result);

			default:
				debug('unknown request type: %s', req.type);
		}

		function done() {
			clearTimeout(req.timeout);
			delete self._requests[res.id];
			req.promise.resolve.apply(null, arguments);
		}
	}

	_onRequest(msg) {
		const req = msgpack.decode(msg);

		if (this.uid == req.uid) return debug('ignore same request uid');

		const res  = this._response.bind(this, req.id, req.channel);
		const errorHandler = this.emit.bind(this, 'error');

		debug('received request %j', req);

		switch (req.type) {
			case REQUEST_TYPES.CLIENTS:
				return Adapter.prototype.clients.call(this, req.rooms).then(list => {
					if (!list) return;
					res(list);
				}).catch(errorHandler);

			case REQUEST_TYPES.CLIENT_ROOMS:
				return Adapter.prototype.clientRooms.call(this, req.rooms).then(list => {
					if (!list) return;
					res(list);
				}).catch(errorHandler);

			case REQUEST_TYPES.ALL_ROOMS:
				var list = Object.keys(this._rooms);
				return res(list);

			case REQUEST_TYPES.REMOTE_JOIN:
				var socket = this._wss._connected[req.sid];
				if (!socket) return;
				return socket.join(req.rooms).then(res).catch(errorHandler);

			case REQUEST_TYPES.REMOTE_LEAVE:
				var socket = this._wss._connected[req.sid];
				if (!socket) return;
				return socket.leave(req.rooms).then(res).catch(errorHandler);

			case REQUEST_TYPES.REMOTE_DISCONNECT:
				var socket = this._wss._connected[req.sid];
				if (!socket) return;
				socket.close();
				return res(true);

			default:
				debug('unknown request type: %s', req.type);
		}
	}

	_channelMatches(channel, pattern) {
		return channel.substr(0, pattern.length) === pattern;
	}
}

module.exports = RedisAdapter;