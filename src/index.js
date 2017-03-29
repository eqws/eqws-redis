const Adapter = require('../../eqws-adapter');
const uid2 = require('uid2');
const msgpack = require('msgpack-lite');
const redis = require('redis').createClient;

const debug = require('debug')('eqws:redis');
const error = require('debug')('eqws:redis:error');

class RedisAdapter extends Adapter {
	constructor(wss, opts = {}) {
		super(wss, opts);

		this.uid = uid2(6);
		this._pub = opts.pubClient || createClient();
		this._sub = opts.subClient || createClient();
		this._prefix = opts.prefix || 'eqws';

		this._channel = this._prefix;
		this._requestChannel = this._channel + '-req#';
		this._responseChannel = this._channel + '-res#';

		this._sub.subscribe([this._channel, this._requestChannel, this._responseChannel], onError);
		this._sub.on('messageBuffer', this._onMessage.bind(this));

		this._pub.on('error', onError);
		this._sub.on('error', onError);

		function createClient() {
			if (opts.uri) {
				return redis(opts.uri);
			} else {
				return redis(opts);
			}
		}

		function onError(err) {
			if (err) this.emit('error', err);
		}
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

	broadcast(packet, rooms, remote = false) {
		if (!remote) {
			const data = packet.encode();
			const msg = msgpack.encode([this.uid, data, rooms]);

			this._pub.publish(this._channel, msg);
		}

		Adapter.prototype.broadcast.call(this, packet, rooms);
	}

	_channelMatches(channel, pattern) {
		return channel.substr(0, pattern.length) === pattern;
	}
}

module.exports = RedisAdapter;