'use strict';

const http = require('node:http');
const https = require('node:https');
const { URL } = require('node:url');

// ─── Minimal MessagePack encoder (only the types used by Blazor SignalR) ─────

function mpEncode(v) {
	if (v === null || v === undefined) {
		return Buffer.from([0xc0]);
	}
	if (typeof v === 'boolean') {
		return Buffer.from([v ? 0xc3 : 0xc2]);
	}
	if (typeof v === 'number' && Number.isInteger(v)) {
		if (v >= 0 && v <= 127) {
			return Buffer.from([v]);
		}
		if (v >= -32 && v < 0) {
			return Buffer.from([(v + 256) & 0xff]);
		}
		if (v >= 0 && v <= 0xff) {
			return Buffer.from([0xcc, v]);
		}
		if (v >= 0 && v <= 0xffff) {
			const b = Buffer.alloc(3);
			b[0] = 0xcd;
			b.writeUInt16BE(v, 1);
			return b;
		}
		if (v >= 0 && v <= 0xffffffff) {
			const b = Buffer.alloc(5);
			b[0] = 0xce;
			b.writeUInt32BE(v, 1);
			return b;
		}
		const b = Buffer.alloc(9);
		b[0] = 0xd3;
		b.writeBigInt64BE(BigInt(v), 1);
		return b;
	}
	if (typeof v === 'string') {
		const enc = Buffer.from(v, 'utf8');
		const len = enc.length;
		if (len <= 31) {
			return Buffer.concat([Buffer.from([0xa0 | len]), enc]);
		}
		if (len <= 0xff) {
			return Buffer.concat([Buffer.from([0xd9, len]), enc]);
		}
		const h = Buffer.alloc(3);
		h[0] = 0xda;
		h.writeUInt16BE(len, 1);
		return Buffer.concat([h, enc]);
	}
	if (Buffer.isBuffer(v) || v instanceof Uint8Array) {
		const bin = Buffer.isBuffer(v) ? v : Buffer.from(v);
		const len = bin.length;
		if (len <= 0xff) {
			return Buffer.concat([Buffer.from([0xc4, len]), bin]);
		}
		const h = Buffer.alloc(3);
		h[0] = 0xc5;
		h.writeUInt16BE(len, 1);
		return Buffer.concat([h, bin]);
	}
	if (Array.isArray(v)) {
		const len = v.length;
		const header =
			len <= 15
				? Buffer.from([0x90 | len])
				: (() => {
						const h = Buffer.alloc(3);
						h[0] = 0xdc;
						h.writeUInt16BE(len, 1);
						return h;
					})();
		return Buffer.concat([header, ...v.map(mpEncode)]);
	}
	if (typeof v === 'object') {
		const keys = Object.keys(v);
		const len = keys.length;
		const header =
			len <= 15
				? Buffer.from([0x80 | len])
				: (() => {
						const h = Buffer.alloc(3);
						h[0] = 0xde;
						h.writeUInt16BE(len, 1);
						return h;
					})();
		const pairs = keys.flatMap(k => [mpEncode(k), mpEncode(v[k])]);
		return Buffer.concat([header, ...pairs]);
	}
	return Buffer.from([0xc0]);
}

function mpDecode(buf) {
	let pos = 0;
	function next() {
		const b = buf[pos++];
		if (b <= 0x7f) {
			return b;
		}
		if (b >= 0xe0) {
			return b - 256;
		}
		if ((b & 0xe0) === 0xa0) {
			const len = b & 0x1f;
			const s = buf.toString('utf8', pos, pos + len);
			pos += len;
			return s;
		}
		if ((b & 0xf0) === 0x90) {
			return Array.from({ length: b & 0xf }, next);
		}
		if ((b & 0xf0) === 0x80) {
			const n = b & 0xf;
			const o = {};
			for (let i = 0; i < n; i++) {
				const k = next();
				o[k] = next();
			}
			return o;
		}
		switch (b) {
			case 0xc0:
				return null;
			case 0xc2:
				return false;
			case 0xc3:
				return true;
			case 0xca: {
				const v = buf.readFloatBE(pos);
				pos += 4;
				return v;
			}
			case 0xcb: {
				const v = buf.readDoubleBE(pos);
				pos += 8;
				return v;
			}
			case 0xcc:
				return buf[pos++];
			case 0xcd: {
				const v = buf.readUInt16BE(pos);
				pos += 2;
				return v;
			}
			case 0xce: {
				const v = buf.readUInt32BE(pos);
				pos += 4;
				return v;
			}
			case 0xcf: {
				const v = buf.readBigUInt64BE(pos);
				pos += 8;
				return Number(v);
			}
			case 0xd0: {
				const v = buf.readInt8(pos);
				pos++;
				return v;
			}
			case 0xd1: {
				const v = buf.readInt16BE(pos);
				pos += 2;
				return v;
			}
			case 0xd2: {
				const v = buf.readInt32BE(pos);
				pos += 4;
				return v;
			}
			case 0xd3: {
				const v = buf.readBigInt64BE(pos);
				pos += 8;
				return Number(v);
			}
			case 0xd9: {
				const n = buf[pos++];
				const s = buf.toString('utf8', pos, pos + n);
				pos += n;
				return s;
			}
			case 0xda: {
				const n = buf.readUInt16BE(pos);
				pos += 2;
				const s = buf.toString('utf8', pos, pos + n);
				pos += n;
				return s;
			}
			case 0xdb: {
				const n = buf.readUInt32BE(pos);
				pos += 4;
				const s = buf.toString('utf8', pos, pos + n);
				pos += n;
				return s;
			}
			case 0xc4: {
				const n = buf[pos++];
				const bin = buf.slice(pos, pos + n);
				pos += n;
				return bin;
			}
			case 0xc5: {
				const n = buf.readUInt16BE(pos);
				pos += 2;
				const bin = buf.slice(pos, pos + n);
				pos += n;
				return bin;
			}
			case 0xc6: {
				const n = buf.readUInt32BE(pos);
				pos += 4;
				const bin = buf.slice(pos, pos + n);
				pos += n;
				return bin;
			}
			case 0xdc: {
				const n = buf.readUInt16BE(pos);
				pos += 2;
				return Array.from({ length: n }, next);
			}
			case 0xdd: {
				const n = buf.readUInt32BE(pos);
				pos += 4;
				return Array.from({ length: n }, next);
			}
			case 0xde: {
				const n = buf.readUInt16BE(pos);
				pos += 2;
				const o = {};
				for (let i = 0; i < n; i++) {
					const k = next();
					o[k] = next();
				}
				return o;
			}
			case 0xdf: {
				const n = buf.readUInt32BE(pos);
				pos += 4;
				const o = {};
				for (let i = 0; i < n; i++) {
					const k = next();
					o[k] = next();
				}
				return o;
			}
			default:
				throw new Error(`msgpack unknown type 0x${b.toString(16)}`);
		}
	}
	return next();
}

// ─── Blazor SignalR message framing (VLQ length prefix + MessagePack body) ───

function writeVlq(n) {
	const bytes = [];
	do {
		let b = n & 0x7f;
		n >>>= 7;
		if (n > 0) {
			b |= 0x80;
		}
		bytes.push(b);
	} while (n > 0);
	return Buffer.from(bytes);
}

function encodeMessage(msg) {
	const payload = mpEncode(msg);
	return Buffer.concat([writeVlq(payload.length), payload]);
}

function decodeMessages(data) {
	const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
	const msgs = [];
	let i = 0;
	while (i < buf.length) {
		let len = 0,
			shift = 0,
			b;
		do {
			b = buf[i++];
			len |= (b & 0x7f) << shift;
			shift += 7;
		} while (b & 0x80);
		if (i + len > buf.length) {
			break;
		}
		try {
			msgs.push(mpDecode(buf.slice(i, i + len)));
		} catch {
			/* skip malformed frame */
		}
		i += len;
	}
	return msgs;
}

// ─── Blazor HTML parsing ─────────────────────────────────────────────────────

function extractBlazorComponents(html) {
	const pattern = /<!--Blazor:(.+?)-->/g;
	const components = [];
	let m;
	while ((m = pattern.exec(html)) !== null) {
		try {
			const json = m[1].replace(/\\u002B/g, '+').replace(/\\u002F/g, '/');
			const data = JSON.parse(json);
			if (data.type === 'server') {
				components.push({
					type: data.type,
					descriptor: data.descriptor || '',
					sequence: data.sequence || 0,
					prerenderId: data.prerenderId || '',
					key: data.key || {},
				});
			}
		} catch {
			/* skip malformed */
		}
	}
	return components;
}

function extractApplicationState(html) {
	let m = html.match(/<script[^>]+id="blazor-application-state"[^>]*>([^<]*)<\/script>/i);
	if (m) {
		return m[1].trim();
	}
	m = html.match(/id="blazor-application-state"[^>]+data-value="([^"]+)"/i);
	if (m) {
		return m[1].trim();
	}
	m = html.match(/blazor-application-state"[^>]*>([A-Za-z0-9+/=]{20,})</i);
	if (m) {
		return m[1].trim();
	}
	return '';
}

// ─── Blazor RenderBatch binary parser ────────────────────────────────────────
// Binary format (last 20 bytes = footer with 5 × uint32 LE section offsets):
//   footer[0] = updatedComponents, [1] = referenceFrames,
//   [2] = disposedComponentIds,    [3] = disposedEventHandlerIds,
//   [4] = strings
// Reference frames section: uint32 count + count × 20-byte frames
// Attribute frame (type 3): [frameType|seq|nameIdx|valueIdx|handlerId] each 4 bytes

const BLAZOR_FRAME_SIZE = 20;
const BLAZOR_FRAME_TYPE_ATTRIBUTE = 3;

function findOnclickHandlers(data) {
	if (data.length < 24) {
		return [];
	}
	const footerOff = data.length - 20;
	const refFramesOff = data.readUInt32LE(footerOff + 4);
	if (refFramesOff >= data.length - 4) {
		return [];
	}
	const frameCount = data.readUInt32LE(refFramesOff);
	const handlers = [];
	let pos = refFramesOff + 4;
	for (let i = 0; i < frameCount; i++) {
		if (pos + BLAZOR_FRAME_SIZE > data.length) {
			break;
		}
		if (data.readInt32LE(pos) === BLAZOR_FRAME_TYPE_ATTRIBUTE) {
			const handlerId = data.readUInt32LE(pos + 16);
			if (handlerId > 0) {
				handlers.push(handlerId);
			}
		}
		pos += BLAZOR_FRAME_SIZE;
	}
	return handlers;
}

// ─── HTML status parser ───────────────────────────────────────────────────────

// "Fast" is the Enpal internal name for Full/Schnellmodus
const MODE_NORMALIZE = { fast: 'Full' };

function extractStatusFromHtml(html) {
	let mode = null,
		status = null;
	const modeMatch = html.match(/Mode\s+(Eco|Full|Fast|Solar|Smart)/i);
	if (modeMatch) {
		const raw = modeMatch[1].charAt(0).toUpperCase() + modeMatch[1].slice(1).toLowerCase();
		mode = MODE_NORMALIZE[raw.toLowerCase()] || raw;
	}
	const connectorMatch = html.match(/Connector\s+(\w+)/i);
	const statusMatch = html.match(/\bStatus\s+(\w+)/i);
	if (connectorMatch) {
		status = connectorMatch[1];
	} else if (statusMatch) {
		status = statusMatch[1];
	}
	return { mode, status };
}

// ─── WallboxBlazorClient ──────────────────────────────────────────────────────

const BUTTON_ORDER = ['start', 'stop', 'eco', 'full', 'solar', 'smart'];
const PING_INTERVAL_MS = 15000;
const MAX_CONNECTION_AGE_MS = 300000;
const HTTP_TIMEOUT_MS = 10000;
const CLICK_TIMEOUT_MS = 10000;

function sleep(ms) {
	return new Promise(r => setTimeout(r, ms));
}

class WallboxBlazorClient {
	constructor(baseUrl, log) {
		this._baseUrl = baseUrl.replace(/\/$/, '');
		this._log = log;
		this._ws = null;
		this._connected = false;
		this._buttonHandlers = {};
		this._mode = null;
		this._status = null;
		this._components = [];
		this._appState = '';
		this._dotnetCallCounter = 0;
		this._rendererInteropId = 1;
		this._pendingClickCallId = null;
		this._clickResolve = null;
		this._clickReject = null;
		this._clickTimeoutHandle = null;
		this._connectedAt = 0;
		this._pingTimer = null;
	}

	get connected() {
		return this._connected;
	}

	get mode() {
		return this._mode;
	}

	get status() {
		return this._status;
	}

	async connect() {
		this._cleanup();
		this._buttonHandlers = {};
		this._mode = null;
		this._status = null;
		this._rendererInteropId = 1;
		this._dotnetCallCounter = 0;

		try {
			this._log.debug(`[Wallbox] Connecting to ${this._baseUrl}/wallbox`);

			// 1. GET /wallbox HTML → extract Blazor bootstrap data
			const html = await this._httpGet('/wallbox');
			this._components = extractBlazorComponents(html);
			this._appState = extractApplicationState(html);

			if (!this._components.length) {
				throw new Error('No Blazor components found in /wallbox HTML');
			}

			// 2. SignalR negotiate
			const neg = await this._httpPostJson('/_blazor/negotiate?negotiateVersion=1', '');
			if (!neg || !neg.connectionToken) {
				throw new Error('No connectionToken in negotiate response');
			}

			// 3. Open WebSocket (Node.js 22 built-in global)
			const parsed = new URL(this._baseUrl);
			const wsUrl = `ws://${parsed.host}/_blazor?id=${encodeURIComponent(neg.connectionToken)}`;

			this._ws = new WebSocket(wsUrl);
			this._ws.binaryType = 'arraybuffer';

			await new Promise((resolve, reject) => {
				const t = setTimeout(() => reject(new Error('WebSocket open timeout')), HTTP_TIMEOUT_MS);
				this._ws.addEventListener('open', () => {
					clearTimeout(t);
					resolve();
				});
				this._ws.addEventListener('error', () => {
					clearTimeout(t);
					reject(new Error('WebSocket connection error'));
				});
			});

			// 4. Blazor handshake (text frame): send protocol negotiation, receive ACK
			await new Promise((resolve, reject) => {
				const t = setTimeout(() => reject(new Error('Blazor handshake timeout')), 5000);
				this._ws.addEventListener(
					'message',
					e => {
						clearTimeout(t);
						const text = e.data instanceof ArrayBuffer ? Buffer.from(e.data).toString() : String(e.data);
						if (text.includes('"error"')) {
							reject(new Error(`Blazor handshake rejected: ${text}`));
						} else {
							resolve();
						}
					},
					{ once: true },
				);
				this._ws.send('{"protocol":"blazorpack","version":1}\x1e');
			});

			// 5. Start background message handler
			this._ws.addEventListener('message', e => {
				if (e.data instanceof ArrayBuffer) {
					this._handleMessages(Buffer.from(e.data)).catch(err => {
						this._log.debug(`[Wallbox] Message handler error: ${err.message}`);
					});
				}
			});
			this._ws.addEventListener('close', () => {
				this._connected = false;
				if (this._pingTimer) {
					clearInterval(this._pingTimer);
					this._pingTimer = null;
				}
				this._log.debug('[Wallbox] WebSocket closed');
			});

			// 6. Start Blazor circuit for /wallbox
			await this._sendStartCircuit();
			await sleep(300);
			await this._sendUpdateRootComponents();

			// 7. Wait up to 5 s for first RenderBatch to deliver button handler IDs
			for (let i = 0; i < 50; i++) {
				await sleep(100);
				if (Object.keys(this._buttonHandlers).length >= 6) {
					break;
				}
			}
			if (Object.keys(this._buttonHandlers).length === 0) {
				throw new Error('No wallbox button handlers received — page may be unsupported');
			}

			this._connected = true;
			this._connectedAt = Date.now();

			// 8. Keep-alive ping every 15 s
			this._pingTimer = setInterval(() => {
				if (this._ws && this._ws.readyState === 1 /* OPEN */) {
					try {
						this._ws.send(encodeMessage([6]));
					} catch {
						/* ignore */
					}
				}
			}, PING_INTERVAL_MS);

			this._log.info(
				`[Wallbox] Connected. Mode=${this._mode}, Status=${this._status}, Buttons=${Object.keys(this._buttonHandlers).join(',')}`,
			);
			return true;
		} catch (err) {
			this._log.error(`[Wallbox] Connect failed: ${err.message}`);
			this._cleanup();
			return false;
		}
	}

	_cleanup() {
		this._connected = false;
		if (this._pingTimer) {
			clearInterval(this._pingTimer);
			this._pingTimer = null;
		}
		if (this._clickTimeoutHandle) {
			clearTimeout(this._clickTimeoutHandle);
			this._clickTimeoutHandle = null;
		}
		if (this._clickReject) {
			this._clickReject(new Error('Connection closed'));
			this._clickReject = null;
			this._clickResolve = null;
			this._pendingClickCallId = null;
		}
		if (this._ws) {
			try {
				this._ws.close();
			} catch {
				/* ignore */
			}
			this._ws = null;
		}
	}

	close() {
		this._cleanup();
	}

	_isStale() {
		if (!this._connected || !this._ws || this._ws.readyState !== 1) {
			return true;
		}
		return Date.now() - this._connectedAt > MAX_CONNECTION_AGE_MS;
	}

	async ensureFreshConnection() {
		if (this._isStale()) {
			return await this.connect();
		}
		return true;
	}

	async clickButton(button) {
		if (!(await this.ensureFreshConnection())) {
			throw new Error('[Wallbox] Cannot connect to Enpal Box for wallbox control');
		}
		const handlerId = this._buttonHandlers[button];
		if (handlerId === undefined) {
			throw new Error(
				`[Wallbox] Unknown button: "${button}". Available: ${Object.keys(this._buttonHandlers).join(', ')}`,
			);
		}

		this._dotnetCallCounter++;
		const callId = this._dotnetCallCounter;
		this._pendingClickCallId = callId;

		const eventDescriptor = { eventHandlerId: handlerId, eventName: 'click', eventFieldInfo: null };
		const eventArgs = {
			type: 'click',
			detail: 1,
			screenX: 0,
			screenY: 0,
			clientX: 0,
			clientY: 0,
			offsetX: 0,
			offsetY: 0,
			pageX: 0,
			pageY: 0,
			movementX: 0,
			movementY: 0,
			button: 0,
			buttons: 0,
			ctrlKey: false,
			shiftKey: false,
			altKey: false,
			metaKey: false,
		};
		const argsJson = JSON.stringify([eventDescriptor, eventArgs]);
		const clickMsg = [
			1,
			{},
			null,
			'BeginInvokeDotNetFromJS',
			[String(callId), null, 'DispatchEventAsync', this._rendererInteropId, argsJson],
		];

		return new Promise((resolve, reject) => {
			this._clickResolve = resolve;
			this._clickReject = reject;
			this._clickTimeoutHandle = setTimeout(() => {
				this._pendingClickCallId = null;
				this._clickResolve = null;
				this._clickReject = null;
				reject(new Error(`[Wallbox] Click timeout for button "${button}"`));
			}, CLICK_TIMEOUT_MS);

			this._sendMessage(clickMsg).catch(err => {
				clearTimeout(this._clickTimeoutHandle);
				this._pendingClickCallId = null;
				this._clickResolve = null;
				this._clickReject = null;
				reject(err);
			});
		});
	}

	async start() {
		return this.clickButton('start');
	}

	async stop() {
		return this.clickButton('stop');
	}

	async setMode(mode) {
		return this.clickButton(mode.toLowerCase());
	}

	async fetchStatus() {
		try {
			const html = await this._httpGet('/wallbox');
			const { mode, status } = extractStatusFromHtml(html);
			if (mode) {
				this._mode = mode;
			}
			if (status) {
				this._status = status;
			}
			if (!mode && !status) {
				this._log.warn(
					`[Wallbox] Status page reachable but no mode/status found in HTML. ` +
						`Check that ${this._baseUrl}/wallbox shows the wallbox controls.`,
				);
			}
			this._log.debug(`[Wallbox] Status: mode=${this._mode}, status=${this._status}`);
			return { mode: this._mode, status: this._status };
		} catch (err) {
			this._log.warn(`[Wallbox] Status fetch failed (${this._baseUrl}/wallbox): ${err.message}`);
			return { mode: this._mode, status: this._status };
		}
	}

	// ─── Internal HTTP helpers ─────────────────────────────────────────────────

	_httpGet(path) {
		return new Promise((resolve, reject) => {
			const url = new URL(this._baseUrl + path);
			const lib = url.protocol === 'https:' ? https : http;
			const req = lib.request(
				{
					hostname: url.hostname,
					port: parseInt(url.port) || (url.protocol === 'https:' ? 443 : 80),
					path: url.pathname + url.search,
					method: 'GET',
					headers: { Accept: 'text/html,*/*' },
				},
				res => {
					let data = '';
					res.on('data', c => (data += c));
					res.on('end', () => {
						if (res.statusCode !== 200) {
							reject(new Error(`HTTP ${res.statusCode} for GET ${path}`));
						} else {
							resolve(data);
						}
					});
				},
			);
			req.on('error', reject);
			req.setTimeout(HTTP_TIMEOUT_MS, () => req.destroy(new Error('HTTP timeout')));
			req.end();
		});
	}

	_httpPostJson(path, body) {
		return new Promise((resolve, reject) => {
			const url = new URL(this._baseUrl + path);
			const lib = url.protocol === 'https:' ? https : http;
			const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
			const req = lib.request(
				{
					hostname: url.hostname,
					port: parseInt(url.port) || (url.protocol === 'https:' ? 443 : 80),
					path: url.pathname + url.search,
					method: 'POST',
					headers: {
						'Content-Type': 'application/json',
						'Content-Length': Buffer.byteLength(bodyStr),
					},
				},
				res => {
					let data = '';
					res.on('data', c => (data += c));
					res.on('end', () => {
						if (res.statusCode !== 200) {
							reject(new Error(`HTTP ${res.statusCode} for POST ${path}`));
						} else {
							try {
								resolve(JSON.parse(data));
							} catch {
								reject(new Error(`Invalid JSON in response for POST ${path}`));
							}
						}
					});
				},
			);
			req.on('error', reject);
			req.setTimeout(HTTP_TIMEOUT_MS, () => req.destroy(new Error('HTTP timeout')));
			req.end(bodyStr);
		});
	}

	// ─── Blazor SignalR protocol messages ──────────────────────────────────────

	async _sendMessage(msg) {
		if (!this._ws || this._ws.readyState !== 1) {
			throw new Error('[Wallbox] WebSocket not open');
		}
		this._ws.send(encodeMessage(msg));
	}

	async _sendStartCircuit() {
		const msg = [
			1,
			{},
			'0',
			'StartCircuit',
			[`${this._baseUrl}/`, `${this._baseUrl}/wallbox`, '[]', this._appState],
		];
		await this._sendMessage(msg);
	}

	async _sendUpdateRootComponents() {
		const operations = this._components.map((comp, i) => ({
			type: 'add',
			ssrComponentId: i + 1,
			marker: {
				type: comp.type,
				prerenderId: comp.prerenderId,
				key: comp.key,
				sequence: comp.sequence,
				descriptor: comp.descriptor,
				uniqueId: i,
			},
		}));
		const batchJson = JSON.stringify({ batchId: 1, operations });
		await this._sendMessage([1, {}, null, 'UpdateRootComponents', [batchJson, this._appState]]);
	}

	async _sendOnRenderCompleted(batchId) {
		await this._sendMessage([1, {}, null, 'OnRenderCompleted', [batchId, null]]);
	}

	async _sendEndInvokeJs(taskId) {
		const resultJson = `[${taskId},true,null]`;
		await this._sendMessage([1, {}, null, 'EndInvokeJSFromDotNet', [taskId, true, resultJson]]);
	}

	// ─── Incoming message dispatcher ──────────────────────────────────────────

	async _handleMessages(buf) {
		const msgs = decodeMessages(buf);
		for (const msg of msgs) {
			if (!Array.isArray(msg) || msg.length === 0) {
				continue;
			}
			const msgType = msg[0];

			if (msgType === 6) {
				continue;
			} // ping — no response needed

			if (msgType === 3) {
				// Completion for StartCircuit / hub invocations
				const resultKind = msg[3];
				if (resultKind === 1) {
					this._log.warn(`[Wallbox] Server invocation error: ${msg[4]}`);
				}
				continue;
			}

			if (msgType === 7) {
				// Server closing connection
				this._log.warn(`[Wallbox] Server sent Close: ${msg[1] || ''}`);
				this._connected = false;
				continue;
			}

			if (msgType !== 1 || msg.length < 4) {
				continue;
			}

			const target = msg[3];
			const args = msg[4] || [];

			if (target === 'JS.RenderBatch') {
				const batchId = args[0];
				const batchData = args[1];
				if (batchData instanceof Buffer || batchData instanceof Uint8Array) {
					this._processRenderBatch(Buffer.isBuffer(batchData) ? batchData : Buffer.from(batchData));
				}
				if (batchId != null) {
					await this._sendOnRenderCompleted(batchId);
				}
			} else if (target === 'JS.BeginInvokeJS') {
				// Capture renderer DotNet object ref ID (needed for DispatchEventAsync)
				if (args[2] && typeof args[2] === 'string' && args[2].includes('"__dotNetObject"')) {
					try {
						const parsed = JSON.parse(args[2]);
						if (Array.isArray(parsed)) {
							for (const item of parsed) {
								if (item && typeof item === 'object' && typeof item['__dotNetObject'] === 'number') {
									this._rendererInteropId = item['__dotNetObject'];
									this._log.debug(
										`[Wallbox] Captured renderer interop ID: ${this._rendererInteropId}`,
									);
									break;
								}
							}
						}
					} catch {
						/* ignore parse errors */
					}
				}
				if (args[0] != null) {
					await this._sendEndInvokeJs(args[0]);
				}
			} else if (target === 'JS.EndInvokeDotNet') {
				// Response to our BeginInvokeDotNetFromJS (button click)
				const callId = parseInt(args[0]);
				const success = args[1];
				const result = args[2];

				if (callId === this._pendingClickCallId) {
					clearTimeout(this._clickTimeoutHandle);
					this._pendingClickCallId = null;
					const resolve = this._clickResolve;
					const reject = this._clickReject;
					this._clickResolve = null;
					this._clickReject = null;
					if (success) {
						resolve(true);
					} else {
						reject(new Error(`[Wallbox] Server rejected click: ${result}`));
					}
				}
			}
		}
	}

	_processRenderBatch(data) {
		// Extract button onclick handler IDs from the binary RenderBatch
		const handlers = findOnclickHandlers(data);
		if (handlers.length >= 6) {
			const wallboxHandlers = handlers.slice(-6);
			this._buttonHandlers = Object.fromEntries(BUTTON_ORDER.map((name, i) => [name, wallboxHandlers[i]]));
			this._log.debug(`[Wallbox] Button handlers: ${JSON.stringify(this._buttonHandlers)}`);
		}

		// Extract mode/status from text content in the batch
		const text = data.toString('utf8', 0, Math.min(data.length, 4096));
		const { mode, status } = extractStatusFromHtml(text);
		if (mode) {
			this._mode = mode;
		}
		if (status) {
			this._status = status;
		}
	}
}

module.exports = { WallboxBlazorClient, extractStatusFromHtml };
