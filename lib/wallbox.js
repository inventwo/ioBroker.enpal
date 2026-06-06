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
// Footer (last 20 bytes): 5 × uint32 LE section offsets.
// The exact order and frame size varies by .NET / Blazor version, so we
// try all plausible combinations automatically.

const BLAZOR_FRAME_TYPE_ATTRIBUTE = 3;

function findOnclickHandlers(data, log) {
	if (data.length < 24) {
		if (log) {
			log.debug(`[Wallbox] findOnclickHandlers: data too short (${data.length} bytes)`);
		}
		return [];
	}
	const footerOff = data.length - 20;
	const footerVals = [
		data.readUInt32LE(footerOff),
		data.readUInt32LE(footerOff + 4),
		data.readUInt32LE(footerOff + 8),
		data.readUInt32LE(footerOff + 12),
		data.readUInt32LE(footerOff + 16),
	];
	if (log) {
		log.debug(`[Wallbox] RenderBatch footer: [${footerVals.join(',')}]`);
	}

	// Try every combination: which footer value is refFramesOff, and which frame
	// size / handler-ID byte-offset applies for this Blazor version.
	const layouts = [
		[16, 12],
		[20, 12],
		[20, 16],
		[24, 12],
		[24, 16],
		[24, 20],
		[28, 12],
		[28, 16],
		[28, 20],
		[28, 24],
	];

	// Try every combination; keep the result with the most handlers found.
	let best = { handlers: [], fi: -1, frameSize: 0, handlerOffset: 0, refOff: 0 };

	for (const [frameSize, handlerOffset] of layouts) {
		for (let fi = 0; fi < 5; fi++) {
			const refFramesOff = footerVals[fi];
			if (refFramesOff === 0 || refFramesOff >= data.length - 4) {
				continue;
			}
			const frameCount = data.readUInt32LE(refFramesOff);
			if (frameCount === 0 || frameCount > 10000) {
				continue;
			}
			if (refFramesOff + 4 + frameCount * frameSize > data.length + frameSize) {
				continue;
			}
			const handlers = [];
			let pos = refFramesOff + 4;
			for (let i = 0; i < frameCount; i++) {
				if (pos + frameSize > data.length) {
					break;
				}
				if (data.readInt32LE(pos) === BLAZOR_FRAME_TYPE_ATTRIBUTE) {
					const handlerId = data.readUInt32LE(pos + handlerOffset);
					if (handlerId > 0 && handlerId < 100000) {
						handlers.push(handlerId);
					}
				}
				pos += frameSize;
			}
			if (handlers.length > best.handlers.length) {
				best = { handlers, fi, frameSize, handlerOffset, refOff: refFramesOff };
			}
		}
	}

	if (best.handlers.length > 0) {
		if (log) {
			log.info(
				`[Wallbox] Frame scan OK: footerIdx=${best.fi} refOff=${best.refOff} frameSize=${best.frameSize} handlerOff=${best.handlerOffset} → ${best.handlers.length} handler(s): [${best.handlers.join(',')}]`,
			);
		}
		return best.handlers;
	}

	// Second pass: collect sequence numbers from attribute frames with sentinel=0xffffffff.
	// In .NET 8 Blazor, real onclick event-handler IDs may all be 0 in the initial batch;
	// the sequence number identifies the button's position in the render tree instead.
	// We collect them as a fallback, flagged negative so the caller can distinguish.
	for (let fi = 0; fi < 5; fi++) {
		const refFramesOff = footerVals[fi];
		if (refFramesOff === 0 || refFramesOff >= data.length - 4) {
			continue;
		}
		const frameCount = data.readUInt32LE(refFramesOff);
		if (frameCount === 0 || frameCount > 10000) {
			continue;
		}
		// Try known attribute-frame sizes; look for sentinel=0xffffffff pattern
		for (const frameSize of [28, 24, 20]) {
			if (refFramesOff + 4 + frameCount * frameSize > data.length + frameSize) {
				continue;
			}
			const seqs = [];
			let pos = refFramesOff + 4;
			for (let i = 0; i < frameCount; i++) {
				if (pos + frameSize > data.length) {
					break;
				}
				if (data.readInt32LE(pos) === BLAZOR_FRAME_TYPE_ATTRIBUTE) {
					// Check for sentinel=0xffffffff at offset 12 or 16 (event-handler marker)
					const s12 = data.readUInt32LE(pos + 12);
					const s16 = pos + 16 < data.length ? data.readUInt32LE(pos + 16) : 0;
					const hasSentinel = s12 === 0xffffffff || s16 === 0xffffffff;
					if (hasSentinel) {
						const seq = data.readUInt32LE(pos + 4);
						if (seq >= 0 && seq < 100) {
							seqs.push(seq + 1); // offset by 1 so 0-seq becomes handler-id 1
						}
					}
				}
				pos += frameSize;
			}
			if (seqs.length >= 2) {
				if (log) {
					log.info(
						`[Wallbox] Sentinel scan: footerIdx=${fi} frameSize=${frameSize} → ${seqs.length} seq-based handler(s): [${seqs.join(',')}]`,
					);
				}
				return seqs;
			}
		}
	}

	// Nothing found — log end-of-batch hex for manual analysis
	if (log) {
		const dumpStart = Math.max(0, footerOff - 80);
		log.info(
			`[Wallbox] No handlers found. End-of-batch hex (${data.length} bytes total): ${data.slice(dumpStart).toString('hex')}`,
		);
	}
	return [];
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
		this._startCircuitResolve = null;
		this._startCircuitReject = null;
		this._allHandlers = [];
		this._framesDumped = false;
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
		this._dotnetCallCounter = 0;
		this._rendererInteropId = 1;
		this._browserRendererId = 0;
		this._framesDumped = false;

		try {
			this._log.info(`[Wallbox] Connecting to ${this._baseUrl}/wallbox …`);

			// 1. GET /wallbox HTML → extract Blazor bootstrap data
			const html = await this._httpGet('/wallbox');
			this._components = extractBlazorComponents(html);
			this._appState = extractApplicationState(html);
			this._log.info(
				`[Wallbox] Step 1 OK: HTML ${html.length} bytes, ${this._components.length} Blazor component(s), appState ${this._appState.length} chars`,
			);

			if (!this._components.length) {
				throw new Error('No Blazor components found in /wallbox HTML');
			}

			// 2. SignalR negotiate
			const neg = await this._httpPostJson('/_blazor/negotiate?negotiateVersion=1', '');
			if (!neg || !neg.connectionToken) {
				throw new Error('No connectionToken in negotiate response');
			}
			this._log.info(`[Wallbox] Step 2 OK: negotiate token=${neg.connectionToken.slice(0, 10)}…`);

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
				this._ws.addEventListener('error', e => {
					clearTimeout(t);
					reject(new Error(`WebSocket connection error: ${e.message || e.type || e}`));
				});
			});
			this._log.info('[Wallbox] Step 3 OK: WebSocket open');

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
			this._log.info('[Wallbox] Step 4 OK: Blazor handshake');

			// 5. Start background message handler — accept ArrayBuffer, Buffer, and Uint8Array
			this._ws.addEventListener('message', e => {
				let bin = null;
				if (e.data instanceof ArrayBuffer) {
					bin = Buffer.from(e.data);
				} else if (Buffer.isBuffer(e.data)) {
					bin = e.data;
				} else if (e.data instanceof Uint8Array) {
					bin = Buffer.from(e.data);
				}
				if (bin) {
					this._handleMessages(bin).catch(err => {
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

			// 6. Start Blazor circuit for /wallbox — wait for completion before proceeding
			const circuitOk = await new Promise(resolve => {
				this._startCircuitResolve = resolve;
				this._startCircuitReject = null;
				const t = setTimeout(() => {
					this._startCircuitResolve = null;
					this._log.info('[Wallbox] StartCircuit: no completion within 3 s, proceeding anyway');
					resolve(true);
				}, 3000);
				this._startCircuitResolve = result => {
					clearTimeout(t);
					this._startCircuitResolve = null;
					resolve(result);
				};
				this._sendStartCircuit().catch(err => {
					clearTimeout(t);
					this._startCircuitResolve = null;
					this._log.warn(`[Wallbox] StartCircuit send error: ${err.message}`);
					resolve(false);
				});
			});
			this._log.info(`[Wallbox] Step 5 OK: StartCircuit result=${circuitOk}`);

			// 7. Wait up to 5 s for all render batches from StartCircuit to arrive.
			// Batches with all 6 button handlers arrive within ~300 ms of StartCircuit.
			// We exit as soon as all handlers are known (minimum 200 ms buffer for late
			// batches).  Do NOT send UpdateRootComponents — it is a Blazor Web App
			// method, not valid for Blazor Server, and causes an immediate server Close.
			for (let i = 0; i < 50; i++) {
				await sleep(100);
				if (i >= 2 && Object.keys(this._buttonHandlers).length >= BUTTON_ORDER.length) {
					break;
				}
			}
			const handlerCount = Object.keys(this._buttonHandlers).length;
			this._log.info(
				`[Wallbox] Step 6: received ${handlerCount} button handler(s): ${JSON.stringify(this._buttonHandlers)}`,
			);
			if (handlerCount === 0) {
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
		if (this._startCircuitReject) {
			this._startCircuitReject(new Error('Connection closed'));
			this._startCircuitReject = null;
			this._startCircuitResolve = null;
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
			const available = Object.keys(this._buttonHandlers);
			const modeButtons = ['eco', 'full', 'solar', 'smart'];
			if (modeButtons.includes(button) && available.length > 0 && !available.some(k => modeButtons.includes(k))) {
				throw new Error(
					`[Wallbox] Lademodus-Schaltflächen nicht verfügbar – bitte sicherstellen, dass ein Fahrzeug an der Wallbox angeschlossen ist.`,
				);
			}
			throw new Error(`[Wallbox] Unknown button: "${button}". Available: ${available.join(', ')}`);
		}

		// Blazor Server .NET 8: dispatch the click via DispatchEventAsync on the
		// DotNetObjectRef obtained from attachWebRendererInterop.
		// Blazor .NET 8: DispatchEventAsync(WebEventData) takes a SINGLE combined object.
		// browserRendererId must match the value from attachWebRendererInterop (typically 1).
		// eventArgs is a pre-serialized JSON string inside the WebEventData object.
		this._dotnetCallCounter += 1;
		const callId = this._dotnetCallCounter;
		this._pendingClickCallId = callId;

		const mouseArgs = {
			type: 'click',
			detail: 1,
			screenX: 100,
			screenY: 100,
			clientX: 100,
			clientY: 100,
			offsetX: 10,
			offsetY: 10,
			pageX: 100,
			pageY: 100,
			movementX: 0,
			movementY: 0,
			button: 0,
			buttons: 0,
			ctrlKey: false,
			shiftKey: false,
			altKey: false,
			metaKey: false,
		};

		// Single WebEventData object (not two separate args)
		const webEventData = {
			browserRendererId: this._browserRendererId,
			eventHandlerId: handlerId,
			eventArgsType: 'mouse',
			eventFieldInfo: null,
			eventArgs: JSON.stringify(mouseArgs),
		};
		const argsJson = JSON.stringify([webEventData]);
		const clickMsg = [
			1,
			{},
			null,
			'BeginInvokeDotNetFromJS',
			[String(callId), null, 'DispatchEventAsync', this._rendererInteropId, argsJson],
		];

		this._log.info(
			`[Wallbox] Sending DispatchEventAsync: button=${button} handlerId=${handlerId} callId=${callId} rendererInteropId=${this._rendererInteropId} browserRendererId=${this._browserRendererId}`,
		);

		return new Promise((resolve, reject) => {
			const TIMEOUT_MS = 8000;
			this._clickResolve = resolve;
			this._clickReject = reject;
			this._clickTimeoutHandle = setTimeout(() => {
				this._pendingClickCallId = null;
				this._clickResolve = null;
				this._clickReject = null;
				reject(new Error(`[Wallbox] Click timeout for button "${button}" (no EndInvokeDotNet received)`));
			}, TIMEOUT_MS);

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
		// Pass the actual component descriptors extracted from the HTML.
		// For .NET 6/7 Blazor Server, the server needs these to re-activate
		// pre-rendered components and start sending RenderBatches.
		const componentRecords = JSON.stringify(
			this._components.map(c => ({
				type: c.type,
				descriptor: c.descriptor,
				sequence: c.sequence,
				prerenderId: c.prerenderId,
				key: c.key,
			})),
		);
		const msg = [
			1,
			{},
			'0',
			'StartCircuit',
			[`${this._baseUrl}/`, `${this._baseUrl}/wallbox`, componentRecords, this._appState],
		];
		this._log.debug(`[Wallbox] StartCircuit: sending ${this._components.length} component descriptor(s)`);
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
				// Completion message (e.g. StartCircuit response)
				const invId = msg[2];
				const resultKind = msg[3];
				const result = msg[4];
				this._log.info(
					`[Wallbox] Completion invId=${invId} kind=${resultKind} result=${JSON.stringify(result)}`,
				);
				if (invId === '0' && this._startCircuitResolve) {
					// resultKind 3 = value: accept true (bool) or a non-empty string (circuit ID in .NET 8)
					const ok =
						resultKind === 3
							? result === true || (typeof result === 'string' && result.length > 0)
							: resultKind === 2;
					this._startCircuitResolve(ok);
				}
				if (resultKind === 1) {
					this._log.warn(`[Wallbox] Server invocation error: ${result}`);
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
				this._log.info(`[Wallbox] Unhandled message type=${msgType} len=${msg.length}`);
				continue;
			}

			const target = msg[3];
			const args = msg[4] || [];

			// Log all server→client invocations during init (before connected)
			if (!this._connected) {
				this._log.info(`[Wallbox] Server call: ${target} (args[0]=${JSON.stringify(args[0])})`);
			}

			if (target === 'JS.RenderBatch') {
				const batchId = args[0];
				const batchData = args[1];
				const typeName =
					batchData == null ? 'null' : batchData.constructor ? batchData.constructor.name : typeof batchData;
				this._log.debug(
					`[Wallbox] JS.RenderBatch batchId=${batchId}, dataType=${typeName}, size=${batchData && batchData.length != null ? batchData.length : '?'}`,
				);
				let binData = null;
				if (batchData instanceof Buffer) {
					binData = batchData;
				} else if (batchData instanceof Uint8Array) {
					binData = Buffer.from(batchData);
				} else if (batchData instanceof ArrayBuffer) {
					binData = Buffer.from(batchData);
				}
				if (binData) {
					this._processRenderBatch(binData);
				} else {
					this._log.warn(
						`[Wallbox] RenderBatch data is not binary (type=${typeName}) — cannot extract button handlers`,
					);
				}
				if (batchId != null) {
					await this._sendOnRenderCompleted(batchId);
				}
			} else if (target === 'JS.BeginInvokeJS') {
				const taskId = args[0];
				const jsFnName = args[1];
				const jsArgsRaw = args[2];
				// Always log the JS function being invoked — crucial for debugging
				this._log.info(
					`[Wallbox] JS.BeginInvokeJS taskId=${taskId} fn=${jsFnName} args=${typeof jsArgsRaw === 'string' ? jsArgsRaw.slice(0, 200) : JSON.stringify(jsArgsRaw)}`,
				);
				// Capture browserRendererId and DotNet object ref ID from attachWebRendererInterop
				// args = [browserRendererId, {"__dotNetObject": N}, {}, {}]
				if (
					jsFnName === 'Blazor._internal.attachWebRendererInterop' &&
					jsArgsRaw &&
					typeof jsArgsRaw === 'string'
				) {
					try {
						const parsed = JSON.parse(jsArgsRaw);
						if (Array.isArray(parsed)) {
							// First element is the browserRendererId
							if (typeof parsed[0] === 'number') {
								this._browserRendererId = parsed[0];
							}
							// Find the DotNetObjectRef
							for (const item of parsed) {
								if (item && typeof item === 'object' && typeof item['__dotNetObject'] === 'number') {
									this._rendererInteropId = item['__dotNetObject'];
									this._log.info(
										`[Wallbox] Captured renderer interop ID: ${this._rendererInteropId}, browserRendererId: ${this._browserRendererId}`,
									);
									break;
								}
							}
						}
					} catch {
						/* ignore parse errors */
					}
				}
				if (taskId != null) {
					await this._sendEndInvokeJs(taskId);
				}
			} else if (target === 'JS.AttachComponent') {
				// .NET 8 Blazor asks us to attach a component to the DOM — acknowledge it
				this._log.info(`[Wallbox] JS.AttachComponent args=${JSON.stringify(args)}`);
				// No response needed (fire-and-forget server→client invocation)
			} else if (target === 'JS.EndInvokeDotNet') {
				// Response to our BeginInvokeDotNetFromJS (button click)
				const callId = parseInt(args[0]);
				const success = args[1];
				const result = args[2];
				this._log.info(
					`[Wallbox] JS.EndInvokeDotNet callId=${callId} success=${success} result=${JSON.stringify(result)} pendingId=${this._pendingClickCallId}`,
				);

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
		this._log.debug(`[Wallbox] RenderBatch size=${data.length} bytes`);

		// After the initial connection is established, incoming render batches are
		// periodic state updates — skip the expensive handler scan and avoid log spam.
		if (this._connected) {
			return;
		}

		const handlers = findOnclickHandlers(data, this._log);
		this._log.debug(`[Wallbox] findOnclickHandlers found ${handlers.length} handler(s): [${handlers.join(',')}]`);

		if (handlers.length > 0) {
			// Accumulate unique handlers across all batches.
			for (const h of handlers) {
				if (!this._allHandlers.includes(h)) {
					this._allHandlers.push(h);
				}
			}
			const take = Math.min(this._allHandlers.length, BUTTON_ORDER.length);
			const wallboxHandlers = this._allHandlers.slice(-take);
			this._buttonHandlers = Object.fromEntries(
				BUTTON_ORDER.slice(0, take).map((name, i) => [name, wallboxHandlers[i]]),
			);
			this._log.debug(`[Wallbox] Accumulated handlers: ${JSON.stringify(this._buttonHandlers)}`);
		}

		// One-time hex dump of ALL reference-frame sections for the first large batch.
		// We dump every byte so we can manually verify the frame layout.
		if (!this._framesDumped && data.length >= 500) {
			this._framesDumped = true;
			const footerOff = data.length - 20;
			for (let fi = 0; fi < 5; fi++) {
				const refOff = data.readUInt32LE(footerOff + fi * 4);
				if (refOff === 0 || refOff >= data.length - 4) {
					continue;
				}
				const fc = data.readUInt32LE(refOff);
				if (fc === 0 || fc > 10000) {
					continue;
				}
				// Dump the ENTIRE section so no frame is hidden
				const sectionEnd = Math.min(data.length, refOff + 4 + fc * 28 + 64);
				this._log.info(
					`[Wallbox] FRAMES-DUMP fi=${fi} refOff=${refOff} fc=${fc}: ${data.slice(refOff, sectionEnd).toString('hex')}`,
				);
			}
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
