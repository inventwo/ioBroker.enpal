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
	const patterns = [
		/<script[^>]+id="blazor-application-state"[^>]*>([^<]*)<\/script>/i,
		/id="blazor-application-state"[^>]+data-value="([^"]+)"/i,
		/blazor-application-state"[^>]*>([A-Za-z0-9+/=_-]{20,})</i,
		/<script[^>]+type="application\/json"[^>]+id="blazor-application-state"[^>]*>([^<]*)<\/script>/i,
	];
	for (const pattern of patterns) {
		const m = html.match(pattern);
		if (m && m[1].trim()) {
			return m[1].trim();
		}
	}
	return '';
}

// ─── Blazor RenderBatch binary parser ────────────────────────────────────────
// Footer (last 20 bytes): 5 × uint32 LE section offsets.
// The exact order and frame size varies by .NET / Blazor version, so we
// try all plausible combinations automatically.

const BLAZOR_FRAME_TYPE_ATTRIBUTE = 3;

function findOnclickHandlers(data, log) {
	// Matches derolli1976/enpal wallbox_client.py — footer[1]=refFrames, footer[2]=dispComp,
	// 20-byte attribute frames, uint64 event handler ID at offset 12.
	if (data.length < 24) {
		if (log) {
			log.debug(`[Wallbox] findOnclickHandlers: data too short (${data.length} bytes)`);
		}
		return [];
	}

	const footerOff = data.length - 20;
	const refFramesOff = data.readUInt32LE(footerOff + 4);
	const dispCompOff = data.readUInt32LE(footerOff + 8);

	if (refFramesOff === 0 || refFramesOff >= data.length || dispCompOff === 0 || dispCompOff >= data.length) {
		if (log) {
			log.debug(
				`[Wallbox] findOnclickHandlers: invalid footer refFrames=${refFramesOff} dispComp=${dispCompOff}`,
			);
		}
		return [];
	}

	const frameCount = data.readUInt32LE(refFramesOff);
	if (frameCount === 0 || frameCount > 10000) {
		return [];
	}

	const frameSize = 20;
	const handlers = [];
	let pos = refFramesOff + 4;
	for (let i = 0; i < frameCount; i++) {
		if (pos + frameSize > dispCompOff) {
			break;
		}
		if (data.readInt32LE(pos) === BLAZOR_FRAME_TYPE_ATTRIBUTE) {
			const eventId = Number(data.readBigUInt64LE(pos + 12));
			if (eventId > 0 && eventId < 100000) {
				handlers.push(eventId);
			}
		}
		pos += frameSize;
	}

	if (log) {
		log.debug(
			`[Wallbox] Handler scan: refFrames=${refFramesOff} dispComp=${dispCompOff} ` +
				`frames=${frameCount} → ${handlers.length} handler(s): [${handlers.join(',')}]`,
		);
	}
	return handlers;
}

// ─── HTML status parser ───────────────────────────────────────────────────────

// "Fast" is the Enpal internal name for Full/Schnellmodus
const MODE_NORMALIZE = { fast: 'Full' };

function extractWordAfter(text, prefix) {
	let idx = 0;
	const validModes = new Set(['Eco', 'Solar', 'Full', 'Smart', 'Fast']);
	while (true) {
		idx = text.indexOf(prefix, idx);
		if (idx < 0) {
			return null;
		}
		const after = text.slice(idx + prefix.length, idx + prefix.length + 25);
		let word = '';
		for (const c of after) {
			if (/[a-z]/i.test(c)) {
				word += c;
			} else if (word) {
				break;
			}
		}
		if (prefix === 'Mode ' && validModes.has(word)) {
			const raw = word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
			return MODE_NORMALIZE[raw.toLowerCase()] || raw;
		}
		if (prefix === 'Status ' && word.length > 2) {
			return normalizeConnectorStatus(word);
		}
		idx += prefix.length;
	}
}

function normalizeWallboxMode(raw) {
	if (!raw) {
		return null;
	}
	const capitalized = raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
	return MODE_NORMALIZE[capitalized.toLowerCase()] || capitalized;
}

function parseDeviceMessagesField(html, fieldName) {
	const escaped = fieldName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	const re = new RegExp(`<td[^>]*>\\s*${escaped}\\s*</td>\\s*<td[^>]*>\\s*([^<]+?)\\s*</td>`, 'i');
	const m = html.match(re);
	return m ? m[1].trim() : null;
}

function normalizeAutomaticChargeStatus(raw) {
	if (!raw) {
		return null;
	}
	const lower = raw.trim().toLowerCase();
	if (lower === 'on' || lower === 'off') {
		return lower.charAt(0).toUpperCase() + lower.slice(1);
	}
	return raw.trim();
}

/** OCPP connector statuses (case-insensitive lookup key → canonical value). */
const CONNECTOR_STATUS_MAP = {
	available: 'Available',
	preparing: 'Preparing',
	charging: 'Charging',
	suspendedev: 'SuspendedEV',
	suspendedevse: 'SuspendedEVSE',
	finishing: 'Finishing',
	reserved: 'Reserved',
	unavailable: 'Unavailable',
	faulted: 'Faulted',
	connected: 'Connected',
};

function normalizeConnectorStatus(raw) {
	if (!raw) {
		return null;
	}
	const trimmed = String(raw).trim();
	const key = trimmed.replace(/[^a-zA-Z]/g, '').toLowerCase();
	if (CONNECTOR_STATUS_MAP[key]) {
		return CONNECTOR_STATUS_MAP[key];
	}
	if (/^[A-Z][a-z]+([A-Z][A-Za-z]*)+$/.test(trimmed)) {
		return trimmed;
	}
	if (/^[a-zA-Z]+$/.test(trimmed)) {
		return trimmed.charAt(0).toUpperCase() + trimmed.slice(1).toLowerCase();
	}
	return trimmed;
}

function extractStatusFromHtml(html) {
	const mode = extractWordAfter(html, 'Mode ');
	let status = extractWordAfter(html, 'Status ');
	if (!status) {
		const connectorMatch = html.match(/Connector\s+(\w+)/i);
		if (connectorMatch) {
			status = connectorMatch[1];
		}
	}
	return { mode, status };
}

// ─── WallboxBlazorClient ──────────────────────────────────────────────────────

const BUTTON_ORDER = ['start', 'stop', 'eco', 'full', 'solar', 'smart'];
const PING_INTERVAL_MS = 15000;
const MAX_CONNECTION_AGE_MS = 300000;
const HTTP_TIMEOUT_MS = 10000;

class WallboxBlazorClient {
	constructor(baseUrl, log, adapter) {
		this._baseUrl = baseUrl.replace(/\/$/, '');
		this._log = log;
		this._adapter = adapter;
		this._ws = null;
		this._connected = false;
		this._buttonHandlers = {};
		this._mode = null;
		this._status = null;
		this._automaticChargeStatus = null;
		this._components = [];
		this._appState = '';
		this._dotnetCallCounter = 0;
		this._rendererInteropId = 1;
		this._postClickMonitorUntil = 0;
		this._pendingClickCallId = null;
		this._clickResolve = null;
		this._clickReject = null;
		this._clickTimeoutHandle = null;
		this._connectedAt = 0;
		this._pingTimer = null;
		this._startCircuitResolve = null;
		this._startCircuitReject = null;
		this._htmlSnippetLogged = false;
		this._htmlStatusUnavailable = false;
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

	get automaticChargeStatus() {
		return this._automaticChargeStatus;
	}

	async connect() {
		this._cleanup();
		this._buttonHandlers = {};
		this._mode = null;
		this._status = null;
		this._automaticChargeStatus = null;
		this._dotnetCallCounter = 0;
		this._rendererInteropId = 1;
		this._browserRendererId = 0;
		this._htmlSnippetLogged = false;

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
				const t = this._adapter.setTimeout(() => reject(new Error('WebSocket open timeout')), HTTP_TIMEOUT_MS);
				this._ws.addEventListener('open', () => {
					this._adapter.clearTimeout(t);
					resolve();
				});
				this._ws.addEventListener('error', e => {
					this._adapter.clearTimeout(t);
					reject(new Error(`WebSocket connection error: ${e.message || e.type || e}`));
				});
			});
			this._log.info('[Wallbox] Step 3 OK: WebSocket open');

			// 4. Blazor handshake (text frame): send protocol negotiation, receive ACK
			await new Promise((resolve, reject) => {
				const t = this._adapter.setTimeout(() => reject(new Error('Blazor handshake timeout')), 5000);
				this._ws.addEventListener(
					'message',
					e => {
						this._adapter.clearTimeout(t);
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
					this._adapter.clearInterval(this._pingTimer);
					this._pingTimer = null;
				}
				this._log.debug('[Wallbox] WebSocket closed');
			});

			// 6. Start Blazor circuit for /wallbox — wait for completion before proceeding
			const circuitOk = await new Promise(resolve => {
				this._startCircuitResolve = resolve;
				this._startCircuitReject = null;
				const t = this._adapter.setTimeout(() => {
					this._startCircuitResolve = null;
					this._log.info('[Wallbox] StartCircuit: no completion within 3 s, proceeding anyway');
					resolve(true);
				}, 3000);
				this._startCircuitResolve = result => {
					this._adapter.clearTimeout(t);
					this._startCircuitResolve = null;
					resolve(result);
				};
				this._sendStartCircuit().catch(err => {
					this._adapter.clearTimeout(t);
					this._startCircuitResolve = null;
					this._log.warn(`[Wallbox] StartCircuit send error: ${err.message}`);
					resolve(false);
				});
			});
			this._log.info(`[Wallbox] Step 5 OK: StartCircuit result=${circuitOk}`);

			// 7. Activate pre-rendered components (required for full wallbox UI)
			await this._adapter.delay(300);
			await this._sendUpdateRootComponents();

			// 8. Wait up to 5 s for render batches with all 6 handlers and mode text
			for (let i = 0; i < 50; i++) {
				await this._adapter.delay(100);
				if (Object.keys(this._buttonHandlers).length >= BUTTON_ORDER.length && this._mode !== null) {
					break;
				}
			}
			const handlerCount = Object.keys(this._buttonHandlers).length;
			this._log.info(
				`[Wallbox] Step 6: received ${handlerCount} button handler(s): ${JSON.stringify(this._buttonHandlers)}`,
			);
			if (handlerCount === 0) {
				throw new Error('No wallbox button handlers received — page may be unsupported or circuit incomplete');
			}
			if (handlerCount < BUTTON_ORDER.length) {
				this._log.warn(
					`[Wallbox] Only ${handlerCount}/${BUTTON_ORDER.length} button handlers after 5 s — control may be incomplete`,
				);
			}
			if (this._mode === null) {
				this._log.warn('[Wallbox] Mode not found in render batches — status from WebSocket unavailable');
			}

			this._connected = true;
			this._connectedAt = Date.now();

			// 9. Keep-alive ping every 15 s
			this._pingTimer = this._adapter.setInterval(() => {
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
			this._adapter.clearInterval(this._pingTimer);
			this._pingTimer = null;
		}
		if (this._clickTimeoutHandle) {
			this._adapter.clearTimeout(this._clickTimeoutHandle);
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
		// Blazor .NET 8: DispatchEventAsync takes TWO arguments:
		//   1. event descriptor: { eventHandlerId, eventName, eventFieldInfo }
		//      NOTE: field name is "eventName" (not "eventArgsType"), value is DOM event name "click"
		//      NOTE: no "browserRendererId" in the descriptor
		//   2. event args: plain JS object (NOT a pre-serialized JSON string)
		// Source: github.com/dotnet/aspnetcore issue #46217 debug trace of real Blazor client.
		this._dotnetCallCounter += 1;
		const callId = this._dotnetCallCounter;
		this._pendingClickCallId = callId;

		const eventDescriptor = {
			eventHandlerId: handlerId,
			eventName: 'click',
			eventFieldInfo: null,
		};
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
		// eventArgs is a plain JSON object, NOT JSON.stringify'd
		const argsJson = JSON.stringify([eventDescriptor, mouseArgs]);
		const clickMsg = [
			1,
			{},
			null,
			'BeginInvokeDotNetFromJS',
			[String(callId), null, 'DispatchEventAsync', this._rendererInteropId, argsJson],
		];

		this._log.info(
			`[Wallbox] Sending DispatchEventAsync: button=${button} handlerId=${handlerId} callId=${callId} rendererInteropId=${this._rendererInteropId}`,
		);

		return new Promise((resolve, reject) => {
			const TIMEOUT_MS = 8000;
			this._clickResolve = resolve;
			this._clickReject = reject;
			this._clickTimeoutHandle = this._adapter.setTimeout(() => {
				this._pendingClickCallId = null;
				this._clickResolve = null;
				this._clickReject = null;
				reject(new Error(`[Wallbox] Click timeout for button "${button}" (no EndInvokeDotNet received)`));
			}, TIMEOUT_MS);

			this._sendMessage(clickMsg).catch(err => {
				this._adapter.clearTimeout(this._clickTimeoutHandle);
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
		// Blazor Server liefert auf manchen Boxen keinen vorgerenderten Status im HTML.
		// Nach dem ersten leeren Ergebnis HTTP-Polling überspringen (Werte kommen per WebSocket).
		if (!this._htmlStatusUnavailable) {
			try {
				const html = await this._httpGet('/wallbox');
				const { mode, status } = extractStatusFromHtml(html);
				if (mode) {
					this._mode = mode;
				}
				if (status) {
					this._status = normalizeConnectorStatus(status);
				}
				if (!mode && !status) {
					this._htmlStatusUnavailable = true;
					if (!this._htmlSnippetLogged) {
						this._htmlSnippetLogged = true;
						this._log.debug(
							`[Wallbox] HTTP /wallbox enthält keinen vorgerenderten Status — ` +
								`Werte werden bei Steueraktionen über WebSocket aktualisiert.`,
						);
					}
				}
			} catch (err) {
				this._log.debug(`[Wallbox] Status HTTP fetch failed: ${err.message}`);
			}
		}
		this._log.debug(`[Wallbox] Status: mode=${this._mode}, status=${this._status}`);
		return { mode: this._mode, status: this._status };
	}

	async fetchDeviceMessagesStatus() {
		try {
			const html = await this._httpGet('/deviceMessages');
			const modeRaw = parseDeviceMessagesField(html, 'Mode.Charge.Connector.1');
			const statusRaw =
				parseDeviceMessagesField(html, 'Status.Wallbox.Connector.1') ||
				parseDeviceMessagesField(html, 'Status.Connector.1');
			if (modeRaw) {
				this._mode = normalizeWallboxMode(modeRaw);
			}
			if (statusRaw) {
				this._status = normalizeConnectorStatus(statusRaw);
			}
			const autoChargeRaw = parseDeviceMessagesField(html, 'Wallbox.Settings.AutomaticChargeStatus.Connector.1');
			if (autoChargeRaw) {
				this._automaticChargeStatus = normalizeAutomaticChargeStatus(autoChargeRaw);
			}
			this._log.debug(
				`[Wallbox] deviceMessages: mode=${this._mode}, status=${this._status}, ` +
					`automaticChargeStatus=${this._automaticChargeStatus}`,
			);
			return {
				mode: this._mode,
				status: this._status,
				automaticChargeStatus: this._automaticChargeStatus,
			};
		} catch (err) {
			this._log.debug(`[Wallbox] deviceMessages fetch failed: ${err.message}`);
			return {
				mode: this._mode,
				status: this._status,
				automaticChargeStatus: this._automaticChargeStatus,
			};
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
		// HA reference: StartCircuit with empty component list; components are
		// activated separately via UpdateRootComponents.
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
					this._adapter.clearTimeout(this._clickTimeoutHandle);
					this._pendingClickCallId = null;
					const resolve = this._clickResolve;
					const reject = this._clickReject;
					this._clickResolve = null;
					this._clickReject = null;
					if (success) {
						// Monitor render batches for 3 s to see if the UI updates in response
						this._postClickMonitorUntil = Date.now() + 3000;
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

		// Extract mode/status from every batch (cheap text scan)
		const text = data.toString('utf8', 0, data.length);
		const { mode, status } = extractStatusFromHtml(text);
		if (mode) {
			this._mode = mode;
		}
		if (status) {
			this._status = normalizeConnectorStatus(status);
		}

		// After connect, skip expensive handler scan on periodic state updates.
		if (this._connected) {
			if (Date.now() < this._postClickMonitorUntil) {
				this._log.info(
					`[Wallbox] Post-click render batch: ${data.length} bytes, hex tail: ${data.slice(-40).toString('hex')}`,
				);
			}
			return;
		}

		const handlers = findOnclickHandlers(data, this._log);
		if (handlers.length >= BUTTON_ORDER.length) {
			const wallboxHandlers = handlers.slice(-BUTTON_ORDER.length);
			this._buttonHandlers = Object.fromEntries(BUTTON_ORDER.map((name, i) => [name, wallboxHandlers[i]]));
			this._log.info(`[Wallbox] Button handler mapping: ${JSON.stringify(this._buttonHandlers)}`);
		}
	}
}

module.exports = { WallboxBlazorClient, extractStatusFromHtml, normalizeConnectorStatus };
