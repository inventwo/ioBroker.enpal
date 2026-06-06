'use strict';

const utils = require('@iobroker/adapter-core');
const http = require('node:http');
const https = require('node:https');
const { URL } = require('node:url');
const { WallboxBlazorClient } = require('./lib/wallbox');

const WALLBOX_CHANNEL = 'wallbox_control';
const VALID_MODES = ['eco', 'solar', 'full', 'smart'];

class Enpal extends utils.Adapter {
	constructor(options) {
		super({
			...options,
			name: 'enpal',
		});
		this.on('ready', this.onReady.bind(this));
		this.on('stateChange', this.onStateChange.bind(this));
		this.on('unload', this.onUnload.bind(this));
		this.syncInterval = null;
		this.wallboxClient = null;
	}

	async onReady() {
		const influxUrl = this.config.influx_url || 'http://localhost:8086';
		const influxToken = this.config.influx_token || '';
		const influxOrg = this.config.influx_org || '';
		const influxBucket = this.config.influx_bucket || '';
		const intervalS = this.config.interval_s || 60;

		await this.setState('info.connection', false, true);

		if (!influxToken || !influxOrg || !influxBucket) {
			this.log.error('InfluxDB configuration incomplete. Please configure URL, token, org ID and bucket.');
			return;
		}

		if (this.config.wallbox_enabled) {
			await this._initWallbox();
		}

		const fluxQuery = `from(bucket: "${influxBucket}")\n  |> range(start: -24h)\n  |> last()`;

		const sync = async () => {
			await this.syncInfluxToIoBroker(influxUrl, influxToken, influxOrg, fluxQuery);
			if (this.wallboxClient) {
				await this._syncWallboxStatus();
			}
		};

		await sync();
		this.syncInterval = this.setInterval(sync, intervalS * 1000);
	}

	async _initWallbox() {
		const enpalUrl = (this.config.enpal_url || '').trim();
		if (!enpalUrl) {
			this.log.warn('Wallbox control enabled but no Enpal Box URL configured. Skipping.');
			return;
		}

		this.log.info(`Wallbox control enabled. Enpal Box URL: ${enpalUrl}`);
		this.wallboxClient = new WallboxBlazorClient(enpalUrl, this.log);

		await this._createWallboxStates();
		this.subscribeStates(`${WALLBOX_CHANNEL}.*`);
	}

	async _createWallboxStates() {
		await this.setObjectNotExistsAsync(WALLBOX_CHANNEL, {
			type: 'channel',
			common: { name: 'Wallbox Control' },
			native: {},
		});

		const statesDef = [
			{
				id: 'start',
				common: {
					name: 'Start charging',
					type: 'boolean',
					role: 'button',
					read: false,
					write: true,
					def: false,
				},
			},
			{
				id: 'stop',
				common: {
					name: 'Stop charging',
					type: 'boolean',
					role: 'button',
					read: false,
					write: true,
					def: false,
				},
			},
			{
				id: 'mode',
				common: {
					name: 'Set charging mode',
					type: 'string',
					role: 'value',
					read: true,
					write: true,
					def: '',
					states: { eco: 'Eco', solar: 'Solar', full: 'Full', smart: 'Smart' },
				},
			},
			{
				id: 'currentMode',
				common: {
					name: 'Current charging mode',
					type: 'string',
					role: 'text',
					read: true,
					write: false,
					def: '',
				},
			},
			{
				id: 'connectorStatus',
				common: {
					name: 'Connector status',
					type: 'string',
					role: 'text',
					read: true,
					write: false,
					def: '',
				},
			},
		];

		for (const { id, common } of statesDef) {
			await this.setObjectNotExistsAsync(`${WALLBOX_CHANNEL}.${id}`, {
				type: 'state',
				common,
				native: {},
			});
		}
	}

	async _syncWallboxStatus() {
		try {
			const { mode, status } = await this.wallboxClient.fetchStatus();
			if (mode) {
				await this.setStateAsync(`${WALLBOX_CHANNEL}.currentMode`, { val: mode, ack: true });
			}
			if (status) {
				await this.setStateAsync(`${WALLBOX_CHANNEL}.connectorStatus`, { val: status, ack: true });
			}
		} catch (err) {
			this.log.debug(`Wallbox status sync failed: ${err.message}`);
		}
	}

	async onStateChange(id, state) {
		if (!state || state.ack) {
			return;
		}
		if (!this.wallboxClient) {
			return;
		}

		const localId = id.replace(`${this.namespace}.`, '');
		if (!localId.startsWith(`${WALLBOX_CHANNEL}.`)) {
			return;
		}

		const stateKey = localId.slice(WALLBOX_CHANNEL.length + 1);

		try {
			if (stateKey === 'start' && state.val === true) {
				this.log.info('Wallbox: starting charge');
				await this.wallboxClient.start();
				await this.setState(`${WALLBOX_CHANNEL}.start`, { val: false, ack: true });
				await this._syncWallboxStatus();
			} else if (stateKey === 'stop' && state.val === true) {
				this.log.info('Wallbox: stopping charge');
				await this.wallboxClient.stop();
				await this.setState(`${WALLBOX_CHANNEL}.stop`, { val: false, ack: true });
				await this._syncWallboxStatus();
			} else if (stateKey === 'mode') {
				const mode = String(state.val).toLowerCase();
				if (!VALID_MODES.includes(mode)) {
					this.log.warn(`Wallbox: invalid mode "${state.val}". Allowed: ${VALID_MODES.join(', ')}`);
					return;
				}
				this.log.info(`Wallbox: setting mode to ${mode}`);
				await this.wallboxClient.setMode(mode);
				await this.setState(`${WALLBOX_CHANNEL}.mode`, { val: mode, ack: true });
				await this._syncWallboxStatus();
			}
		} catch (err) {
			this.log.error(`Wallbox control failed: ${err.message}`);
		}
	}

	onUnload(callback) {
		try {
			if (this.syncInterval) {
				this.clearInterval(this.syncInterval);
				this.syncInterval = null;
			}
			if (this.wallboxClient) {
				this.wallboxClient.close();
				this.wallboxClient = null;
			}
			callback();
		} catch (error) {
			this.log.error(`Error during unload: ${error.message}`);
			callback();
		}
	}

	queryInflux(influxUrl, influxToken, influxOrg, fluxQuery) {
		return new Promise((resolve, reject) => {
			let parsed;
			try {
				parsed = new URL(influxUrl);
			} catch {
				return reject(new Error(`Invalid InfluxDB URL: ${influxUrl}`));
			}

			const lib = parsed.protocol === 'https:' ? https : http;
			const options = {
				hostname: parsed.hostname,
				port: parseInt(parsed.port) || (parsed.protocol === 'https:' ? 443 : 80),
				path: `/api/v2/query?org=${encodeURIComponent(influxOrg)}`,
				method: 'POST',
				headers: {
					Authorization: `Token ${influxToken}`,
					'Content-Type': 'application/vnd.flux',
					Accept: 'application/csv',
				},
			};

			const req = lib.request(options, res => {
				let data = '';
				res.on('data', chunk => {
					data += chunk;
				});
				res.on('end', () => {
					if (res.statusCode !== 200) {
						return reject(new Error(`InfluxDB HTTP error ${res.statusCode}: ${data}`));
					}
					this.log.debug(`InfluxDB RAW (first 500 chars): ${data.substring(0, 500)}`);
					resolve(this.parseCsv(data));
				});
			});

			req.on('error', reject);
			req.end(fluxQuery.trim());
		});
	}

	parseCsv(csv) {
		const lines = csv.split('\n').filter(l => l.trim() !== '' && !l.startsWith('#'));
		if (lines.length < 2) {
			return [];
		}

		const headers = lines[0].split(',');
		const results = [];

		for (let i = 1; i < lines.length; i++) {
			const cols = lines[i].split(',');
			if (cols.length < headers.length) {
				continue;
			}

			const row = {};
			headers.forEach((h, idx) => {
				row[h.trim()] = (cols[idx] || '').trim();
			});

			if (!row['_field'] || row['_value'] === undefined) {
				continue;
			}

			results.push({
				measurement: row['_measurement'] || 'unknown',
				field: row['_field'],
				value: isNaN(Number(row['_value'])) ? row['_value'] : Number(row['_value']),
				unit: row['_unit'] || row['unit'] || '',
				tag_device: row['device'] || row['name'] || '',
			});
		}

		return results;
	}

	async ensureParentChannels(id) {
		const parts = id.split('.');
		for (let i = 1; i < parts.length; i++) {
			const channelId = parts.slice(0, i).join('.');
			const obj = await this.getObjectAsync(channelId);
			if (!obj) {
				await this.setObjectNotExistsAsync(channelId, {
					type: 'channel',
					common: { name: parts[i - 1] },
					native: {},
				});
			}
		}
	}

	async createOrUpdateState(id, value, unit) {
		await this.ensureParentChannels(id);
		const type = typeof value === 'number' ? 'number' : 'string';

		// Normalize unit: convert "Percent" to "%", hide "None"
		let displayUnit = '';
		if (unit && unit !== 'None') {
			displayUnit = unit === 'Percent' ? '%' : unit;
		}

		await this.setObjectNotExistsAsync(id, {
			type: 'state',
			common: {
				name: id.split('.').pop(),
				type,
				role: type === 'number' ? 'value' : 'text',
				unit: displayUnit,
				read: true,
				write: false,
			},
			native: {},
		});
		await this.setStateAsync(id, { val: value, ack: true });
	}

	async syncInfluxToIoBroker(influxUrl, influxToken, influxOrg, fluxQuery) {
		this.log.debug('InfluxDB sync started...');

		let rows;
		try {
			rows = await this.queryInflux(influxUrl, influxToken, influxOrg, fluxQuery);
		} catch (e) {
			this.log.error(`InfluxDB query failed: ${e.message}`);
			await this.setState('info.connection', false, true);
			return;
		}

		if (!rows.length) {
			this.log.warn('InfluxDB: No records returned.');
			await this.setState('info.connection', false, true);
			return;
		}

		await this.setState('info.connection', true, true);

		for (const row of rows) {
			const sanitize = s => s.replace(/[^a-zA-Z0-9_-]/g, '_');
			const nameParts = [sanitize(row.measurement)];
			if (row.tag_device) {
				nameParts.push(sanitize(row.tag_device));
			}
			nameParts.push(sanitize(row.field));

			const dpId = nameParts.join('.');
			await this.createOrUpdateState(dpId, row.value, row.unit);
			this.log.debug(`Updated: ${dpId} = ${row.value} ${row.unit}`);
		}

		if (this.config.show_sync_info) {
			this.log.info(`InfluxDB sync completed. ${rows.length} data points updated.`);
		}
	}
}

if (require.main !== module) {
	module.exports = options => new Enpal(options);
} else {
	new Enpal();
}
