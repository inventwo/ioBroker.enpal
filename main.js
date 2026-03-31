'use strict';

const utils = require('@iobroker/adapter-core');
const http = require('http');
const https = require('https');
const { URL } = require('url');

class Enpal extends utils.Adapter {
	constructor(options) {
		super({
			...options,
			name: 'enpal',
		});
		this.on('ready', this.onReady.bind(this));
		this.on('unload', this.onUnload.bind(this));
		this.syncInterval = null;
	}

	async onReady() {
		const influxUrl = this.config.influx_url || 'http://localhost:8086';
		const influxToken = this.config.influx_token || '';
		const influxOrg = this.config.influx_org || '';
		const influxBucket = this.config.influx_bucket || '';
		const intervalMs = this.config.interval_ms || 60000;

		await this.setState('info.connection', false, true);

		if (!influxToken || !influxOrg || !influxBucket) {
			this.log.error('InfluxDB configuration incomplete. Please configure URL, token, org ID and bucket.');
			return;
		}

		const fluxQuery = `from(bucket: "${influxBucket}")\n  |> range(start: -24h)\n  |> last()`;

		const sync = async () => {
			await this.syncInfluxToIoBroker(influxUrl, influxToken, influxOrg, fluxQuery);
		};

		await sync();
		this.syncInterval = this.setInterval(sync, intervalMs);
	}

	onUnload(callback) {
		try {
			if (this.syncInterval) {
				this.clearInterval(this.syncInterval);
				this.syncInterval = null;
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
		await this.setObjectNotExistsAsync(id, {
			type: 'state',
			common: {
				name: id.split('.').pop(),
				type,
				role: type === 'number' ? 'value' : 'text',
				unit: unit || '',
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

		this.log.info(`InfluxDB sync completed. ${rows.length} data points updated.`);
	}
}

if (require.main !== module) {
	module.exports = options => new Enpal(options);
} else {
	new Enpal();
}
