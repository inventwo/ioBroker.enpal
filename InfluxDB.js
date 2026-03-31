// =============================================================
// ioBroker Script: InfluxDB → 0_userdata.0 Datenpunkte
// Adapter: javascript
// InfluxDB Version: 2.x
// =============================================================

// ── Konfiguration ─────────────────────────────────────────────
const INFLUX_URL    = 'http://192.168.130.160:8086';                                                              // InfluxDB Host
const INFLUX_TOKEN  = 'B4_8vqiTpgZEkUsKpKwigyxz6Wuka4iG1NoqpdZYc2g_6FMEVULqJxruz3zQUHGMsfgjWFtUuvvwIyRyA4IHZA=='; // InfluxDB API-Token
const INFLUX_ORG    = '685f99d360e967f5';                                                                         // Org-ID
const INFLUX_BUCKET = 'solar';                                                                                    // Bucket-Name

// Flux-Abfrage: letzten Wert der letzten 24 Stunden, alle Measurements
const FLUX_QUERY = `
from(bucket: "${INFLUX_BUCKET}")
  |> range(start: -24h)
  |> last()
`;

// Prefix für alle anzulegenden Datenpunkte
const DP_PREFIX = '0_userdata.0.ENPAL';

// Abfrage-Intervall in Millisekunden (hier: 60 Sekunden)
const INTERVAL_MS = 60 * 1000;
// ── Ende Konfiguration ────────────────────────────────────────

/**
 * Führt eine Flux-Abfrage gegen InfluxDB 2.x aus und gibt
 * ein Array von Zeilen-Objekten { measurement, field, value, unit } zurück.
 */
async function queryInflux(fluxQuery) {
    return new Promise((resolve, reject) => {
        const body = fluxQuery.trim();
        const lib  = require('http');

        const options = {
            hostname: '192.168.130.160',
            port:     8086,
            path:     '/api/v2/query?org=enpal',
            method:   'POST',
            headers: {
                'Authorization': `Token ${INFLUX_TOKEN}`,
                'Content-Type':  'application/vnd.flux',
                'Accept':        'application/csv',
            },
        };

        const req = lib.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    return reject(new Error(`InfluxDB HTTP ${res.statusCode}: ${data}`));
                }
                log('InfluxDB RAW (erste 500 Zeichen): ' + data.substring(0, 500), 'debug');
                resolve(parseCsv(data));
            });
        });

        req.on('error', reject);
        req.end(body);
    });
}

/**
 * Parst das Annotated-CSV-Format von InfluxDB 2.x.
 * Gibt ein Array von { measurement, field, value, unit } zurück.
 */
function parseCsv(csv) {
    const lines   = csv.split('\n').filter(l => l.trim() !== '' && !l.startsWith('#'));
    if (lines.length < 2) return [];

    const headers = lines[0].split(',');
    const results = [];

    for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split(',');
        if (cols.length < headers.length) continue;

        const row = {};
        headers.forEach((h, idx) => { row[h.trim()] = (cols[idx] || '').trim(); });

        // Annotated CSV hat führende Leerzeile + result/table Spalten
        if (!row['_field'] || row['_value'] === undefined) continue;

        results.push({
            measurement: row['_measurement'] || 'unknown',
            field:       row['_field'],
            value:       isNaN(Number(row['_value'])) ? row['_value'] : Number(row['_value']),
            unit:        row['_unit'] || row['unit'] || '',
            tag_device:  row['device'] || row['name'] || '',
        });
    }

    return results;
}

/**
 * Stellt sicher, dass alle Parent-Channel-Objekte für eine ID existieren.
 */
async function ensureParentChannels(id) {
    const parts = id.split('.');
    // 0_userdata.0 ist der Root, ab Index 2 prüfen wir Channels
    for (let i = 3; i < parts.length; i++) {
        const channelId = parts.slice(0, i).join('.');
        await new Promise((resolve) => {
            getObject(channelId, (err, obj) => {
                if (err || !obj) {
                    setObject(channelId, {
                        type:   'channel',
                        common: { name: parts[i - 1] },
                        native: {},
                    }, resolve);
                } else {
                    resolve();
                }
            });
        });
    }
}

/**
 * Legt ioBroker-Datenpunkt an (falls nicht vorhanden)
 * und setzt den aktuellen Wert.
 */
async function createOrUpdateState(id, value, unit) {
    await ensureParentChannels(id);
    return new Promise((resolve) => {
        getObject(id, (err, obj) => {
            const type   = typeof value === 'number' ? 'number' : 'string';
            const common = {
                name:  id.split('.').pop(),
                type,
                role:  type === 'number' ? 'value' : 'text',
                unit:  unit || '',
                read:  true,
                write: false,
            };

            if (err || !obj) {
                // Datenpunkt existiert noch nicht → anlegen
                setObject(id, { type: 'state', common, native: {} }, () => {
                    setState(id, { val: value, ack: true }, resolve);
                });
            } else {
                // Datenpunkt existiert bereits → nur Wert aktualisieren
                setState(id, { val: value, ack: true }, resolve);
            }
        });
    });
}

/**
 * Hauptfunktion: InfluxDB abfragen und Datenpunkte aktualisieren.
 */
async function syncInfluxToIoBroker() {
    log('InfluxDB-Sync gestartet...', 'debug');

    let rows;
    try {
        rows = await queryInflux(FLUX_QUERY);
    } catch (e) {
        log(`InfluxDB-Abfrage fehlgeschlagen: ${e.message}`, 'error');
        return;
    }

    if (!rows.length) {
        log('InfluxDB: Keine Datensätze zurückgegeben.', 'warn');
        return;
    }

    for (const row of rows) {
        // Punkte in Teilnamen durch _ ersetzen um tiefe Hierarchien zu vermeiden
        const sanitize = (s) => s.replace(/[^a-zA-Z0-9_-]/g, '_');

        const nameParts = [DP_PREFIX, sanitize(row.measurement)];
        if (row.tag_device) nameParts.push(sanitize(row.tag_device));
        nameParts.push(sanitize(row.field));

        const dpId = nameParts.join('.');

        await createOrUpdateState(dpId, row.value, row.unit);
        log(`Aktualisiert: ${dpId} = ${row.value} ${row.unit}`, 'debug');
    }

    log(`InfluxDB-Sync abgeschlossen. ${rows.length} Datenpunkte aktualisiert.`, 'info');
}

// ── Scheduler ─────────────────────────────────────────────────
// Einmalig beim Start ausführen
syncInfluxToIoBroker();

// Dann regelmäßig wiederholen
const timer = setInterval(syncInfluxToIoBroker, INTERVAL_MS);

// Aufräumen wenn Skript gestoppt wird
onStop(() => {
    clearInterval(timer);
    log('InfluxDB-Sync gestoppt.', 'info');
}, 1000);
