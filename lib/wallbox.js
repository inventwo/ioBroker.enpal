'use strict';

const fs = require('node:fs');
const puppeteer = require('puppeteer-core');

// ─── Button configuration ─────────────────────────────────────────────────────

/**
 * Text labels of the buttons on the Enpal Box /wallbox page.
 * Matched case-insensitively against the visible button text.
 */
const BUTTON_LABELS = {
	start: 'Start Charging',
	stop: 'Stop Charging',
	eco: 'Set Eco',
	full: 'Set Full',
	solar: 'Set Solar',
	smart: 'Set Smart',
};

/** Milliseconds to keep the browser alive after the last command. */
const BROWSER_IDLE_MS = 300_000; // 5 minutes

/** Maximum time in ms to wait for a button to become clickable. */
const BUTTON_TIMEOUT_MS = 15_000;

// ─── Browser executable detection ────────────────────────────────────────────

/** Known paths where Chrome / Chromium / Edge may be installed. */
const BROWSER_CANDIDATES = [
	// Windows — Chrome
	'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
	'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
	// Windows — Edge
	'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
	'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
	// Linux — Chromium
	'/usr/bin/chromium-browser',
	'/usr/bin/chromium',
	// Linux — Chrome
	'/usr/bin/google-chrome',
	'/usr/bin/google-chrome-stable',
	// macOS — Chrome
	'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
];

/**
 * Returns the path to the first browser executable found on this system,
 * or `null` if none is available.
 *
 * @returns {string|null} Absolute path to the browser executable, or null if not found.
 */
function detectBrowserPath() {
	for (const p of BROWSER_CANDIDATES) {
		if (fs.existsSync(p)) {
			return p;
		}
	}
	return null;
}

// ─── WallboxBrowserClient ─────────────────────────────────────────────────────

/**
 * Controls the Enpal Box wallbox by driving a headless browser, exactly as the
 * reference Python / Selenium implementation does.  Each action opens the
 * /wallbox page, waits for Blazor to finish rendering, then clicks the
 * appropriate button by its visible text label.
 *
 * A single browser instance is shared across commands and automatically shut
 * down after {@link BROWSER_IDLE_MS} ms of inactivity.
 */
class WallboxBrowserClient {
	/**
	 * @param {string} baseUrl  e.g. "http://192.168.130.160"
	 * @param {{ info: Function, warn: Function, error: Function, debug: Function }} log ioBroker logger instance.
	 */
	constructor(baseUrl, log) {
		this._baseUrl = baseUrl.replace(/\/+$/, '');
		this._log = log;

		this._browser = null;
		this._idleTimer = null;
	}

	// ─── Public API ─────────────────────────────────────────────────────────

	/** Start charging. */
	async start() {
		await this._clickButton('start');
	}

	/** Stop charging. */
	async stop() {
		await this._clickButton('stop');
	}

	/**
	 * Set the charging mode.
	 *
	 * @param {'eco'|'full'|'solar'|'smart'} mode Desired charging mode.
	 */
	async setMode(mode) {
		const key = String(mode).toLowerCase();
		if (!BUTTON_LABELS[key]) {
			throw new Error(`Unknown wallbox mode: "${mode}". Valid: ${Object.keys(BUTTON_LABELS).join(', ')}`);
		}
		await this._clickButton(key);
	}

	/** Cleanly shut down the browser if it is open. */
	close() {
		this._cancelIdleTimer();
		if (this._browser) {
			this._browser.close().catch(e => this._log.debug(`[Wallbox] browser.close error: ${e.message}`));
			this._browser = null;
		}
	}

	// ─── Internal ───────────────────────────────────────────────────────────

	/**
	 * Clicks the wallbox button identified by `key` (one of the keys in
	 * {@link BUTTON_LABELS}).
	 *
	 * @param {string} key Button key from BUTTON_LABELS.
	 */
	async _clickButton(key) {
		const label = BUTTON_LABELS[key];
		this._log.info(`[Wallbox] Clicking button "${label}"…`);

		const page = await this._getPage();

		try {
			// XPath mirrors the Python / Selenium implementation:
			// find a <span> whose trimmed text matches the label (case-insensitive),
			// then navigate up to the parent <button> element.
			const upper = label.toUpperCase();
			const xpath =
				`//span[translate(normalize-space(text()),` +
				`'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ')='${upper}']/..`;

			this._log.debug(`[Wallbox] XPath: ${xpath}`);

			// Wait until clickable, then click
			await page.waitForSelector(`::-p-xpath(${xpath})`, {
				timeout: BUTTON_TIMEOUT_MS,
				visible: true,
			});
			const [el] = await page.$$(`::-p-xpath(${xpath})`);
			if (!el) {
				throw new Error(`Button "${label}" not found on page`);
			}
			await el.click();
			this._log.info(`[Wallbox] Button "${label}" clicked successfully`);
		} catch (err) {
			// On error close the browser so the next call gets a fresh instance
			this.close();
			throw new Error(`[Wallbox] Failed to click "${label}": ${err.message}`);
		}

		this._scheduleIdleClose();
	}

	/**
	 * Returns an open page at /wallbox, reusing the existing browser if
	 * possible, otherwise launching a new one.
	 *
	 * @returns {Promise<import('puppeteer-core').Page>} The loaded wallbox page.
	 */
	async _getPage() {
		if (!this._browser || !this._browser.connected) {
			this._browser = await this._launchBrowser();
		}

		// Reuse first existing page or open a new one
		const pages = await this._browser.pages();
		const page = pages.length > 0 ? pages[0] : await this._browser.newPage();

		// Suppress unhandled JS errors from the Blazor app itself
		page.on('pageerror', () => {});

		const url = `${this._baseUrl}/wallbox`;
		this._log.info(`[Wallbox] Loading ${url} …`);
		await page.goto(url, { waitUntil: 'networkidle2', timeout: 30_000 });
		this._log.debug('[Wallbox] Page loaded');

		return page;
	}

	/**
	 * Launches a new headless Chromium / Chrome / Edge browser.
	 *
	 * @returns {Promise<import('puppeteer-core').Browser>} The launched browser instance.
	 */
	async _launchBrowser() {
		const executablePath = detectBrowserPath();
		if (!executablePath) {
			throw new Error(
				'[Wallbox] No Chrome/Chromium/Edge browser found. ' +
					'Please install Chromium (e.g. "sudo apt install chromium-browser") ' +
					'or Google Chrome.',
			);
		}
		this._log.info(`[Wallbox] Launching headless browser: ${executablePath}`);
		const browser = await puppeteer.launch({
			executablePath,
			headless: true,
			args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
		});
		this._log.debug('[Wallbox] Browser launched');
		return browser;
	}

	_scheduleIdleClose() {
		this._cancelIdleTimer();
		this._idleTimer = setTimeout(() => {
			this._log.debug('[Wallbox] Closing idle browser');
			this.close();
		}, BROWSER_IDLE_MS);
	}

	_cancelIdleTimer() {
		if (this._idleTimer) {
			clearTimeout(this._idleTimer);
			this._idleTimer = null;
		}
	}
}

module.exports = { WallboxBrowserClient };
