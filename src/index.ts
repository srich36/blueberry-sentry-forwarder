// src/index.ts
// Datadog (array or single JSON log) -> Sentry Envelope (one request per log).
// Set SENTRY_DSN in env (wrangler.toml [vars] or as a secret).

interface Env {
	SENTRY_DSN: string; // e.g. https://<PUBLIC_KEY>@oXXXX.ingest.us.sentry.io/<PROJECT_ID>
}

type DDLog = {
	date?: string | number;
	timestamp?: string | number;
	service?: string;
	host?: string;
	source?: string;
	_id?: string;
	message?: unknown;
	status?: string;
	level?: string;
	environment?: string;
	env?: string;
	attributes?: {
		timestamp?: string | number;
		service?: string;
		hostname?: string;
		env?: string;
		level?: string;
		log_level?: string | number;
		topic?: string;
		function?: { path?: string };
		convex?: Record<string, unknown>;
		[k: string]: unknown;
	} & Record<string, unknown>;
	[k: string]: unknown;
};

type SentryEvent = {
	event_id?: string;
	message: string;
	level: 'fatal' | 'error' | 'warning' | 'info' | 'debug';
	timestamp?: string; // ISO8601
	platform?: 'other';
	logger?: string;
	environment?: string;
	tags?: Record<string, string>;
	fingerprint?: string[];
	extra?: Record<string, unknown>;
};

const te = new TextEncoder();

const mapLevel = (lvl: unknown): SentryEvent['level'] => {
	if (lvl == null) return 'error';
	const s = String(lvl).toLowerCase();
	if (s === 'fatal' || s === 'emergency' || s === 'critical' || s === 'alert') return 'fatal';
	if (s === 'warn' || s === 'warning') return 'warning';
	if (s === 'error' || s === 'err') return 'error';
	if (s === 'debug') return 'debug';
	if (s === 'info' || s === 'notice') return 'info';
	return 'error';
};

const envelopeUrlFromDsn = (dsn: string): string => {
	const u = new URL(dsn);
	const host = u.host; // e.g. oXXXX.ingest.us.sentry.io
	const projectId = u.pathname.replace(/^\/+/, ''); // e.g. 1234567
	return `https://${host}/api/${projectId}/envelope/`;
};

const toIso = (d?: string | number): string | undefined => {
	if (d == null) return undefined;
	try {
		// numeric epoch or date string → ISO
		if (typeof d === 'number') return new Date(d).toISOString();
		const n = Number(d);
		if (!Number.isNaN(n) && d.trim() !== '') return new Date(n).toISOString();
		return new Date(d).toISOString();
	} catch {
		return undefined;
	}
};

const toSentryEvent = (log: DDLog): SentryEvent => {
	const rawMsg = log.message ?? 'datadog log';
	const msg = typeof rawMsg === 'string' ? rawMsg : JSON.stringify(rawMsg);
	const date = log.date ?? log.timestamp ?? log.attributes?.timestamp;
	const lvl = log.level ?? log.status ?? log.attributes?.log_level ?? log.attributes?.level;

	const service = (log.service as string) || (log.attributes?.service as string) || 'unknown';

	const host = (log.host as string) || (log.attributes?.hostname as string) || 'unknown';

	const env =
		(log.environment as string) ||
		(log.env as string) ||
		(log.attributes?.env as string) ||
		(log.attributes?.convex?.['deployment_type'] as string) ||
		'prod';

	const normalizeEnv = (env: string): string => {
		if (env === 'production') return 'prod';
		return env;
	};

	const functionPath = (log.attributes?.function as { path?: string } | undefined)?.path || 'n/a';

	return {
		message: msg.slice(0, 8000), // keep the issue title readable
		level: mapLevel(lvl),
		timestamp: toIso(date),
		platform: 'other',
		logger: 'datadog',
		environment: normalizeEnv(env),
		tags: {
			service,
			host,
			dd_source: (log.source as string) || 'datadog',
			function_path: functionPath,
		},
		extra: {
			datadog_id: log._id,
			topic: log.attributes?.topic,
			convex: log.attributes?.convex,
			// keep an eye on size — trim if you hit Sentry item limits (~1MB)
			attributes: log.attributes,
		},
	};
};

const uuid32 = (): string => crypto.randomUUID().replace(/-/g, '');

const buildEnvelope = (dsn: string, event: SentryEvent): string => {
	const event_id = uuid32();
	const header = {
		event_id,
		dsn,
		sent_at: new Date().toISOString(),
		sdk: { name: 'dd-to-sentry-relay', version: '1.0.0' },
	};
	const payload = JSON.stringify({ event_id, ...event });
	const itemHeader = {
		type: 'event',
		length: te.encode(payload).length,
		content_type: 'application/json',
	};
	// 3 lines: envelope header, item header, payload
	return `${JSON.stringify(header)}\n${JSON.stringify(itemHeader)}\n${payload}\n`;
};

async function parseIncomingJson(req: Request): Promise<DDLog[] | DDLog> {
	const enc = req.headers.get('content-encoding')?.toLowerCase() || '';
	if (enc.includes('gzip')) {
		const ds = new DecompressionStream('gzip');
		const decompressed = (req.body as ReadableStream).pipeThrough(ds);
		const text = await new Response(decompressed).text();
		return JSON.parse(text || '[]');
	}
	return await req.json();
}

export default {
	async fetch(req: Request, env: Env): Promise<Response> {
		if (req.method !== 'POST') {
			return new Response('ok', { status: 200 });
		}
		if (!env.SENTRY_DSN) {
			return new Response('SENTRY_DSN missing', { status: 500 });
		}

		let payload: DDLog[] | DDLog;
		try {
			payload = await parseIncomingJson(req);
		} catch {
			return new Response('bad json', { status: 400 });
		}

		const envelopeUrl = envelopeUrlFromDsn(env.SENTRY_DSN);
		const items = Array.isArray(payload) ? payload : [payload];

		const sends = items.map((it) => {
			const event = toSentryEvent(it);
			const envlp = buildEnvelope(env.SENTRY_DSN, event);
			return fetch(envelopeUrl, {
				method: 'POST',
				headers: { 'content-type': 'application/x-sentry-envelope' },
				body: envlp,
			});
		});

		const results = await Promise.allSettled(sends);
		const forwarded = results.filter((r) => r.status === 'fulfilled').length;
		const failed = results.length - forwarded;

		return new Response(JSON.stringify({ forwarded, failed }), {
			status: failed ? 207 : 200,
			headers: { 'content-type': 'application/json' },
		});
	},
};
