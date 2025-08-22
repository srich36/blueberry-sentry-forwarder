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
		function?: { path?: string; type?: string; mutation_retry_count?: number; request_id?: string };
		convex?: Record<string, unknown>;
		[k: string]: unknown;
	} & Record<string, unknown>;
	[k: string]: unknown;
};

type SentryStackFrame = {
	filename: string;
	function?: string;
	lineno?: number;
	colno?: number;
	in_app?: boolean;
};

type SentryException = {
	type: string;
	value: string;
	stacktrace?: {
		frames: SentryStackFrame[];
	};
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
	exception?: {
		values: SentryException[];
	};
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

type NormalizedMessage = {
	message: string;
	extractedIds: Record<string, string>;
};

const normalizeMessage = (message: string): NormalizedMessage => {
	const extractedIds: Record<string, string> = {};
	let normalized = message;
	let idCounter = 0;

	// Convex IDs (32 character alphanumeric strings)
	normalized = normalized.replace(/\b[a-z0-9]{32}\b/g, (match) => {
		const key = `convex_id_${++idCounter}`;
		extractedIds[key] = match;
		return '<convex_id>';
	});

	// Compound numeric IDs (e.g., 157322910959964_1171172118373752)
	normalized = normalized.replace(/\b\d{10,}_\d{10,}\b/g, (match) => {
		const key = `compound_id_${++idCounter}`;
		extractedIds[key] = match;
		return '<compound_id>';
	});

	// UUIDs
	normalized = normalized.replace(
		/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi,
		(match) => {
			const key = `uuid_${++idCounter}`;
			extractedIds[key] = match;
			return '<uuid>';
		}
	);

	// MongoDB ObjectIds (24 hex characters)
	normalized = normalized.replace(/\b[0-9a-f]{24}\b/gi, (match) => {
		const key = `object_id_${++idCounter}`;
		extractedIds[key] = match;
		return '<object_id>';
	});

	// Hex IDs with prefix
	normalized = normalized.replace(/(0x|#)[0-9a-f]{6,}/gi, (match) => {
		const key = `hex_id_${++idCounter}`;
		extractedIds[key] = match;
		return '<hex_id>';
	});

	// IDs with prefixes (user_12345, order-98765, etc.)
	normalized = normalized.replace(/\b(user|order|item|session|request|id)[_-]\d{3,}\b/gi, (match) => {
		const prefix = match.split(/[_-]/)[0];
		const key = `${prefix}_id_${++idCounter}`;
		extractedIds[key] = match;
		return `${prefix}_<id>`;
	});

	// Standalone long numeric IDs (10+ digits)
	normalized = normalized.replace(/\b\d{10,}\b/g, (match) => {
		const key = `numeric_id_${++idCounter}`;
		extractedIds[key] = match;
		return '<numeric_id>';
	});

	return {
		message: normalized,
		extractedIds,
	};
};

const toSentryEvent = (log: DDLog): SentryEvent => {
	const rawMsg = log.message ?? log.msg ?? `${JSON.stringify(log)}`;
	const msg = typeof rawMsg === 'string' ? rawMsg : JSON.stringify(rawMsg);
	
	// Normalize the message to improve fingerprinting
	const { message: normalizedMsg, extractedIds } = normalizeMessage(msg);
	
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
	const functionType = log.attributes?.function?.type as string | undefined;
	const mutationRetryCount = log.attributes?.function?.mutation_retry_count as number | undefined;
	const requestId = log.attributes?.function?.request_id as string | undefined;

	return {
		message: normalizedMsg.slice(0, 8000), // keep the issue title readable
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
			function_type: functionType || 'unknown',
			has_retry: mutationRetryCount ? 'true' : 'false',
		},
		extra: {
			datadog_id: log._id,
			topic: log.attributes?.topic,
			convex: log.attributes?.convex,
			function_metadata: {
				path: functionPath,
				type: functionType,
				mutation_retry_count: mutationRetryCount,
				request_id: requestId,
			},
			// Preserve extracted IDs for debugging
			extracted_ids: extractedIds,
			// Original message for reference
			original_message: msg.slice(0, 2000),
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
