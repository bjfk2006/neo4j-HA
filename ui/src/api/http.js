// Tiny fetch wrapper with global 401 → /login redirect and request-id passthrough.

function isAbsolute(url) {
  return /^https?:\/\//i.test(url)
}

// Default per-request timeout. Long enough for heavy consistency scans
// (Phase 2 data-diff on largeish graphs), short enough to fail fast on
// genuinely unreachable Agents. Callers can override via opts.timeoutMs.
const DEFAULT_TIMEOUT_MS = 30_000

async function request(url, opts = {}) {
  const finalUrl = isAbsolute(url) ? url : url
  const headers = {
    'Accept': 'application/json',
    'X-Requested-With': 'ha-ui',
    ...(opts.headers || {})
  }
  if (opts.body && typeof opts.body !== 'string' && !(opts.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json'
    opts.body = JSON.stringify(opts.body)
  }
  // AbortController-based timeout. We don't pass opts.signal through if caller
  // already set one — chaining external + timeout abort is rare and complicated.
  const controller = new AbortController()
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  let res
  try {
    res = await fetch(finalUrl, {
      credentials: 'same-origin',
      signal: controller.signal,
      ...opts,
      headers
    })
  } catch (e) {
    clearTimeout(timer)
    if (e.name === 'AbortError') {
      throw new HttpError(0, 'timeout',
        `Request timed out after ${timeoutMs}ms: ${url}`)
    }
    throw e
  }
  clearTimeout(timer)
  if (res.status === 401 && !url.endsWith('/api/login')) {
    // Force a re-login. Lazy-import to avoid module cycle with the store.
    const { useAuthStore } = await import('../stores/auth.js')
    useAuthStore().clearUser()
    if (location.pathname !== '/login') {
      const next = encodeURIComponent(location.pathname + location.search)
      location.replace(`/login?redirect=${next}`)
    }
    throw new HttpError(401, 'unauthorized', 'Session expired')
  }
  const text = await res.text()
  let body = null
  if (text) {
    try { body = JSON.parse(text) }
    catch (e) { body = { raw: text } }
  }
  if (!res.ok) {
    const err = body && typeof body === 'object' ? body : {}
    throw new HttpError(res.status, err.error || 'http_error', err.message || res.statusText, body)
  }
  return body
}

export class HttpError extends Error {
  constructor(status, code, message, body) {
    super(message || code || 'HTTP error')
    this.status = status
    this.code = code
    this.body = body
  }
}

export const http = {
  get:  (u, o = {}) => request(u, { ...o, method: 'GET' }),
  post: (u, body, o = {}) => request(u, { ...o, method: 'POST', body }),
  del:  (u, o = {}) => request(u, { ...o, method: 'DELETE' })
}

// ----- typed endpoints -----
export const api = {
  login: (username, password) => http.post('/api/login', { username, password }),
  logout: () => http.post('/api/logout'),
  me: () => http.get('/api/me'),

  clusterStatus: () => http.get('/api/cluster/status'),
  nodeDetail: (id) => http.get(`/api/cluster/nodes/${encodeURIComponent(id)}`),
  failover: (nodeId) => http.post(
    `/api/cluster/failover${nodeId ? `?nodeId=${encodeURIComponent(nodeId)}` : ''}`),
  switchover: (targetNodeId) => http.post(
    `/api/cluster/switchover${targetNodeId ? `?targetNodeId=${encodeURIComponent(targetNodeId)}` : ''}`),
  fullsync: (nodeId) => http.post(
    `/api/cluster/fullsync?nodeId=${encodeURIComponent(nodeId)}`),
  backupPrepare: (nodeId) => http.post(
    `/api/cluster/backup/prepare${nodeId ? `?nodeId=${encodeURIComponent(nodeId)}` : ''}`),
  backupComplete: () => http.post('/api/cluster/backup/complete'),
  backupStatus: () => http.get('/api/cluster/backup/status'),

  audit: (since, limit = 50) => {
    const qs = new URLSearchParams()
    if (since) qs.set('since', since)
    qs.set('limit', String(limit))
    return http.get(`/api/audit?${qs.toString()}`)
  },
  metricsSummary: () => http.get('/api/metrics-summary'),

  // Data consistency (Phase 1+2)
  dataStats: () => http.get('/api/cluster/data-stats'),
  dataDiff:  (opts = {}) => {
    const qs = new URLSearchParams()
    if (opts.scope)    qs.set('scope', opts.scope)
    if (opts.scopeArg) qs.set('scopeArg', opts.scopeArg)
    if (opts.limit)    qs.set('limit', String(opts.limit))
    if (opts.type)     qs.set('type', opts.type)
    if (opts.nodeId)   qs.set('nodeId', opts.nodeId)
    // Diff scan can take 10–30s on large graphs; allow 45s before aborting.
    return http.get(`/api/cluster/data-diff?${qs.toString()}`, { timeoutMs: 45_000 })
  }
}
