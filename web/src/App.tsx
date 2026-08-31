import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { APIError, api } from './api'
import {
  applyTheme,
  persistThemePreference,
  readThemePreference,
  syncThemeColor,
  themeStorageKey,
  type ThemePreference,
} from './theme'
import type {
  Command,
  CommandOutput,
  Instance,
  JobResponse,
  JobsResponse,
  JobStatus,
  QueueCounts,
  QueueSummary,
} from './types'

const tokenKey = 'slipway.web.token'
type OutputState =
  | { state: 'loading' }
  | { state: 'ready'; output: CommandOutput }
  | { state: 'error'; message: string }

export default function App() {
  const [token, setToken] = useState(() => sessionStorage.getItem(tokenKey) ?? '')
  const [authMessage, setAuthMessage] = useState('')
  const [theme, setTheme] = useThemePreference()

  useEffect(() => {
    syncThemeColor(token ? 'dashboard' : 'auth')
  }, [theme, token])

  const authenticate = (value: string) => {
    const normalized = value.trim()
    sessionStorage.setItem(tokenKey, normalized)
    setAuthMessage('')
    setToken(normalized)
  }

  const signOut = (message = '') => {
    sessionStorage.removeItem(tokenKey)
    setToken('')
    setAuthMessage(message)
  }

  if (!token) {
    return <TokenGate message={authMessage} theme={theme} onThemeChange={setTheme} onAuthenticate={authenticate} />
  }
  return <Dashboard token={token} theme={theme} onThemeChange={setTheme} onSignOut={signOut} />
}

function useThemePreference() {
  const [theme, setTheme] = useState<ThemePreference>(readThemePreference)

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') {
      applyTheme(theme, false)
      return
    }
    const query = window.matchMedia('(prefers-color-scheme: dark)')
    const syncSystemTheme = () => applyTheme(theme, query.matches)
    syncSystemTheme()
    if (theme !== 'system') return
    query.addEventListener('change', syncSystemTheme)
    return () => query.removeEventListener('change', syncSystemTheme)
  }, [theme])

  useEffect(() => {
    const syncStoredTheme = (event: StorageEvent) => {
      if (event.key === themeStorageKey) setTheme(readThemePreference())
    }
    window.addEventListener('storage', syncStoredTheme)
    return () => window.removeEventListener('storage', syncStoredTheme)
  }, [])

  const updateTheme = useCallback((preference: ThemePreference) => {
    persistThemePreference(preference)
    setTheme(preference)
  }, [])

  return [theme, updateTheme] as const
}

function ThemeControl({ theme, onChange, className = '' }: { theme: ThemePreference; onChange: (theme: ThemePreference) => void; className?: string }) {
  return (
    <label className={`theme-control ${className}`.trim()}>
      <span>Theme</span>
      <select aria-label="Color theme" value={theme} onChange={(event) => onChange(event.target.value as ThemePreference)}>
        <option value="system">Auto</option>
        <option value="light">Light</option>
        <option value="dark">Night</option>
      </select>
    </label>
  )
}

function TokenGate({
  message,
  theme,
  onThemeChange,
  onAuthenticate,
}: {
  message: string
  theme: ThemePreference
  onThemeChange: (theme: ThemePreference) => void
  onAuthenticate: (token: string) => void
}) {
  const [value, setValue] = useState('')

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (value.trim()) onAuthenticate(value)
  }

  return (
    <main className="auth-shell">
      <ThemeControl className="auth-theme-control" theme={theme} onChange={onThemeChange} />
      <section className="auth-card" aria-labelledby="auth-title">
        <img className="brand-mark" src="/icon.png" alt="" />
        <p className="eyebrow">Local control plane</p>
        <h1 id="auth-title">Enter the control room.</h1>
        <p className="auth-copy">
          Paste the access token from the private token file shown in the <code>slipwayd</code> startup log.
          It stays in this browser tab only.
        </p>
        <form onSubmit={submit}>
          <label htmlFor="access-token">Web access token</label>
          <input
            id="access-token"
            type="password"
            autoComplete="off"
            spellCheck="false"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            placeholder="Paste token"
            autoFocus
          />
          {message && <p className="form-error" role="alert">{message}</p>}
          <button className="button button-primary button-wide" type="submit" disabled={!value.trim()}>
            Open dashboard <span aria-hidden="true">→</span>
          </button>
        </form>
        <p className="auth-note">
          This token is sent with dashboard API requests. Plain HTTP does not protect it on an untrusted network.
        </p>
      </section>
      <div className="auth-grid" aria-hidden="true" />
    </main>
  )
}

function Dashboard({
  token,
  theme,
  onThemeChange,
  onSignOut,
}: {
  token: string
  theme: ThemePreference
  onThemeChange: (theme: ThemePreference) => void
  onSignOut: (message?: string) => void
}) {
  const [tab, setTab] = useState<'queues' | 'instances'>('queues')
  const [queues, setQueues] = useState<QueueSummary[] | null>(null)
  const [instances, setInstances] = useState<Instance[] | null>(null)
  const [selectedQueueID, setSelectedQueueID] = useState('')
  const [status, setStatus] = useState<'' | JobStatus>('')
  const [watch, setWatch] = useState('')
  const [offset, setOffset] = useState(0)
  const [jobs, setJobs] = useState<JobsResponse | null>(null)
  const [selectedJobID, setSelectedJobID] = useState<number | null>(null)
  const [jobDetail, setJobDetail] = useState<JobResponse | null>(null)
  const [outputs, setOutputs] = useState<Record<number, OutputState>>({})
  const [refreshError, setRefreshError] = useState('')
  const [jobsError, setJobsError] = useState('')
  const [detailError, setDetailError] = useState('')
  const [actionError, setActionError] = useState('')
  const [actionID, setActionID] = useState('')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const overviewController = useRef<AbortController | null>(null)
  const jobsController = useRef<AbortController | null>(null)
  const jobsRequestKey = useRef('')
  const detailController = useRef<AbortController | null>(null)
  const detailRequestKey = useRef('')
  const outputControllers = useRef(new Map<number, AbortController>())

  const handleError = useCallback((error: unknown, fallback: string) => {
    if (error instanceof APIError && error.status === 401) {
      onSignOut('That token was not accepted. Check the token file and try again.')
      return ''
    }
    return error instanceof Error ? error.message : fallback
  }, [onSignOut])

  const refreshOverview = useCallback(async () => {
    if (overviewController.current) return
    const controller = new AbortController()
    overviewController.current = controller
    try {
      const [queueResult, instanceResult] = await Promise.all([
        api.queues(token, controller.signal),
        api.instances(token, controller.signal),
      ])
      if (controller.signal.aborted) return
      setQueues(queueResult.queues)
      setInstances(instanceResult.instances)
      setLastUpdated(new Date(queueResult.generated_at))
      setRefreshError('')
    } catch (error) {
      if (controller.signal.aborted) return
      const message = handleError(error, 'Could not refresh slipway')
      if (message) setRefreshError(message)
    } finally {
      if (overviewController.current === controller) overviewController.current = null
    }
  }, [handleError, token])

  useEffect(() => {
    void refreshOverview()
    const refresh = () => {
      if (!document.hidden) void refreshOverview()
    }
    const interval = window.setInterval(refresh, 4000)
    document.addEventListener('visibilitychange', refresh)
    window.addEventListener('focus', refresh)
    return () => {
      window.clearInterval(interval)
      document.removeEventListener('visibilitychange', refresh)
      window.removeEventListener('focus', refresh)
    }
  }, [refreshOverview])

  useEffect(() => {
    if (!queues?.length) {
      setSelectedQueueID('')
      return
    }
    if (!queues.some((queue) => queue.id === selectedQueueID)) {
      setSelectedQueueID(queues[0].id)
    }
  }, [queues, selectedQueueID])

  const selectedQueue = queues?.find((queue) => queue.id === selectedQueueID) ?? null

  const refreshJobs = useCallback(async () => {
    if (tab !== 'queues') {
      jobsController.current?.abort()
      jobsController.current = null
      jobsRequestKey.current = ''
      return
    }
    if (!selectedQueueID || selectedQueue?.database_state !== 'ready') {
      jobsController.current?.abort()
      jobsController.current = null
      jobsRequestKey.current = ''
      setJobs(null)
      return
    }
    const requestKey = `${selectedQueueID}\u0000${status}\u0000${watch}\u0000${offset}`
    if (jobsController.current) {
      if (jobsRequestKey.current === requestKey) return
      jobsController.current.abort()
    }
    const controller = new AbortController()
    jobsController.current = controller
    jobsRequestKey.current = requestKey
    try {
      const result = await api.jobs(token, selectedQueueID, status, watch, offset, controller.signal)
      if (controller.signal.aborted) return
      setJobs(result)
      setJobsError('')
    } catch (error) {
      if (controller.signal.aborted) return
      const message = handleError(error, 'Could not refresh jobs')
      if (message) setJobsError(message)
    } finally {
      if (jobsController.current === controller) {
        jobsController.current = null
        jobsRequestKey.current = ''
      }
    }
  }, [handleError, offset, selectedQueue?.database_state, selectedQueueID, status, tab, token, watch])

  useEffect(() => {
    setJobs(null)
    setJobsError('')
    void refreshJobs()
    const interval = window.setInterval(() => {
      if (!document.hidden) void refreshJobs()
    }, 4000)
    return () => window.clearInterval(interval)
  }, [refreshJobs])

  const refreshDetail = useCallback(async () => {
    if (!selectedQueueID || selectedJobID === null) {
      detailController.current?.abort()
      detailController.current = null
      detailRequestKey.current = ''
      return
    }
    const requestKey = `${selectedQueueID}\u0000${selectedJobID}`
    if (detailController.current) {
      if (detailRequestKey.current === requestKey) return
      detailController.current.abort()
    }
    const controller = new AbortController()
    detailController.current = controller
    detailRequestKey.current = requestKey
    try {
      const detail = await api.job(token, selectedQueueID, selectedJobID, controller.signal)
      if (controller.signal.aborted) return
      setJobDetail(detail)
      setDetailError('')
    } catch (error) {
      if (controller.signal.aborted) return
      const message = handleError(error, 'Could not load job detail')
      if (message) setDetailError(message)
    } finally {
      if (detailController.current === controller) {
        detailController.current = null
        detailRequestKey.current = ''
      }
    }
  }, [handleError, selectedJobID, selectedQueueID, token])

  useEffect(() => {
    detailController.current?.abort()
    detailController.current = null
    detailRequestKey.current = ''
    for (const controller of outputControllers.current.values()) controller.abort()
    outputControllers.current.clear()
    setJobDetail(null)
    setOutputs({})
    setDetailError('')
    if (selectedJobID === null) return
    void refreshDetail()
  }, [refreshDetail, selectedJobID, selectedQueueID])

  useEffect(() => {
    if (selectedJobID === null || !jobDetail || !['QUEUED', 'RUNNING'].includes(jobDetail.job.status)) return
    const interval = window.setInterval(() => {
      if (!document.hidden) void refreshDetail()
    }, 2500)
    return () => window.clearInterval(interval)
  }, [jobDetail, refreshDetail, selectedJobID])

  useEffect(() => () => {
    overviewController.current?.abort()
    jobsController.current?.abort()
    detailController.current?.abort()
    for (const controller of outputControllers.current.values()) controller.abort()
  }, [])

  const totals = useMemo(() => {
    const result: QueueCounts = { total: 0, queued: 0, running: 0, succeeded: 0, failed: 0 }
    for (const queue of queues ?? []) {
      result.total += queue.counts.total
      result.queued += queue.counts.queued
      result.running += queue.counts.running
      result.succeeded += queue.counts.succeeded
      result.failed += queue.counts.failed
    }
    return result
  }, [queues])

  const changeQueue = (id: string) => {
    jobsController.current?.abort()
    jobsController.current = null
    jobsRequestKey.current = ''
    setSelectedQueueID(id)
    setStatus('')
    setWatch('')
    setOffset(0)
    setJobs(null)
    setJobsError('')
    setSelectedJobID(null)
  }

  const changeStatus = (nextStatus: '' | JobStatus) => {
    jobsController.current?.abort()
    setJobs(null)
    setJobsError('')
    setStatus(nextStatus)
    setOffset(0)
  }

  const changeWatch = (nextWatch: string) => {
    jobsController.current?.abort()
    setJobs(null)
    setJobsError('')
    setWatch(nextWatch)
    setOffset(0)
  }

  const changeOffset = (nextOffset: number) => {
    jobsController.current?.abort()
    setJobs(null)
    setJobsError('')
    setOffset(nextOffset)
  }

  const performAction = async (kind: 'start' | 'stop', id: string) => {
    if (kind === 'stop' && !window.confirm('Stop this slipway instance gracefully?')) return
    setActionID(id)
    setActionError('')
    try {
      if (kind === 'start') await api.start(token, id)
      else await api.stop(token, id)
      await refreshOverview()
      await refreshJobs()
    } catch (error) {
      const message = handleError(error, `Could not ${kind} instance`)
      if (message) setActionError(message)
    } finally {
      setActionID('')
    }
  }

  const loadOutput = async (commandID: number) => {
    if (!selectedQueueID) return
    outputControllers.current.get(commandID)?.abort()
    const controller = new AbortController()
    outputControllers.current.set(commandID, controller)
    setOutputs((current) => ({ ...current, [commandID]: { state: 'loading' } }))
    try {
      const output = await api.output(token, selectedQueueID, commandID, controller.signal)
      if (controller.signal.aborted) return
      setOutputs((current) => ({ ...current, [commandID]: { state: 'ready', output } }))
    } catch (error) {
      if (controller.signal.aborted) return
      const message = handleError(error, 'Could not load output')
      if (message) setOutputs((current) => ({ ...current, [commandID]: { state: 'error', message } }))
    } finally {
      if (outputControllers.current.get(commandID) === controller) outputControllers.current.delete(commandID)
    }
  }

  const closeJob = useCallback(() => setSelectedJobID(null), [])

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <img className="brand-mark brand-mark-small" src="/icon.png" alt="" />
          <div>
            <strong>slipway</strong>
            <span>Control room</span>
          </div>
        </div>
        <nav className="topnav" aria-label="Primary">
          <button className={tab === 'queues' ? 'active' : ''} aria-current={tab === 'queues' ? 'page' : undefined} onClick={() => setTab('queues')}>Queues</button>
          <button className={tab === 'instances' ? 'active' : ''} aria-current={tab === 'instances' ? 'page' : undefined} onClick={() => setTab('instances')}>Instances</button>
        </nav>
        <div className="topbar-actions">
          <ThemeControl theme={theme} onChange={onThemeChange} />
          <span className={`connection-state ${refreshError ? 'offline' : lastUpdated ? 'online' : 'connecting'}`}>
            <i /> {refreshError ? 'Refresh failed' : lastUpdated ? 'Local daemon' : 'Connecting'}
          </span>
          <button className="text-button" onClick={() => onSignOut()}>Lock</button>
        </div>
      </header>

      {(refreshError || actionError) && (
        <div className="notice notice-error" role="alert">
          <span>{actionError || refreshError}</span>
          {actionError
            ? <button onClick={() => setActionError('')}>Dismiss</button>
            : <button onClick={() => void refreshOverview()}>Retry</button>}
        </div>
      )}

      <main className="workspace">
        <section className="page-intro">
          <div>
            <p className="eyebrow">{tab === 'queues' ? 'Durable work' : 'Runtime history'}</p>
            <h1>{tab === 'queues' ? 'Every queue, one view.' : 'Instance activity.'}</h1>
          </div>
          <div className="update-block">
            <span>Last sync</span>
            <strong>{lastUpdated ? formatClock(lastUpdated.toISOString()) : 'Connecting…'}</strong>
            <button className="icon-button" onClick={() => void refreshOverview()} aria-label="Refresh dashboard">↻</button>
          </div>
        </section>

        {tab === 'queues' ? (
          <>
            <section className="metrics" aria-label="Queue totals">
              <Metric label="All jobs" value={totals.total} tone="ink" />
              <Metric label="Queued" value={totals.queued} tone="queued" />
              <Metric label="Running" value={totals.running} tone="running" />
              <Metric label="Failed" value={totals.failed} tone="failed" />
            </section>

            {queues === null ? (
              <DashboardSkeleton />
            ) : queues.length === 0 ? (
              <EmptyState title="No queues registered" copy="Start an instance with slipwayd --config or the trusted slipway CLI. Its durable queue will appear here." />
            ) : (
              <section className="queue-workspace">
                <QueueRail
                  queues={queues}
                  selectedID={selectedQueueID}
                  actionID={actionID}
                  onSelect={changeQueue}
                  onAction={performAction}
                />
                {selectedQueue && (
                  <QueuePanel
                    queue={selectedQueue}
                    jobs={jobs}
                    error={jobsError}
                    status={status}
                    watch={watch}
                    offset={offset}
                    onStatus={changeStatus}
                    onWatch={changeWatch}
                    onOffset={changeOffset}
                    onRefresh={() => void refreshJobs()}
                    onSelectJob={setSelectedJobID}
                  />
                )}
              </section>
            )}
          </>
        ) : (
          <InstancesPanel instances={instances} actionID={actionID} onStop={(id) => void performAction('stop', id)} />
        )}
      </main>

      {selectedJobID !== null && selectedQueue && (
        <JobDrawer
          key={`${selectedQueue.id}:${selectedJobID}`}
          queue={selectedQueue}
          detail={jobDetail}
          error={detailError}
          outputs={outputs}
          onLoadOutput={(id) => void loadOutput(id)}
          onClose={closeJob}
        />
      )}
    </div>
  )
}

function QueueRail({
  queues,
  selectedID,
  actionID,
  onSelect,
  onAction,
}: {
  queues: QueueSummary[]
  selectedID: string
  actionID: string
  onSelect: (id: string) => void
  onAction: (kind: 'start' | 'stop', id: string) => void
}) {
  return (
    <aside className="queue-rail" aria-label="Configured queues">
      <div className="section-label"><span>Configured queues</span><b>{queues.length}</b></div>
      <div className="queue-list">
        {queues.map((queue) => {
          const active = Boolean(queue.active_instance)
          const actionKey = active ? queue.active_instance!.id : queue.id
          const instanceState = queue.active_instance?.state
          const stateCopy = instanceState
            ? `Instance ${instanceState}`
            : queue.database_state === 'ready'
              ? 'Stopped · history available'
              : queue.database_state === 'missing'
                ? 'Queue not initialized'
                : 'Queue unavailable'
          return (
            <article className={`queue-card ${selectedID === queue.id ? 'selected' : ''}`} key={queue.id} title={queue.config_path}>
              <button className="queue-select" onClick={() => onSelect(queue.id)} aria-pressed={selectedID === queue.id}>
                <span className={`queue-orb ${active ? 'live' : ''}`} aria-hidden="true" />
                <span className="queue-card-copy">
                  <strong>{queue.display_name}</strong>
                  <small>{stateCopy}</small>
                </span>
                <span className="queue-total">{queue.counts.total}</span>
              </button>
              <button
                className={`mini-action ${active ? 'stop' : ''}`}
                onClick={() => onAction(active ? 'stop' : 'start', actionKey)}
                disabled={Boolean(actionID) || instanceState === 'stopping'}
                aria-label={`${active ? 'Stop' : 'Start'} ${queue.display_name}`}
              >
                {actionID === actionKey ? '…' : instanceState === 'stopping' ? '…' : active ? '■' : '▶'}
              </button>
            </article>
          )
        })}
      </div>
    </aside>
  )
}

function QueuePanel({
  queue,
  jobs,
  error,
  status,
  watch,
  offset,
  onStatus,
  onWatch,
  onOffset,
  onRefresh,
  onSelectJob,
}: {
  queue: QueueSummary
  jobs: JobsResponse | null
  error: string
  status: '' | JobStatus
  watch: string
  offset: number
  onStatus: (status: '' | JobStatus) => void
  onWatch: (watch: string) => void
  onOffset: (offset: number) => void
  onRefresh: () => void
  onSelectJob: (id: number) => void
}) {
  return (
    <section className="queue-panel" aria-labelledby="queue-title">
      <header className="queue-header">
        <div>
          <div className="queue-title-line">
            <h2 id="queue-title">{queue.display_name}</h2>
            <StatusPill status={queue.active_instance?.state ?? 'stopped'} />
          </div>
          <p title={queue.config_path}>{queue.config_path}</p>
        </div>
        <div className="hash-block" title={queue.config_hash}>
          <span>Config</span>
          <code>{queue.config_hash.slice(0, 10)}</code>
        </div>
      </header>

      <div className="queue-count-strip">
        <CountButton label="All" value={queue.counts.total} active={!status} onClick={() => onStatus('')} />
        <CountButton label="Queued" value={queue.counts.queued} active={status === 'QUEUED'} onClick={() => onStatus('QUEUED')} />
        <CountButton label="Running" value={queue.counts.running} active={status === 'RUNNING'} onClick={() => onStatus('RUNNING')} />
        <CountButton label="Succeeded" value={queue.counts.succeeded} active={status === 'SUCCEEDED'} onClick={() => onStatus('SUCCEEDED')} />
        <CountButton label="Failed" value={queue.counts.failed} active={status === 'FAILED'} onClick={() => onStatus('FAILED')} />
      </div>

      <div className="table-tools">
        <label>
          <span>Watch</span>
          <select value={watch} onChange={(event) => onWatch(event.target.value)}>
            <option value="">All watches</option>
            {queue.watches.map((name) => <option key={name} value={name}>{name}</option>)}
          </select>
        </label>
        <button className="icon-button" onClick={onRefresh} aria-label="Refresh jobs">↻</button>
      </div>

      {queue.database_state !== 'ready' ? (
        <EmptyState
          title={queue.database_state === 'missing' ? 'Queue not created yet' : 'Queue unavailable'}
          copy={queue.error ?? 'Start the instance to initialize this queue.'}
          compact
        />
      ) : error && !jobs ? (
        <EmptyState title="Could not read this queue" copy={error} compact />
      ) : jobs === null ? (
        <TableSkeleton />
      ) : jobs.jobs.length === 0 ? (
        <>
          <EmptyState
            title={status || watch ? 'No matching jobs' : 'No jobs recorded'}
            copy={offset > 0 ? 'This page is now empty; return to newer jobs.' : status || watch ? 'Try a different status or watch filter.' : 'Matching files will appear here when they are discovered.'}
            compact
          />
          {offset > 0 && (
            <footer className="pagination">
              <span>Empty page</span>
              <div><button onClick={() => onOffset(Math.max(0, offset - jobs.limit))}>← Newer</button></div>
            </footer>
          )}
        </>
      ) : (
        <>
          {error && <p className="inline-warning">Showing the last result: {error}</p>}
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Job</th>
                  <th>Watch</th>
                  <th>Attempts</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {jobs.jobs.map((job) => (
                  <tr key={job.id}>
                    <td><StatusPill status={job.status} /></td>
                    <td>
                      <button className="job-link" onClick={() => onSelectJob(job.id)}>
                        <strong>{fileName(job.path)}</strong>
                        <small title={job.path}>#{job.id} · {job.path}</small>
                        {job.status === 'QUEUED' && job.last_error && (
                          <small className="retry-note" title={job.last_error}>Retry {relativeTime(job.available_at)} · {job.last_error}</small>
                        )}
                      </button>
                    </td>
                    <td><code className="watch-tag">{job.watch_name}</code></td>
                    <td>{job.attempts}<span className="muted"> / {job.max_retries + 1}</span></td>
                    <td>{relativeTime(job.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <footer className="pagination">
            <span>{offset + 1}–{offset + jobs.jobs.length}</span>
            <div>
              <button disabled={offset === 0} onClick={() => onOffset(Math.max(0, offset - jobs.limit))}>← Newer</button>
              <button disabled={!jobs.has_more} onClick={() => onOffset(offset + jobs.limit)}>Older →</button>
            </div>
          </footer>
        </>
      )}
    </section>
  )
}

function InstancesPanel({
  instances,
  actionID,
  onStop,
}: {
  instances: Instance[] | null
  actionID: string
  onStop: (id: string) => void
}) {
  if (instances === null) return <TableSkeleton />
  if (instances.length === 0) {
    return <EmptyState title="No instance history" copy="Start a configured queue to create its first runtime instance." />
  }
  return (
    <section className="instances-card">
      <div className="section-label"><span>Current daemon lifetime</span><b>{instances.length}</b></div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr><th>State</th><th>Instance</th><th>Configuration</th><th>Started</th><th>Duration</th><th /></tr>
          </thead>
          <tbody>
            {instances.map((instance) => (
              <tr key={instance.id}>
                <td><StatusPill status={instance.state} /></td>
                <td>
                  <strong>{instance.name}</strong>
                  <small className="cell-subtitle">{instance.id}</small>
                  {instance.error && <small className="cell-error" title={instance.error}>{instance.error}</small>}
                </td>
                <td><span className="path-cell" title={instance.config_path}>{instance.config_path}</span></td>
                <td>{formatDate(instance.started_at)}</td>
                <td>{duration(instance.started_at, instance.finished_at)}</td>
                <td>
                  {['running', 'stopping'].includes(instance.state) && (
                    <button className="button button-danger button-small" disabled={Boolean(actionID) || instance.state === 'stopping'} onClick={() => onStop(instance.id)}>
                      {instance.state === 'stopping' ? 'Stopping' : actionID === instance.id ? 'Working…' : 'Stop'}
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="retention-note">Instance history is bounded to this daemon lifetime. Queue history remains durable.</p>
    </section>
  )
}

function JobDrawer({
  queue,
  detail,
  error,
  outputs,
  onLoadOutput,
  onClose,
}: {
  queue: QueueSummary
  detail: JobResponse | null
  error: string
  outputs: Record<number, OutputState>
  onLoadOutput: (commandID: number) => void
  onClose: () => void
}) {
  const dialogRef = useRef<HTMLElement>(null)
  const closeRef = useRef<HTMLButtonElement>(null)
  const initializedRuns = useRef(false)
  const [expandedRuns, setExpandedRuns] = useState<Set<number>>(() => new Set())

  useEffect(() => {
    if (!detail || initializedRuns.current || detail.runs.length === 0) return
    initializedRuns.current = true
    setExpandedRuns(new Set([detail.runs[detail.runs.length - 1].id]))
  }, [detail])

  useEffect(() => {
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const previousOverflow = document.body.style.overflow
    const background = Array.from(document.querySelectorAll<HTMLElement>('.topbar, .workspace, .notice'))
    const priorInert = background.map((element) => element.inert)
    document.body.style.overflow = 'hidden'
    background.forEach((element) => { element.inert = true })
    closeRef.current?.focus()

    const keydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        onClose()
        return
      }
      if (event.key !== 'Tab' || !dialogRef.current) return
      const focusable = Array.from(dialogRef.current.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ))
      if (focusable.length === 0) {
        event.preventDefault()
        return
      }
      const first = focusable[0]
      const last = focusable[focusable.length - 1]
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault()
        first.focus()
      }
    }
    document.addEventListener('keydown', keydown)
    return () => {
      document.removeEventListener('keydown', keydown)
      document.body.style.overflow = previousOverflow
      background.forEach((element, index) => { element.inert = priorInert[index] })
      previouslyFocused?.focus()
    }
  }, [onClose])

  return (
    <div className="drawer-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <aside ref={dialogRef} className="job-drawer" role="dialog" aria-modal="true" aria-labelledby="job-detail-title">
        <header className="drawer-header">
          <div>
            <p className="eyebrow">{queue.display_name} / job detail</p>
            <h2 id="job-detail-title">{detail ? fileName(detail.job.path) : 'Loading job…'}</h2>
          </div>
          <button ref={closeRef} className="close-button" onClick={onClose} aria-label="Close job detail">×</button>
        </header>
        <div className="drawer-body">
          {error && !detail ? (
            <EmptyState title="Could not load job" copy={error} compact />
          ) : detail === null ? (
            <DetailSkeleton />
          ) : (
            <>
              {error && <p className="inline-warning">Showing the last job detail: {error}</p>}
              <section className="job-summary">
                <div className="job-summary-title">
                  <StatusPill status={detail.job.status} />
                  <code>Job #{detail.job.id}</code>
                </div>
                <dl>
                  <Detail label="Watch" value={detail.job.watch_name} />
                  <Detail label="Attempts" value={`${detail.job.attempts} / ${detail.job.max_retries + 1}`} />
                  <Detail label="Created" value={formatDate(detail.job.created_at)} />
                  <Detail label="Updated" value={formatDate(detail.job.updated_at)} />
                  <Detail label="Available" value={formatDate(detail.job.available_at)} />
                  <Detail label="Started" value={formatDate(detail.job.started_at)} />
                  <Detail label="Finished" value={formatDate(detail.job.finished_at)} />
                </dl>
                <div className="path-block"><span>Source file</span><code>{detail.job.path}</code></div>
                <div className="path-block"><span>Fingerprint</span><code>{detail.job.fingerprint || '—'}</code></div>
                {detail.job.last_error && <div className="error-block"><strong>Last error</strong><pre>{detail.job.last_error}</pre></div>}
              </section>

              <section className="runs-section">
                <div className="section-label"><span>Attempts</span><b>{detail.runs.length}</b></div>
                {detail.runs.length === 0 ? (
                  <EmptyState title="Waiting for first attempt" copy="This job has not been claimed by a worker yet." compact />
                ) : detail.runs.map((run) => (
                  <details
                    className="run-card"
                    key={run.id}
                    open={expandedRuns.has(run.id)}
                    onToggle={(event) => {
                      const runID = run.id
                      const open = event.currentTarget.open
                      setExpandedRuns((current) => {
                        const next = new Set(current)
                        if (open) next.add(runID)
                        else next.delete(runID)
                        return next
                      })
                    }}
                  >
                    <summary>
                      <span><b>Attempt {run.attempt}</b><small>{formatDate(run.started_at)}</small></span>
                      <StatusPill status={run.status} />
                    </summary>
                    <div className="run-body">
                      {run.error && <div className="error-block"><strong>Attempt error</strong><pre>{run.error}</pre></div>}
                      {run.commands.map((command) => (
                        <CommandCard
                          key={command.id}
                          command={command}
                          outputState={outputs[command.id]}
                          onLoadOutput={() => onLoadOutput(command.id)}
                        />
                      ))}
                    </div>
                  </details>
                ))}
              </section>
            </>
          )}
        </div>
      </aside>
    </div>
  )
}

function CommandCard({ command, outputState, onLoadOutput }: { command: Command; outputState?: OutputState; onLoadOutput: () => void }) {
  const invocation = [command.program, ...command.args].map(shellToken).join(' ')
  return (
    <article className="command-card">
      <header>
        <div><span className="step-number">{command.sequence}</span><strong>{command.name || command.program}</strong></div>
        <StatusPill status={command.status} />
      </header>
      <pre className="invocation">{invocation}</pre>
      <dl>
        <Detail label="Exit" value={command.exit_code === undefined ? '—' : String(command.exit_code)} />
        <Detail label="Timeout" value={command.timeout} />
        <Detail label="Duration" value={duration(command.started_at, command.finished_at)} />
      </dl>
      {command.working_directory && <p className="working-dir" title={command.working_directory}>cwd · {command.working_directory}</p>}
      {command.error && <div className="error-block"><strong>Command error</strong><pre>{command.error}</pre></div>}
      <div className="output-heading">
        <span>Captured output · {formatBytes(command.stdout_bytes + command.stderr_bytes)}</span>
        {!outputState && <button onClick={onLoadOutput}>Load output</button>}
        {outputState?.state === 'loading' && <span>Loading…</span>}
        {outputState?.state === 'error' && <button onClick={onLoadOutput}>Retry</button>}
        {outputState?.state === 'ready' && <button onClick={onLoadOutput}>Reload output</button>}
      </div>
      {outputState?.state === 'error' && <p className="inline-warning">{outputState.message}</p>}
      {outputState?.state === 'ready' && (
        <div className="output-grid">
          <OutputBlock label="stdout" value={outputState.output.stdout} />
          <OutputBlock label="stderr" value={outputState.output.stderr} />
        </div>
      )}
    </article>
  )
}

function OutputBlock({ label, value }: { label: string; value: string }) {
  return <div className="output-block"><span>{label}</span><pre>{value || 'No output captured.'}</pre></div>
}

function Metric({ label, value, tone }: { label: string; value: number; tone: string }) {
  return <article className={`metric metric-${tone}`}><span>{label}</span><strong>{value.toLocaleString()}</strong><i /></article>
}

function CountButton({ label, value, active, onClick }: { label: string; value: number; active: boolean; onClick: () => void }) {
  return <button className={active ? 'active' : ''} aria-pressed={active} onClick={onClick}><span>{label}</span><strong>{value.toLocaleString()}</strong></button>
}

function StatusPill({ status }: { status: string }) {
  const normalized = status.toLowerCase()
  return <span className={`status status-${normalized}`}><i />{normalized}</span>
}

function Detail({ label, value }: { label: string; value: ReactNode }) {
  return <div><dt>{label}</dt><dd>{value}</dd></div>
}

function EmptyState({ title, copy, compact = false }: { title: string; copy: string; compact?: boolean }) {
  return <section className={`empty-state ${compact ? 'compact' : ''}`}><span aria-hidden="true">∅</span><h2>{title}</h2><p>{copy}</p></section>
}

function DashboardSkeleton() {
  return <div className="queue-workspace"><div className="skeleton skeleton-rail" /><div className="skeleton skeleton-panel" /></div>
}

function TableSkeleton() {
  return <div className="table-skeleton" aria-label="Loading"><i /><i /><i /><i /></div>
}

function DetailSkeleton() {
  return <div className="detail-skeleton"><i /><i /><i /></div>
}

function fileName(path: string) {
  return path.split(/[\\/]/).pop() || path
}

function formatClock(value: string) {
  return new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value))
}

function formatDate(value?: string) {
  if (!value) return '—'
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value))
}

function relativeTime(value: string) {
  const seconds = Math.round((new Date(value).getTime() - Date.now()) / 1000)
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
  if (Math.abs(seconds) < 60) return formatter.format(seconds, 'second')
  const minutes = Math.round(seconds / 60)
  if (Math.abs(minutes) < 60) return formatter.format(minutes, 'minute')
  const hours = Math.round(minutes / 60)
  if (Math.abs(hours) < 24) return formatter.format(hours, 'hour')
  return formatter.format(Math.round(hours / 24), 'day')
}

function duration(start: string, finish?: string) {
  const milliseconds = (finish ? new Date(finish).getTime() : Date.now()) - new Date(start).getTime()
  if (milliseconds < 1000) return `${Math.max(0, milliseconds)}ms`
  const seconds = Math.floor(milliseconds / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`
}

function shellToken(value: string) {
  return /^[a-zA-Z0-9_./:@%+=,-]+$/.test(value) ? value : JSON.stringify(value)
}
