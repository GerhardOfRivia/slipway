import type {
  APIErrorBody,
  CommandOutput,
  InstancesResponse,
  JobResponse,
  JobsResponse,
  QueuesResponse,
} from './types'

export class APIError extends Error {
  status: number
  code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'APIError'
    this.status = status
    this.code = code
  }
}

async function request<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  headers.set('Authorization', `Bearer ${token}`)
  headers.set('Accept', 'application/json')
  if (init?.method === 'POST') {
    headers.set('Content-Type', 'application/json')
    headers.set('X-slipway-Web', '1')
  }

  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    let body: APIErrorBody = {}
    try {
      body = (await response.json()) as APIErrorBody
    } catch {
      // Preserve the HTTP fallback below when a proxy returned non-JSON.
    }
    throw new APIError(
      response.status,
      body.error?.code ?? 'request_failed',
      body.error?.message ?? `Request failed with status ${response.status}`,
    )
  }
  return (await response.json()) as T
}

export const api = {
  queues: (token: string, signal?: AbortSignal) => request<QueuesResponse>('/api/v1/queues', token, { signal }),
  instances: (token: string, signal?: AbortSignal) => request<InstancesResponse>('/api/v1/instances?all=true', token, { signal }),
  jobs: (token: string, queueID: string, status: string, watch: string, offset: number, signal?: AbortSignal) => {
    const query = new URLSearchParams({ limit: '50', offset: String(offset) })
    if (status) query.set('status', status)
    if (watch) query.set('watch', watch)
    return request<JobsResponse>(`/api/v1/queues/${queueID}/jobs?${query}`, token, { signal })
  },
  job: (token: string, queueID: string, jobID: number, signal?: AbortSignal) =>
    request<JobResponse>(`/api/v1/queues/${queueID}/jobs/${jobID}`, token, { signal }),
  output: (token: string, queueID: string, commandID: number, signal?: AbortSignal) =>
    request<CommandOutput>(`/api/v1/queues/${queueID}/commands/${commandID}/output`, token, { signal }),
  start: (token: string, queueID: string) =>
    request(`/api/v1/queues/${queueID}/start`, token, { method: 'POST', body: '{}' }),
  stop: (token: string, instanceID: string) =>
    request(`/api/v1/instances/${instanceID}/stop`, token, { method: 'POST', body: '{}' }),
}
