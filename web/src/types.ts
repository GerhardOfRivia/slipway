export type JobStatus = 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED'
export type InstanceState = 'running' | 'stopping' | 'exited' | 'failed'
export type CommandStatus = 'RUNNING' | 'SUCCEEDED' | 'FAILED'

export interface QueueCounts {
  queued: number
  running: number
  succeeded: number
  failed: number
  total: number
}

export interface Instance {
  id: string
  name: string
  config_path: string
  config_hash: string
  database_path: string
  state: InstanceState
  created_at: string
  started_at: string
  finished_at?: string
  error?: string
}

export interface QueueSummary {
  id: string
  display_name: string
  config_path: string
  config_hash: string
  database_path: string
  watches: string[]
  database_state: 'ready' | 'missing' | 'unavailable'
  counts: QueueCounts
  active_instance?: Instance
  error?: string
}

export interface QueuesResponse {
  queues: QueueSummary[]
  generated_at: string
}

export interface InstancesResponse {
  instances: Instance[]
  generated_at: string
}

export interface Job {
  id: number
  watch_name: string
  path: string
  fingerprint?: string
  status: JobStatus
  attempts: number
  max_retries: number
  available_at: string
  last_error?: string
  created_at: string
  updated_at: string
  started_at?: string
  finished_at?: string
}

export interface JobsResponse {
  jobs: Job[]
  limit: number
  offset: number
  has_more: boolean
  generated_at: string
}

export interface Command {
  id: number
  sequence: number
  name: string
  program: string
  args: string[]
  working_directory?: string
  timeout: string
  status: CommandStatus
  exit_code?: number
  error?: string
  started_at: string
  finished_at?: string
  stdout_bytes: number
  stderr_bytes: number
}

export interface Run {
  id: number
  attempt: number
  status: JobStatus
  error?: string
  started_at: string
  finished_at?: string
  commands: Command[]
}

export interface JobResponse {
  job: Job
  runs: Run[]
}

export interface CommandOutput {
  command_id: number
  stdout: string
  stderr: string
}

export interface APIErrorBody {
  error?: {
    code?: string
    message?: string
  }
}
