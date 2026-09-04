# slipway

slipway is a small, durable file-triggered job runner. The long-lived `slipwayd`
daemon manages slipway instances. Each instance watches one or more directories,
waits for matching files to stop changing, stores each file as a job in SQLite,
and executes its command pipeline. The `slipway` command can run configs in the
foreground through the daemon or directly, control detached daemon-managed
instances, and inspect durable queue and execution history.

written and designed with help from openai's (5.6 sol)

## quick install

```bash
curl -fsSL https://raw.githubusercontent.com/GerhardOfRivia/slipway/refs/heads/main/install.sh | sh
```

slipway supports Linux on AMD64 and ARM64. The installer places both `slipway`
and `slipwayd` in the selected install directory.

## getting started

Run a config in the foreground:

```bash
slipway run --config csv_pipeline.yaml
```

`run` first checks the selected control socket. If `slipwayd` is reachable, it
starts daemon-managed instances, streams their logs to standard output, and waits
for them. If no daemon is listening, it logs that it is running daemonless and
runs the selected configs in the current process instead. It accepts one YAML
file or a directory of YAML files and runs every selected config concurrently.
Press Ctrl-C to stop all selected configs gracefully, including acknowledged
daemon-managed instances. `--rm` removes daemon-managed instances from
slipwayd after they exit instead of retaining them in instance history; it is a
no-op during daemonless fallback. For one selected config, `--name` sets its
daemon instance name or its daemonless log label. `--socket` uses the same
explicit, environment, and per-user resolution as other daemon commands.
Errors such as denied socket access or a daemon rejecting a config do not
trigger daemonless execution.

To manage an instance in the background, first start the daemon as the user that
should run the configured programs:

```bash
slipwayd
```

Then use the daemon-backed lifecycle commands from another terminal:

```bash
slipway start --config csv_pipeline.yaml --name csv-pipeline
slipway ps
slipway stop csv-pipeline
```

`start --config` also accepts a directory and creates one detached instance for
each discovered YAML file. `--name` may only be used when the selection resolves
to one config. Daemon-backed `run` instances appear in `ps`; daemonless fallback
processes do not and must be stopped with Ctrl-C or SIGTERM. Do not run a config
daemonless while the same queue database is active in `slipwayd` or another
`slipway run` process.

## checking configuration

Load and validate configuration before running it locally or starting a managed
instance, and show every watch's sequential command pipeline:

```bash
slipway check --config csv_pipeline.yaml
slipway check --raw --config csv_pipeline.yaml
```

`check` uses the same file, directory, `SLIPWAY_CONFIG`, and default discovery
rules as the other config-aware commands. It does not contact the daemon or
open the queue database. Program paths in the display reflect config-relative
path resolution, reusable `values` are expanded, and job-dependent templates
such as `{{file}}` remain unexpanded. By default, each invocation is displayed
as a readable shell-like command line, with spaces and shell syntax safely
single-quoted. This is only a presentation format: slipway still executes the
program and argument vector directly, without a shell. `check --raw` selects
the previous, authoritative representation consisting of a quoted program
followed by its JSON argument array. Pipeline steps run in numbered order and
are not shell pipes. Commands with an output file also show their configured
output path.

## generating pipeline configuration

`parse` turns an already shell-tokenized command into a pipeline YAML fragment
without executing it. Use `--` to separate slipway's options from the command:

```bash
slipway parse -- docker run --rm --gpus all \
  --mount 'type=bind,source={{dir}},target=/input,readonly' \
  --env 'INPUT={{basename}}' \
  nvidia/cuda:12.8.1-base-ubuntu24.04 nvidia-smi --query-gpu=name
```

```yaml
pipeline:
  - name: docker-run
    executor: docker
    image: nvidia/cuda:12.8.1-base-ubuntu24.04
    container_args:
      - --rm
      - --gpus
      - all
    mounts:
      - source: '{{dir}}'
        target: /input
        options:
          - readonly
    container_env:
      INPUT: '{{basename}}'
    command: nvidia-smi
    command_args:
      - --query-gpu=name
```

Paste the fragment under a watch, adjusting its indentation to match that
watch. `--name` overrides the generated step name. For direct Docker and Podman
`run` commands, including their `container run` aliases, `parse` separates
runtime options, the image, and the container command. Ordinary pre-image
runtime options become `container_args`; explicit `KEY=value` environment
options become `container_env`; representable bind mounts become `mounts`; and
tokens after the image become `command` and `command_args`. If the first
post-image token begins with `-`, `parse` leaves `command` unset and puts the
whole post-image tail in `command_args` for the image's default entrypoint.

Conversion is conservative. When an environment or mount form cannot be
represented structurally—such as `--env HOME`, duplicate or file-sourced
environment values, a named volume, or an advanced short-form `-v` mount—
`parse` leaves every option in that group verbatim in `container_args`. When an
option makes the image boundary ambiguous, such as an unknown option without an
attached value, `parse` writes a warning to stderr and emits the entire
invocation in the lossless raw `args` form. Non-`run` runtime commands and
Apptainer invocations also remain raw.
Other executables generate a normal `command` entry with `program` and `args`
fields.

## managed instances

The daemon exposes its lifecycle API over a local Unix socket:

```text
slipwayd [--socket path] [--config path] [--web-listen address] [--log-level level]
slipway run [--rm] [--config file-or-directory] [--name name] [--socket path]
slipway start --config file-or-directory [--name name] [--socket path]
slipway ps [--all] [--socket path]
slipway stop [--socket path] id-or-name [id-or-name ...]
```

When the daemon is reachable, `run` creates one or more attached instances and
streams their logs until they finish. With `--rm`, each attached instance is
removed from the daemon registry after it exits, including when it fails; its
durable queue is preserved. Disconnecting the client does not stop or remove a
live instance, but the daemon still removes it when it eventually exits.
`start` creates detached instances. `ps` lists running instances;
`ps --all` also shows the 100 most recent stopped and failed instances from the
current daemon lifetime. `stop` accepts any mixture of instance IDs and names.
`start`, `ps`, and `stop` require a running daemon; `run` falls back as described
above only when no daemon is listening.

Every instance log record includes both a unique `instance_id` for that run and
a SHA-256 `config_hash` of the effective loaded configuration. The config hash
stays the same across runs when the effective settings are unchanged, making
configuration changes visible when comparing logs.

The socket used by each daemon-backed command is selected in this order:

1. Its explicit `--socket` value.
2. `SLIPWAY_SOCKET`, when set.
3. `$XDG_RUNTIME_DIR/slipway/slipway.sock`, when `XDG_RUNTIME_DIR` is set.
4. `slipway/slipway.sock` beneath the operating system's per-user cache directory.
5. A UID-specific directory beneath the system temporary directory when no
   user cache directory is available.

All clients must resolve the same socket as the daemon. Socket access is
equivalent to permission to start configured programs as the daemon user. Keep
it private and prefer one daemon per user. Do not expose the socket to
untrusted users.

The MVP instance registry is held in memory. Active entries and the 100 most
recent terminal entries are retained, except for instances started by
`run --rm`; older terminal entries are evicted. The registry is lost when the
daemon exits, while queue databases remain durable.
Configs supplied to `slipwayd` with `--config`, or through `SLIPWAY_CONFIG`, are
bootstrapped again after the daemon restarts; instances created with `start`
must otherwise be submitted again.

`slipwayd --config path` starts the control daemon and immediately creates
detached instances from that file or directory. With neither `--config` nor
`SLIPWAY_CONFIG`, the daemon starts with no instances and waits for lifecycle
commands. A directory is scanned non-recursively for lowercase `*.yaml` and
`*.yml` files in filename order.

Each config runs independently with its own watches, worker pool, retry
settings, reusable values, and SQLite queue. Concurrent instances must use
distinct database paths. Relative database, watch, working-directory, and
structured bind-mount source paths are resolved from the directory containing
that YAML file, including relative paths inserted through `values`.
Relative program paths that contain a slash are resolved the same way; bare
program names still use `PATH` lookup. Relative local structured Apptainer image
names and paths are resolved from the config directory; absolute paths and paths
based on `{{file}}` or `{{dir}}` retain those meanings. Transport references
containing `://`, such as `docker://` and `library://`, and `docker-daemon:`
references remain unchanged. Relative paths following `docker-archive:` or
`oci-archive:` are resolved from the config directory.
Relative command output paths are resolved against the command's expanded
working directory. If no working directory is configured, they use the current
directory of the `slipway` or `slipwayd` process executing the config.

For databases that do not exist yet beneath the same underlying directory,
slipway conservatively treats case-folded or Unicode-normalized path spellings as
aliases. Pre-create distinct databases if a case-sensitive filesystem must use
names that differ only by case or Unicode normalization.

## optional web dashboard

`slipwayd` can serve an embedded dashboard without a second service. The web
listener is disabled by default, and a loopback address is the recommended
setting:

```bash
slipwayd --config ~/.local/slipway.d --web-listen 127.0.0.1:8080
```

`SLIPWAY_WEB_LISTEN` supplies the same setting when the flag is omitted. Open
<http://127.0.0.1:8080>, then paste the access token from the token file named
in the daemon's startup log. The daemon creates a token beside its private
control socket using the `.web-token` suffix and mode `0600`, replaces it at
startup, and removes it after a clean shutdown. For the packaged system
service, read it with:

```bash
sudo cat /run/slipway/slipway.sock.web-token
```

The dashboard treats queues and instances separately. It shows all queues
known during the current daemon lifetime, including queues whose instances
have stopped, with counts for queued, running, succeeded, and failed jobs. It
also shows job attempts and command metadata, loads captured output only when
requested, and can restart a known queue or stop an active instance. The web
API never accepts arbitrary config or database paths.

Every API request requires the bearer token. Keep the token private because it
authorizes dashboard reads and start/stop actions as the daemon user. The
listener also accepts an explicit wildcard address such as `0.0.0.0:8080` (or
`[::]:8080`) and logs a warning when one is used. Connect to a wildcard listener
with a literal IP address; arbitrary HTTP `Host` names remain rejected. A
wildcard bind exposes the dashboard on every available interface, and bearer
authentication does not encrypt its HTTP traffic. Prefer loopback with an SSH
tunnel. If direct network access is necessary, restrict the port with a
firewall and protect the token and network path. Concrete non-loopback bind
addresses remain rejected; use the wildcard form to opt in explicitly.

## queue and history inspection

The existing inspection commands remain job-oriented and do not use the daemon
socket:

```bash
slipway version
slipway status
slipway queue
slipway jobs
slipway jobs --status failed
slipway jobs --watch incoming
slipway job 42
slipway logs 42
```

`status` prints aggregate job counts. `queue` shows currently queued and
running jobs. `job` shows every run and command, while `logs` prints captured
stdout and stderr for each command; it does not print instance or daemon logs.
When multiple configs are loaded, status and job listings identify their source
config. If a numeric job ID exists in multiple databases, select its config
explicitly:

```bash
slipway job --config ~/.local/slipway.d/incoming.yaml 42
slipway logs --config ~/.local/slipway.d/incoming.yaml 42
```

Inspection commands open existing queue databases read-only; they report a
missing database instead of creating an empty one and can be used while the
daemon is stopped. Use `--config path` or `SLIPWAY_CONFIG` to select one YAML file
or a directory. With neither set, inspection commands load every `*.yaml` and
`*.yml` file, non-recursively, from both:

```text
/etc/slipway.d
~/.local/slipway.d
```

System configs are loaded first, with filenames sorted within each directory.
If neither directory contains a config, `./slipway.yaml` is used as a
backwards-compatible fallback. If that is also absent, slipway exits with an error
listing every location it searched.

## configuration

```yaml
queue:
  workers: 2
  max_retries: 3
  retry_delay: 10s

database:
  path: ./slipway.db

values:
  shared_dir: /srv/slipway

watches:
  - name: incoming
    path: ./incoming
    recursive: true
    process_existing: true
    reprocess_on_change: false
    include:
      - "*.csv"
    exclude:
      - "*.partial"
    settle_for: 3s

    pipeline:
      - name: process
        executor: command
        program: /usr/local/bin/process-file
        args:
          - "--input"
          - "{{file}}"
          - "--job-id"
          - "{{job_id}}"
          - "--shared-dir"
          - "{{shared_dir}}"
        timeout: 15m
        working_directory: "{{shared_dir}}"
        output: "{{shared_dir}}/{{stem}}.json"
        env:
          SLIPWAY_INPUT: "{{basename}}"
```

Durations use Go syntax such as `250ms`, `10s`, or `15m`. The defaults are one
worker per CPU, a `10s` retry delay, a `1s` settle period, and `./slipway.db`.
`max_retries` counts retries after the first attempt.

Each pipeline entry may set `executor` to `command`, `docker`, `podman`, or
`apptainer`. Omitting `executor` is equivalent to `executor: command`. Command
entries require `program`, which names the executable to run. The other
executor kinds run their same-named CLI from `PATH` by default; an optional
`program` overrides that binary, for example to select `/usr/local/bin/podman`.
For a container entry, `program` always names the host-side runtime CLI, not the
program inside the container.

Container entries can describe the invocation with structured fields:

```yaml
      - name: process-in-container
        executor: docker
        image: ghcr.io/example/processor:1.2.3
        container_args:
          - "--rm"
          - "--network=none"
        mounts:
          - source: "{{dir}}"
            target: /input
            options:
              - ro
              - bind-propagation=rslave
        container_env:
          SLIPWAY_INPUT: "{{basename}}"
        command: /app/process-file
        command_args:
          - "--input"
          - "/input/{{basename}}"
```

`image` is required in structured mode. Every mount requires a host `source`
and a container `target` that is absolute after template expansion. A mount's
optional `options` list supplies ordered `--mount` fields such as `ro`,
`readonly`, `bind-propagation=rslave`, `relabel=shared`, or
`bind-recursive=disabled`. Each list item is one complete field, including any
`=value` portion. These options are runtime-specific and are passed through
without a name or value allowlist, so the selected Docker, Podman, or Apptainer
version validates them. Within `options`, slipway does not normalize,
deduplicate, or resolve contradictory access modes; listed order is preserved.
`type` and source or target aliases cannot be repeated in `options`; use the
structured fields for those values. Both config-local and per-job templates are
expanded in mount options. Existing configurations may still use
`read_only: true`; it is accepted as an input-only compatibility alias and
normalized to a leading `ro` option.

`container_env` sets variables inside the container, but its values are
materialized in the runtime argv and command history and should not be treated
as a secret store. `container_args` holds additional `run` or `exec` action
options, without the action itself. Runtime-global options that must precede
the action require the raw container `args` form or a wrapper selected with
`program`. A standalone `--` is rejected because it would stop the runtime from
parsing the generated mount and environment options.

`command` is the optional first post-image command token, and each
`command_args` entry follows it. When parsing Docker or Podman, a first token
beginning with `-` instead starts `command_args`, leaving `command` unset; the
resulting runtime argv is unchanged. Apptainer uses `command` as the program for
`exec`; Docker and Podman apply the image's normal `ENTRYPOINT` and `CMD`
semantics. slipway preserves the order of mounts, mount options,
`container_args`, and `command_args`, and sorts `container_env` keys for
deterministic output. After template expansion, each mount becomes two argv
entries: `--mount` followed by a CSV-encoded
`type=bind,source=...,target=...[,<option>...]` value. CSV encoding keeps
commas and quotes in expanded mount sources, targets, and option values from
becoming separate fields in the runtime's mount parser. Each `container_env`
entry similarly becomes `--env` plus one argument representing `KEY=value`;
for Apptainer that argument is encoded for its CSV-based parser so delimiters
cannot create extra environment entries.

For Docker and Podman, structured fields produce arguments in this order:

```text
run <container_args> <mount options> <environment options> <image> [<command>] <command_args>
```

Apptainer uses `exec` when `command` is set and `run` when it is omitted:

```text
exec|run --no-eval <container_args> <mount options> <environment options> <image> [<command>] <command_args>
```

slipway adds `--no-eval` to structured Apptainer invocations to disable
Apptainer's normal startup evaluation of environment values and OCI command
tokens. slipway does not inspect or alter image metadata, and does not add
cleanup, networking, user, container working-directory, or environment-isolation
options implicitly. Put options such as `--rm`, `--network`, `--workdir`, or
Apptainer's `--cleanenv` in `container_args`.

The original raw container-argument form remains supported for existing
configurations:

```yaml
        executor: docker
        args: ["run", "--rm", "example/image:latest", "process", "{{file}}"]
```

In this form, slipway passes `args` to the runtime CLI unchanged, so they must
include the runtime subcommand, image, and all runtime-specific options.
Nonempty raw `args` cannot be combined with structured container fields.
Structured container fields cannot be used with `executor: command`.
`slipway parse` generates this form as a safe fallback when a Docker or Podman
`run` invocation cannot be represented structurally. Long-form bind mounts can
retain additional fields in `options`. Basic path-based
`-v SOURCE:TARGET[:ro|rw]` bind mounts can also be converted, but advanced
short-form modes remain in raw `container_args` because their long-form
equivalents differ between runtimes. The generated invocation for a converted
basic `-v` mount uses `--mount`; unlike Docker's `-v`, this requires the host
source to exist when the pipeline runs. A converted relative source is resolved
relative to the YAML file after the fragment is pasted, rather than relative to
the directory where `parse` was invoked.

The pipeline `env` and `working_directory` settings configure the host-side
runtime CLI process. Use `container_env` for explicit container variables and
`container_args` for a container working directory. Docker and Podman containers
do not automatically inherit these host-side values, but Apptainer imports much
of its host environment by default; add `--cleanenv` to minimize inherited host
environment. slipway considers the step complete when the runtime CLI exits, so
use a foreground invocation when queue completion must mean that the container
workload has finished.

Include and exclude patterns without a slash match the basename at any watched
depth. Patterns with a slash match the path relative to the watch root and may
use `**` for zero or more directories. Excludes take precedence. With
`reprocess_on_change: false`, a watch/path pair is processed once; when true, a
new size/modification-time fingerprint creates another persistent job.

Top-level `values` provide reusable, config-local strings for pipelines:

```yaml
values:
  media_root: /srv/media
  incoming_media: "{{media_root}}/incoming"
  container_data: /data

watches:
  - name: media
    path: ./incoming
    pipeline:
      - name: inspect
        executor: docker
        image: example/inspector:latest
        mounts:
          - source: "{{incoming_media}}"
            target: "{{container_data}}"
        command_args:
          - "{{container_data}}/{{basename}}"
```

Value keys are case-sensitive and must start with a letter or underscore and
otherwise contain only letters, digits, and underscores. Values may reference
other declared values; reference cycles are rejected. The built-in names
`file`, `dir`, `basename`, `stem`, `ext`, and `job_id` are reserved. Built-in
templates inside a value remain available for per-job expansion, while an
undeclared `{{name}}` remains unexpanded for compatibility with downstream
templating tools. Normal field-specific path resolution still applies after
that expansion. Values are expanded in the pipeline fields listed below,
before semantic validation and config-relative path resolution.

Values are ordinary configuration data, not secrets. Their expanded contents
may appear in process arguments, environment values, logs, and persisted
command history.

The following templates are expanded independently in ordinary command
arguments, structured container images, mount sources, targets, and options,
container arguments, container commands and command arguments, working
directories, output paths, and host or container environment values:

| Template | Value |
| --- | --- |
| `{{file}}` | Absolute file path |
| `{{dir}}` | Parent directory |
| `{{basename}}` | Filename with extension |
| `{{stem}}` | Filename without its final extension |
| `{{ext}}` | Final extension, including the dot |
| `{{job_id}}` | SQLite job ID |

slipway passes the selected executable and argument slice directly to Go's
process execution API; it never constructs a shell command. This applies both
to ordinary commands and to the Docker, Podman, and Apptainer CLIs. Spaces,
wildcard characters, semicolons, `$()`, and other shell-looking text in
filenames remain literal argument data when handed to the selected executable.
Host-side `env` entries override the runner process environment for that command.
Structured Apptainer invocations include `--no-eval` to disable its normal
startup evaluation. Raw Apptainer `args` remain unchanged, so add `--no-eval`
there when needed. A container image's own entry point or runscript may still
interpret the arguments it receives.

Stopping or timing out a Docker or Podman step terminates the runtime CLI
process group, but a container managed by a separate runtime daemon may outlive
that CLI. The container runtime's `run --rm` removes the container after it
eventually exits; it does not stop a live container whose CLI was killed. When
cancellation must extend to the container, use a runtime-aware wrapper or
container-side deadline that reliably stops it. Bind-mount sources must be
accessible to the selected runtime. slipway does not create or transfer mount
sources itself, though a runtime-specific option may ask the runtime to create
one. In particular, a
remote daemon or VM-backed runtime may not see paths from the daemon host.

The optional `output` setting saves the command's complete stdout stream to a
file while retaining the usual captured stdout for `slipway logs`; stderr remains
captured separately and is not written to that file. The parent directory must
already exist. Each attempt creates or truncates the file before starting the
command, so use job-specific templates when commands may run concurrently and
expect retries to replace partial output from an earlier attempt.

Symbolic-link files are ignored, and recursive watches do not traverse
symbolic-link directories. Watched directories are nevertheless a
trust boundary: another process with write access can replace a checked file
before a configured command opens it, so do not watch directories writable by
untrusted users.

Each instance limits its registered watch set to 4,096 directories, limits
simultaneous file-settling work to 1,024 paths, and keeps at most 4,096 recently
delivered fingerprints in memory. Directory discovery reads bounded batches.
Reaching a work limit fails that instance explicitly instead of allowing an
unbounded backlog; SQLite remains the durable source of fingerprint
deduplication. Removing or replacing a configured root—or renaming an ancestor
so the configured path no longer reaches the original directory—also fails the
instance; recreate the expected path and start the instance again. On Linux,
slipway also reconciles its bounded directory registry with the kernel watch list
once per second. It prunes vanished subtrees and fails a persistent missing or
incompatible watch instead of leaving an apparently running but incomplete
instance; a one-interval grace lets queued rename/remove events reconcile
first.

Captured stdout and stderr are each limited to their first 1 MiB per command.
When output exceeds that limit, the stored stream ends with a truncation marker
instead of allowing one command to consume unbounded runner memory or database
space. This history limit does not truncate stdout written through a command's
`output` setting.

## delivery and recovery

SQLite is the authoritative queue. Claiming a job and creating its run record
happen atomically. Failed attempts become eligible again through a persisted
`available_at` timestamp, so workers do not sleep for the retry delay. On
startup, each runner marks unfinished run/command history as interrupted and
immediately requeues jobs left `RUNNING`.

These choices provide **at-least-once execution**. A process may have completed
just before a host or runner crash but be executed again after recovery, so
pipelines should be idempotent. Use one database per config. Stopping a managed
instance or interrupting a foreground run cancels its active commands, persists
their failed attempts when possible, stops its watcher, and waits for its workers
to exit. On Linux, cancellation kills the command's process group,
which includes ordinary descendants; a descendant that deliberately creates a
new session or process group is outside that guarantee. Output-pipe cleanup is
time-bounded so an escaped descendant cannot indefinitely block shutdown merely
by retaining an inherited descriptor. SIGINT and SIGTERM stop foreground runs
and every daemon-managed instance gracefully.

## systemd

An example system service is provided at
[`contrib/systemd/slipway.service`](contrib/systemd/slipway.service). It expects:

- the `slipwayd` daemon binary at `/usr/local/bin/slipwayd`
- the `slipway` client binary at `/usr/local/bin/slipway`
- a dedicated `slipway` user and group
- one or more configs in `/etc/slipway.d`
- writable queue data under `/var/lib/slipway`
- a private control socket at `/run/slipway/slipway.sock`

Install the binaries and unit, create the service account, and add a config:

```bash
go build -o slipway ./cmd/slipway
go build -o slipwayd ./cmd/slipwayd
sudo install -Dm0755 slipway /usr/local/bin/slipway
sudo install -Dm0755 slipwayd /usr/local/bin/slipwayd
sudo install -Dm0644 contrib/systemd/slipway.service /etc/systemd/system/slipway.service

# Skip useradd if the account already exists.
sudo useradd --system --home-dir /var/lib/slipway --shell /usr/sbin/nologin slipway
sudo install -d -m0755 /etc/slipway.d
sudo install -o root -g slipway -m0640 ./my-slipway.yaml /etc/slipway.d/incoming.yaml
```

Replace `./my-slipway.yaml` with the config you created from the example below.

For a system service, use an absolute database path such as
`/var/lib/slipway/incoming.db`. Every managed pipeline runs as the `slipway` account,
so ensure that account can traverse each watch directory, read input files,
execute pipeline programs, and write any pipeline outputs. systemd creates
`/var/lib/slipway` through `StateDirectory=slipway` and the private socket directory
`/run/slipway` through `RuntimeDirectory=slipway`.

Then load and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now slipway
sudo systemctl status slipway
sudo journalctl -u slipway -f
sudo slipway ps --socket /run/slipway/slipway.sock
```

The unit bootstraps `/etc/slipway.d`, sends SIGTERM for graceful shutdown, restarts
after failures, and logs to the journal. The in-memory instance registry is
rebuilt from that bootstrap directory after each restart. To use another
configuration file, directory, or socket, create `/etc/default/slipway` containing:

```bash
SLIPWAY_CONFIG=/path/to/slipway.d
SLIPWAY_SOCKET=/run/slipway/slipway.sock
# Optional; the dashboard is disabled when this is unset.
SLIPWAY_WEB_LISTEN=127.0.0.1:8080
```

The packaged system socket is private to root and the `slipway` service account.
If you deliberately relax its ownership or permissions, every user who can
connect to it can make the service execute configured programs as `slipway`. A
per-user daemon is recommended for interactive and user-owned workloads.

## architecture

- `internal/config`: discovery, strict YAML decoding, defaults, and validation
- `internal/watcher`: Linux filesystem events, recursive discovery, matching,
  and settling
- `internal/queue`: SQLite jobs, runs, command history, claims, and recovery
- `internal/executor`: backend interface and safe local process executor
- `internal/worker`: concurrent consumers and sequential pipeline execution
- `internal/daemon`: one instance's component lifecycle and discovery-to-queue
  wiring
- `internal/control`: instance supervision, Unix-socket API, and client
- `internal/webui`: optional token-protected dashboard API and embedded frontend
- `internal/cli`: command parsing for the `slipway` client and `slipwayd` daemon

The worker depends on the executor interface rather than the local
implementation. Container executor kinds select their runtime CLI and reuse
the same host-process capture and history mechanics; the CLI's exit remains the
step-completion boundary. The interface leaves room for future backends such as
Slurm or Flux.

## development

### isolated temporary Go toolchain

If Go is not installed, or you want to keep the toolchain and its caches out of
your home directory, you can download Go into a temporary directory. This
example uses Go 1.27.0 for Linux on AMD64; choose another published version or
supported Linux architecture from [go.dev/dl](https://go.dev/dl/) when needed.

```bash
export SLIPWAY_GO_VERSION=1.27.0
export SLIPWAY_GO_OS=linux
export SLIPWAY_GO_ARCH=amd64
export SLIPWAY_GO_DIR=$(pwd)/.go

mkdir -p "$SLIPWAY_GO_DIR/toolchain"
curl -fL \
  "https://go.dev/dl/go${SLIPWAY_GO_VERSION}.${SLIPWAY_GO_OS}-${SLIPWAY_GO_ARCH}.tar.gz" \
  -o "$SLIPWAY_GO_DIR/go.tar.gz"
tar -xzf "$SLIPWAY_GO_DIR/go.tar.gz" \
  -C "$SLIPWAY_GO_DIR/toolchain" \
  --strip-components=1

export PATH="$SLIPWAY_GO_DIR/toolchain/bin:$PATH"
export GOPATH="$SLIPWAY_GO_DIR/gopath"
export GOMODCACHE="$SLIPWAY_GO_DIR/modcache"
export GOCACHE="$SLIPWAY_GO_DIR/buildcache"

go version
go mod download
```

`PATH` selects the temporary Go binary, `GOPATH` isolates Go's workspace,
`GOMODCACHE` isolates downloaded modules, and `GOCACHE` isolates compiled build
artifacts. You do not need to set `GOROOT`; the Go binary discovers its own
toolchain directory. These exports affect only the current shell.

For Linux on ARM64, set `SLIPWAY_GO_ARCH=arm64`.

To remove the temporary toolchain and caches when you are finished:

```bash
if [ -n "${SLIPWAY_GO_DIR:-}" ] && [ -d "$SLIPWAY_GO_DIR" ]; then
  rm -rf -- "$SLIPWAY_GO_DIR"
fi
unset SLIPWAY_GO_DIR SLIPWAY_GO_VERSION SLIPWAY_GO_OS SLIPWAY_GO_ARCH
unset GOPATH GOMODCACHE GOCACHE
hash -r
```

### checks

```bash
go fmt ./...
go vet ./...
go test ./...
```

The production dashboard assets are committed beneath `internal/webui/dist`
so normal Go builds need no Node.js installation. After changing files under
`web`, rebuild those assets with Node.js 22:

```bash
(cd web && npm ci && npm run build)
```

or

```bash
make web
```

### build

```bash
make
./bin/slipway version
./bin/slipwayd version
```

![icon](icon.png)
