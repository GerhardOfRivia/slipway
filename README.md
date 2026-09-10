# slipway

slipway is a small, durable file-triggered job runner. It watches directories,
waits for matching files to stop changing, queues them in SQLite, and runs a
command pipeline for each file. Use `slipway` to run and inspect jobs, and
`slipwayd` to manage background instances and an optional web dashboard.

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
slipway test csv_pipeline.yaml
```

`test` takes a config path, runs the pipeline in the foreground, and owns
its standalone queue database. No instance name is needed. It does not contact
`slipwayd`. Press Ctrl-C to stop gracefully.

To manage an instance in the background, first start the daemon as the user that
should run the configured programs:

```bash
slipwayd
```

Then use the daemon-backed lifecycle commands from another terminal:

```bash
slipway start csv_pipeline.yaml csv-pipeline
slipway ps
slipway stop csv-pipeline
```

`test` requires exactly one selected config. `start` derives a name from the
config filename when one is omitted. The daemon assigns each managed
instance its own queue database; only standalone runs use `database.path`.
Only daemon-managed instances appear in `ps`.

Config paths are always explicit; there is no automatic discovery or
`SLIPWAY_CONFIG` fallback. Options may appear before or after positional
arguments. Use `--` for paths beginning with a dash, as in
`slipway check -- -pipeline.yaml`.

## checking configuration

Validate a config and display each watch's pipeline without running it:

```bash
slipway check csv_pipeline.yaml
slipway check --raw csv_pipeline.yaml
```

`check` accepts a YAML file or directory and never contacts the daemon or opens
the database. It resolves config-relative paths and reusable `values`, leaving
per-job templates such as `{{file}}` unexpanded. Steps appear in execution order
as shell-like command lines, with output paths when configured. This display
does not imply shell execution; use `--raw` to see each quoted program and JSON
argument array.

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

Paste the fragment under a watch, adjusting its indentation. `--name` overrides
the generated step name. Docker and Podman `run` commands, including
`container run`, become structured fields. Unsupported environment or mount
forms stay in `container_args`; an ambiguous image boundary produces a warning
and preserves the whole invocation in raw `args`. Other runtime commands,
including Apptainer, remain raw. Ordinary executables become `command` entries
with `program` and `args`.

Basic `-v SOURCE:TARGET[:ro|rw]` bind mounts become `--mount`, which requires the
host source to exist. Converted relative sources resolve from the YAML file's
directory, not the directory where `parse` ran.

`parse`, `check`, and instance startup warn about Docker's interactive TTY flags
(`-it`, or `--interactive --tty`) and the invalid `--it` spelling. slipway provides
no interactive stdin or TTY; remove these flags for unattended jobs. Warnings
leave the supplied arguments unchanged.

## managed instances

The daemon exposes its lifecycle API over a local Unix socket:

```text
slipwayd [--state-dir path] [--socket path] [--web-listen address] [--log-level level]
slipway test <config>
slipway start <config-or-instance> [name] [--socket path]
slipway ps [--all] [--socket path]
slipway stop [--socket path] id-or-name [id-or-name ...]
```

`start`, `ps`, and `stop` require a running daemon. `start` persistently registers
an instance before acknowledging success. `ps` lists running instances;
`ps --all` includes all registered stopped and failed instances. `stop` accepts
multiple IDs or names and persists the intention to stay stopped, including
when an instance has already failed.

The daemon stores `registry.sqlite` and `queues/<instance-id>.sqlite` in a
persistent state directory, selected by `--state-dir`, `SLIPWAY_STATE_DIR`, or
`$XDG_STATE_HOME/slipway` (default `~/.local/state/slipway`). Only one daemon may
own a state directory, even if another socket is specified. Keep the complete
state directory, including SQLite companion files, on persistent storage.

On every daemon start, saved instances whose desired state is running resume
automatically with their stable IDs, names, queues, and validated configuration
snapshots. Shutting down the daemon preserves this intention; an explicit
`slipway stop` clears it. Individual runner failures remain visible in `ps --all`
and do not prevent other instances from starting. Failed instances are retried
on the next daemon start; there is no automatic crash loop during a daemon run.

The original YAML need not remain available for recovery. `slipway start NAME`
(or an ID) resumes the saved snapshot. To apply YAML edits, stop the instance and
start its config path again; this retains its ID and queue. Snapshot paths and
the default command working directory are fixed at registration. Referenced
watch directories, programs, and images must still be available to the daemon.

Start the daemon with `slipwayd`, then register each instance from another
terminal with `slipway start <config> [name]`. The daemon accepts only its own
options; config paths, directories, and instance names belong to the client.
Every daemon start restores the saved registry; a new state directory starts
empty.

Managed queues are independent of the YAML's `database.path`. Existing standalone
or older-version queue files are left untouched and are not automatically
imported into new managed queues. Inspect those files with `--local`; registering
a new managed instance starts fresh queue history.

The foreground command is `slipway test <config>`. It replaces `slipway run`,
accepts no instance name, `--rm`, or `--socket`, and never registers an instance
with the daemon. It executes pipeline commands; use `slipway check` to validate
and inspect a config without executing it.

Instance logs include a unique `instance_id` and a stable SHA-256 `config_hash`
of the effective configuration for comparing runs.

The socket used by each daemon-backed command is selected in this order:

1. Its explicit `--socket` value.
2. `SLIPWAY_SOCKET`, when set.
3. `$XDG_RUNTIME_DIR/slipway/slipway.sock`, when `XDG_RUNTIME_DIR` is set.
4. `slipway/slipway.sock` beneath the operating system's per-user cache directory.
5. A UID-specific directory beneath the system temporary directory when no
   user cache directory is available.

Clients and daemon must use the same socket. Keep it private: access permits
starting programs as the daemon user. Prefer one daemon per user.

## optional web dashboard

Enable the embedded dashboard with a loopback listener (disabled by default):

```bash
slipwayd --web-listen 127.0.0.1:8080
```

Alternatively, set `SLIPWAY_WEB_LISTEN`. Open <http://127.0.0.1:8080> and paste
the token from the file named in the startup log. The file is beside the
control socket with suffix `.web-token` and mode `0600`; it is replaced at
startup and removed on clean shutdown. For the system service:

```bash
sudo cat /run/slipway/slipway.sock.web-token
```

The dashboard shows known queues, job counts, attempts, commands, and captured
output, including queues whose instances have stopped. It can restart known
queues and stop active instances. Its header shows the daemon build version
(`dev` when built without an override).

Keep the token private: it authorizes reads and start/stop actions as the daemon
user. Prefer loopback with an SSH tunnel. To opt into network access, use a
wildcard bind such as `0.0.0.0:8080` or `[::]:8080` and connect by literal IP;
concrete non-loopback bind addresses and arbitrary HTTP hostnames are rejected.
Wildcard binds expose every interface, and HTTP traffic is unencrypted, so
restrict access with a firewall and protect the network path.

## queue and history inspection

Inspect a managed instance by name, ID, or its registered config path:

```bash
slipway status incoming
slipway queue incoming
slipway jobs incoming --status failed
slipway jobs incoming --watch incoming
slipway job incoming 42
slipway logs incoming 42
```

`status` prints counts; `queue` lists queued and running jobs; `jobs` lists job
history. `job` shows a job's runs and commands, and `logs` prints captured command
stdout and stderr. These commands read through `slipwayd` and accept `--socket`.
Stopped instances remain inspectable, even if their original YAML is gone.
A config-directory path selects its registered queues; select one instance when
a job ID exists in multiple queues.

Use `--local` to inspect a standalone or legacy queue directly, without a daemon:

```bash
slipway status --local incoming.yaml
slipway logs --local incoming.yaml 42
```

Local inspection loads the selected YAML file or directory and opens each
`database.path` read-only. Missing databases are errors. It cannot be combined
with `--socket` and never silently replaces managed inspection.

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

Durations use Go syntax such as `250ms`, `10s`, `15m`, `2h`. The defaults are one
worker per CPU, a `10s` retry delay, a `1s` settle period, and `./slipway.db`.
`max_retries` counts retries after the first attempt.

Relative database, watch, working-directory, and bind-mount source paths resolve
from the YAML file's directory after `values` expansion. Program paths containing
a slash resolve the same way; bare names use `PATH`. Absolute paths and paths
based on `{{file}}` or `{{dir}}` retain their meanings. Output paths resolve from
the command's working directory, or the runner's current directory if unset.

Structured Apptainer images follow the same rules for local paths and paths
after `docker-archive:` or `oci-archive:`. References containing `://` or starting
with `docker-daemon:` remain unchanged.

For standalone runs, use a distinct `database.path` per concurrent config. For
databases not yet created in the same directory, case-folded or Unicode-normalized
names are treated as aliases; pre-create them if your filesystem must distinguish
those names. Managed instances receive separate databases automatically.

Pipeline steps run sequentially. `executor` defaults to `command`, which requires
`program`. Other choices are `shell` (default `/bin/sh`), `docker`, `podman`, and
`apptainer` (their same-named CLIs on `PATH`). Set `program` to override the shell
or host-side container runtime binary.

Shell entries use `command` as shell source and support expansion, pipelines,
redirection, and other syntax provided by the selected shell:

```yaml
      - name: process-sidecars
        executor: shell
        command: |
          set -eu
          for file in "$1"/*.csv; do
            [ -e "$file" ] || continue
            process-file -- "$file"
          done
        command_args:
          - "{{dir}}"
```

The host invocation is exactly:

```text
/bin/sh -c <command> <step-name> <command_args...>
```

The step name is `$0`; `command_args` supplies `$1` onward and `"$@"`. Enable
shell options explicitly, as with `set -eu` above.

Config-local `values` may expand in shell source, but per-job templates such as
`{{file}}` are forbidden there. Pass job data through `command_args` or `env`
and quote the corresponding shell parameter to prevent filenames from becoming
executable shell source.

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

`image` is required. Each mount needs a host `source` and an absolute container
`target` after expansion. Its `options` are ordered, runtime-specific `--mount`
fields, passed through unchanged; do not repeat the mount type, source, or target
there. Templates work in options too. The legacy `read_only: true` alias becomes
a leading `ro` option.

`container_args` holds action options without `run` or `exec` itself. Use raw
`args` for runtime-global options that precede the action; a standalone `--` is
not allowed in structured mode. `command` is the optional first post-image token,
followed by `command_args`. If that first token starts with `-`, `parse` puts the
whole tail in `command_args` for the image's default entrypoint.

`container_env` sets container variables. Values appear in runtime arguments and
command history, so do not use it as a secret store. Mounts and Apptainer
environment values are CSV-encoded to preserve embedded commas and quotes.

For Docker and Podman, structured fields produce arguments in this order:

```text
run <container_args> <mount options> <environment options> <image> [<command>] <command_args>
```

Apptainer uses `exec` when `command` is set and `run` when it is omitted:

```text
exec|run --no-eval <container_args> <mount options> <environment options> <image> [<command>] <command_args>
```

Structured Apptainer invocations include `--no-eval` to disable startup
evaluation of environment values and OCI command tokens. Docker and Podman use
the image's normal `ENTRYPOINT` and `CMD` semantics. Add cleanup, networking,
working-directory, or environment-isolation options explicitly in
`container_args`, such as `--rm`, `--network=none`, or `--cleanenv`.

Use raw `args` for full control of the runtime invocation:

```yaml
        executor: docker
        args: ["run", "--rm", "example/image:latest", "process", "{{file}}"]
```

Raw `args` must include the subcommand, image, and runtime options, and cannot
be combined with structured container fields. Add `--no-eval` explicitly for
raw Apptainer invocations when needed.

Pipeline `env` and `working_directory` configure the host-side runtime process.
Use `container_env` for container variables and `container_args` for its working
directory. Apptainer inherits much of the host environment unless `--cleanenv`
is set. Use foreground runtime invocations: a step completes when the CLI exits.

Include and exclude patterns without a slash match the basename at any watched
depth. Patterns with a slash match the path relative to the watch root and may
use `**` for zero or more directories. Excludes take precedence. With
`reprocess_on_change: false`, a watch/path pair is processed once; when true, a
new size/modification-time fingerprint creates another persistent job.

Top-level `values` define reusable, config-local strings, as in `shared_dir`
above. Keys are case-sensitive identifiers (letters, digits, and underscores,
starting with a letter or underscore). Values can reference one another;
cycles and redefinitions of built-in template names are rejected. Undeclared
`{{name}}` placeholders remain unchanged for downstream tools.

Values expand before validation and path resolution; built-in templates within
them expand per job. Expanded values may appear in arguments, environment,
logs, and command history, so do not use them for secrets.

Per-job templates work in arguments, structured container images and commands,
mounts, working directories, output paths, and host or container environment
values. Shell source accepts only config-local `values`:

| Template | Value |
| --- | --- |
| `{{file}}` | Absolute file path |
| `{{dir}}` | Parent directory |
| `{{basename}}` | Filename with extension |
| `{{stem}}` | Filename without its final extension |
| `{{ext}}` | Final extension, including the dot |
| `{{job_id}}` | SQLite job ID |

Command and container executors pass arguments directly without a shell;
filename characters such as spaces, semicolons, and `$()` remain literal.
A container's entrypoint or runscript may still interpret its arguments.
Host-side `env` entries override the runner's environment for that command.

Stopping a Docker or Podman step kills the runtime CLI process group, but a
container managed by a separate daemon may outlive it. `--rm` removes a container
after exit; it does not stop it. Use a runtime-aware wrapper or container-side
deadline when cancellation must stop the workload.

Bind-mount sources must be accessible to the runtime; slipway does not create
or transfer them. Remote daemons and VM-backed runtimes may not see host paths.

`output` saves complete stdout to a file; stderr stays in captured history.
The parent directory must exist. Each attempt creates or truncates the file, so
use job-specific paths for concurrent work and expect retries to replace partial
output. Captured stdout and stderr are each limited to their first 1 MiB per
command, followed by a truncation marker; this limit does not affect `output`.

Symlink files and directories are ignored. Watch only trusted directories:
another writer can replace a file between checking it and the command opening it.

Each instance supports up to 4,096 watched directories and 1,024 files settling
at once. Exceeding these limits fails the instance. Removing or replacing a watch
root, or a persistent loss of a kernel watch, also fails it; restore the expected
path and restart the instance.

## delivery and recovery

SQLite persists jobs, retries, and execution history. On startup, unfinished
runs are marked interrupted and jobs left `RUNNING` are immediately requeued.

Execution is **at least once**: a command completed just before a crash may run
again after recovery, so pipelines should be idempotent. Stopping an instance
cancels active commands, records failed attempts when possible, and waits for
workers to exit. SIGINT and SIGTERM gracefully stop foreground runs; signaling
the daemon stops all its instances.

On Linux, cancellation kills the command's process group, including ordinary
descendants. Processes that create a new session or process group can escape;
output-pipe cleanup is time-bounded so they cannot indefinitely block shutdown.

## container

Run as your regular, non-root user from the Slipway repository root.
The UID/GID must be nonzero and not conflict with existing base-image accounts.
For standard Docker without user-namespace remapping:

```bash
docker build --pull \
  --build-arg SLIPWAY_UID="$(id -u)" \
  --build-arg SLIPWAY_GID="$(id -g)" \
  --build-arg VERSION="$(git describe --tags --always --dirty)" \
  -t localhost/slipway:local .
```

For Podman, replace `docker build --pull` with
`podman build --pull=always --format docker` to retain the image health check.
For reproducible base images, override `NODE_IMAGE`, `GO_IMAGE`, and
`RUNTIME_IMAGE` with digest-pinned references. Builds target the selected image
platform; cross-compilation or emulation requires separate setup.

### prepare the socket and workspace

Use a private socket directory and a persistent workspace:

```bash
SOCKET_DIR="${XDG_RUNTIME_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}}/slipway"
WORKSPACE="$HOME/slipway-work"

install -d -m 0700 "$SOCKET_DIR"
mkdir -p "$WORKSPACE/configs" "$WORKSPACE/incoming" "$WORKSPACE/state"
```

Do not run another daemon against this socket. Existing socket/lock files must
belong to the same user. The mounted directory must be owned by the daemon's
mapped UID and have mode 0700; image-layer ownership cannot fix a bind mount.

### run with Docker

```bash
docker run -d \
  --name slipwayd \
  --restart unless-stopped \
  --stop-timeout 45 \
  --mount "type=bind,src=$SOCKET_DIR,dst=/run/slipway" \
  --mount "type=bind,src=$WORKSPACE,dst=$WORKSPACE" \
  --workdir "$WORKSPACE" \
  --env "SLIPWAY_STATE_DIR=$WORKSPACE/state" \
  localhost/slipway:local
```

### run with rootless Podman instead

```bash
podman run -d \
  --name slipwayd \
  --restart unless-stopped \
  --stop-timeout 45 \
  --userns=keep-id \
  --mount "type=bind,src=$SOCKET_DIR,dst=/run/slipway" \
  --mount "type=bind,src=$WORKSPACE,dst=$WORKSPACE" \
  --workdir "$WORKSPACE" \
  --env "SLIPWAY_STATE_DIR=$WORKSPACE/state" \
  localhost/slipway:local
```

On SELinux-enforcing systems, replace the two `--mount` options with:

```bash
-v "$SOCKET_DIR:/run/slipway:z" \
-v "$WORKSPACE:$WORKSPACE:z"
```

Only relabel these dedicated directories, not your whole home/runtime directory.
The shared `:z` label is appropriate when multiple containers share the mounts.
SELinux socket-connection policy can require additional configuration even after
filesystem permissions and labels are correct. Do not globally disable SELinux
as a workaround. Rootless restart-at-boot requires separate service/session setup.

### connect a host-side client

```bash
export SLIPWAY_SOCKET="$SOCKET_DIR/slipway.sock"
slipway ps

# Once this config exists and its paths/executables are usable in the container:
slipway start "$WORKSPACE/configs/incoming.yaml" incoming

# The image includes a client for diagnostics too:
docker exec slipwayd slipway ps
docker logs slipwayd
```

Use `podman exec` and `podman logs` for a Podman-managed container.

The workspace is mounted at the same absolute path on both sides because
`slipway start` sends config paths to the daemon, which reads and saves a snapshot.
Set `SLIPWAY_STATE_DIR` to a persistent mounted directory; the Compose example
uses `${SLIPWAY_WORKSPACE}/state`. It contains both the registry and managed queues.
Persist the complete state directory, including SQLite companion files.

The health check validates control-API connectivity, not successful pipeline
processing. Override the socket through `SLIPWAY_SOCKET` rather than only through
`--socket`, so both daemon and health-check client use the new address.

### restart behavior

Instances submitted with `slipway start` resume automatically when the container
starts using the same persistent `SLIPWAY_STATE_DIR`. Explicitly stopped
instances stay stopped. Register new instances through `slipway start` after
the container is running.

### optional dashboard

To enable the embedded dashboard, add these options **before** the image name:

```bash
--env SLIPWAY_WEB_LISTEN=0.0.0.0:5280 \
--publish 127.0.0.1:5280:5280
```

Open <http://127.0.0.1:5280> and read the access token on the host:

```bash
# Docker
docker exec slipwayd cat /run/slipway/slipway.sock.web-token

# Docker Compose
docker compose exec -T slipwayd cat /run/slipway/slipway.sock.web-token
```

This binds the application to all interfaces inside the container but publishes
its port only on the host loopback address. Other containers with network access
to this container may still reach it. Keep the bearer token private, and do not
expose its unencrypted HTTP listener to an untrusted network.

## systemd

Install the binaries and [example unit](contrib/systemd/slipway.service),
create a dedicated service account, and add a config:

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

Replace `./my-slipway.yaml` with your [configuration](#configuration).

The system service sets `SLIPWAY_STATE_DIR=/var/lib/slipway` for the registry and
managed queues. Every managed pipeline runs as the `slipway` account,
so ensure that account can traverse each watch directory, read input files,
execute pipeline programs, and write any pipeline outputs. systemd creates
`/var/lib/slipway` through `StateDirectory=slipway` and the private socket directory
`/run/slipway` through `RuntimeDirectory=slipway`.

Set the control socket and optional dashboard in `/etc/default/slipway`:

```bash
SLIPWAY_SOCKET=/run/slipway/slipway.sock
# Optional; the dashboard is disabled when this is unset.
# SLIPWAY_WEB_LISTEN=127.0.0.1:8080
```

The unit restores registered instances and restarts after failures. Register
instances with `slipway start` after starting the service; no unit edits are
needed for individual instances.

Load and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now slipway
sudo systemctl status slipway
sudo slipway ps --socket /run/slipway/slipway.sock
sudo journalctl -u slipway -f
```

Apply later changes with `sudo systemctl restart slipway`.

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

## development

Install [Go](https://go.dev/dl/) at the version required by `go.mod` or newer.
For a repository-local toolchain, place Go at `.go/toolchain/bin/go` and run
`source activate` to select it and keep Go's caches under `.go`.

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

Or run `make web` to rebuild through Docker.

### build

```bash
make
./bin/slipway version
./bin/slipwayd version
```

![icon](icon.png)
