# demo

In this demo I am using openalpr to read an image and get the json results then
delete the image.

## setup

```bash
# Build docker image
docker pull openalpr/openalpr:latest

# Mkdir
mkdir image

# Download test image
wget http://plates.openalpr.com/h786poj.jpg -P image/
```

## foreground test

Test the demo in the foreground:

```bash
./bin/slipway test slipway.yaml
```

`slipway test` runs locally in the foreground with its own queue database.
Press Ctrl-C to stop it gracefully.

If you run slipway before making the image directory it will fail because the sqlite db
is stored there see `slipway.yaml:database.path`

`test` takes only the config path; no instance name is required.

## daemon-managed

To run the demo in the background, start `slipwayd` in one terminal and
then use the management commands from another:

### start

```bash
slipway start slipway.yaml
```

The daemon assigns its own queue database and remembers this instance across
restarts. `slipway stop demo` stops it persistently; `slipway start demo` resumes
the saved configuration. Note the name defaults to the config basename.

### ps

```sh
slipway ps
```

```txt
ID            NAME     STATUS   STARTED                         CONFIG
0d191360f434  slipway  running  2026-09-10T05:04:46.187041365Z  "/home/g/git/slipway/demo/slipway.yaml"
```

### status

```sh
slipway status slipway
```

```txt
CONFIG     /home/g/git/slipway/demo/slipway.yaml
DATABASE   /home/g/git/slipway/demo/config/queues/0d191360f434.sqlite
TOTAL      1
QUEUED     0
RUNNING    0
SUCCEEDED  1
FAILED     0
```

### queue

```sh
slipway queue slipway
```

```txt
ID  STATUS  WATCH  ATTEMPT  AVAILABLE  PATH
```

### jobs

```sh
slipway jobs slipway
```

```txt
ID  STATUS     WATCH          ATTEMPT  AVAILABLE  PATH
1   SUCCEEDED  demo-openalpr  1/4      -          "/home/g/git/slipway/demo/image/h786poj.jpg"
```

### job

```sh
slipway job slipway 1
```

```txt
Job 1
Status:       SUCCEEDED
Watch:        demo-openalpr
File:         "/home/g/git/slipway/demo/image/h786poj.jpg"
Fingerprint:  slipway:path
Attempts:     1/4
Available:    2026-09-10T05:04:49.321304899Z
Created:      2026-09-10T05:04:49.321304899Z
Updated:      2026-09-10T05:04:52.580662974Z

Runs
  Run 1 (id=1)  SUCCEEDED  started=2026-09-10T05:04:49.341697806Z  finished=2026-09-10T05:04:52.580662974Z
    Command 1 (id=1)  SUCCEEDED  "docker" ["run","--rm","--mount","type=bind,source=/home/g/git/slipway/demo/image,target=/data,ro","openalpr/openalpr:latest","--json","--country","eu","/data/h786poj.jpg"]
      started=2026-09-10T05:04:49.344310933Z  finished=2026-09-10T05:04:52.572889883Z  exit=0  timeout=15m0s
    Command 2 (id=2)  SUCCEEDED  "/usr/bin/rm" ["/home/g/git/slipway/demo/image/h786poj.jpg"]
      started=2026-09-10T05:04:52.573860517Z  finished=2026-09-10T05:04:52.579859615Z  exit=0  timeout=15m0s
```

### logs

```sh
slipway logs slipway 1
```

```txt
== Run 1 / Command 1: openalpr (SUCCEEDED) ==
-- stdout --
{"version":2,"data_type":"alpr_results","epoch_time":1789016691683,"img_width":480,"img_height":640,"processing_time_ms":131.912506,"regions_of_interest":[{"x":0,"y":0,"width":480,"height":640}],"results":[{"plate":"IH786P0J","confidence":89.022095,"matches_template":0,"plate_index":0,"region":"","region_confidence":0,"processing_time_ms":29.223324,"requested_topn":10,"coordinates":[{"x":142,"y":384},{"x":283,"y":384},{"x":283,"y":409},{"x":143,"y":409}],"candidates":[{"plate":"IH786P0J","confidence":89.022095,"matches_template":0},{"plate":"H786P0J","confidence":87.484154,"matches_template":0},{"plate":"HH786P0J","confidence":85.219376,"matches_template":0},{"plate":"UH786P0J","confidence":84.651138,"matches_template":0},{"plate":"HC786P0J","confidence":84.076279,"matches_template":0},{"plate":"H3786P0J","confidence":83.755943,"matches_template":0},{"plate":"HG786P0J","confidence":83.716553,"matches_template":0},{"plate":"IH786PDJ","confidence":82.757408,"matches_template":0},{"plate":"IH786POJ","confidence":82.669128,"matches_template":0},{"plate":"IH786PQJ","confidence":82.250946,"matches_template":0}]}]}
-- stderr --

== Run 1 / Command 2: rm (SUCCEEDED) ==
-- stdout --

-- stderr --
```

