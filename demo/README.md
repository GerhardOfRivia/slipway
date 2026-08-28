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

## foreground run

Run the demo in the foreground:

```bash
./bin/slipway run
```

`slipway` will connect to `slipwayd` when it is reachable and otherwise logs that it is
running daemonless. Press Ctrl-C to stop it gracefully in either mode.

If you run slipway before making the image directory it will fail because the sqlite db
is stored there see `slipway.yaml:database.path`

If you want your configs to have different names use the `--config file.yaml` option.

## daemon-managed run

To run the demo in the background, start `./bin/slipwayd` in one terminal and
then use the management commands from another:

```bash
./bin/slipway start
./bin/slipway ps
```
