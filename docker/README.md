# Local Spark cluster (Docker)

A minimal Spark standalone cluster for local development: one master + one
worker, running via `apache/spark:4.2.0-python3` (matches the `pyspark==4.2.0`
pin in `requirements.txt`).

The official `apache/spark` image is used so the driver (host) and cluster
(containers) run the exact same Spark version.

Sized for a 6GB-RAM-capped WSL2 environment: the worker is capped at 2GB /
2 cores and the master at 1GB / 1 core (both enforced by Docker via
`mem_limit`/`cpus`), leaving headroom for the OS, VS Code server, and a local
Jupyter kernel/PySpark driver process running alongside the cluster.

## One-time setup

Before first run, grant the container write access to the processed data
directory (the container runs as a different, unprivileged uid than your host
user, so it needs this explicitly):

```bash
chmod -R o+w data/processed
```

Run this from the project root. `data/raw/` is intentionally left untouched --
Spark only ever reads the source CSV from there, never writes to it.

## Start the cluster

From this `docker/` directory:

```bash
docker compose up -d
```

`-d` runs it in the background. Omit it to watch the logs in your terminal
(useful the first time, to confirm both containers start cleanly).

## Confirm it's running

Open the Spark Master web UI: **http://localhost:8080**

You should see:
- **Status: ALIVE** near the top
- One entry under **Workers** (State: ALIVE), with ~2.0 GB memory and 2 cores
  listed as available

You can also check container status directly:

```bash
docker compose ps
```

Once a job is running (e.g. from a local PySpark session connecting to
`spark://localhost:7077`), its DAG/stage UI is available at
**http://localhost:4040** for the duration of that job.

## Stop the cluster

```bash
docker compose down
```

This stops and removes the containers (but not the images). Data written
under this project's `data/` directory persists on the host, since it's a
bind mount rather than a container volume.

## Notes

- The `../data` directory is bind-mounted into both containers at
  `/opt/spark/work-dir/data`, so cluster jobs can read `data/raw/` and write
  to `data/processed/` using that container path.
- No transformation/job code lives here yet -- this is cluster infrastructure
  only.
