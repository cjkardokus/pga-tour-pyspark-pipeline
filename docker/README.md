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

You also need a `docker/.env` file (gitignored -- every clone sets its own)
telling Docker Compose the absolute path this repo is checked out at. From
this `docker/` directory:

```bash
echo "PROJECT_ROOT=$(cd .. && pwd)" > .env
```

See `.env.example` for the format. This has to match what
`src/transform.py` computes for itself at runtime (`Path(__file__).resolve()
.parent.parent`) -- see the comment block at the top of
`docker-compose.yml` for why that matters.

## Start the cluster

From this `docker/` directory (matters both so the `../data` bind-mount
source resolves correctly and so Docker Compose picks up the `.env` file
above, which it only auto-loads from the current working directory):

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

- The `../data` directory is bind-mounted into both containers at the SAME
  absolute path it lives at on the host (`${PROJECT_ROOT}/data`, from
  `docker/.env`), not remapped to a container-only path. This matters
  because a PySpark driver connecting in client deploy mode (as
  `src/transform.py` does) resolves `spark.read`/`spark.write` paths on its
  OWN local filesystem -- since the driver runs locally on the host, that
  path has to exist there too, not just inside the containers.
  `src/transform.py` derives that same path itself at runtime (via
  `Path(__file__).resolve().parent.parent`), so as long as `docker/.env`'s
  `PROJECT_ROOT` is set correctly (see One-time setup above), both sides
  agree automatically -- nothing machine-specific is hardcoded in either
  file.
- No transformation/job code lives here yet -- this is cluster infrastructure
  only.
