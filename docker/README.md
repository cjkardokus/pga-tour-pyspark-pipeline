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

Before first run, create a `docker/.env` file (gitignored -- every clone
sets its own) telling Docker Compose the absolute path this repo is checked
out at, plus your host user/group id. From this `docker/` directory:

```bash
printf 'PROJECT_ROOT=%s\nUID=%s\nGID=%s\n' "$(cd .. && pwd)" "$(id -u)" "$(id -g)" > .env
```

See `.env.example` for what each value means:

- `PROJECT_ROOT` has to match what `src/transform.py` computes for itself
  at runtime (`Path(__file__).resolve().parent.parent`) -- see the comment
  block at the top of `docker-compose.yml` for why that matters.
- `UID`/`GID` run the Spark containers as *you*, so files the driver
  (host) and executors (containers) create under `data/` don't clash on
  ownership/permissions -- see the comment block at the top of
  `docker-compose.yml` for the specific failure this avoids. This is why
  there's no separate `chmod -R o+w data/processed` step here anymore:
  once the containers run as your uid, they already have the same write
  access to `data/` that you do.

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
- Both services also set `user: "${UID}:${GID}"` (from `docker/.env`), so
  the containers run as your host user rather than the image's baked-in
  uid. `spark.write` with `mode("overwrite")` deletes and recreates its
  output directory on every run -- the driver (host, your uid) creates the
  top-level directory, and the executor (container) then has to create
  files inside it. Matching uids means that always works; mismatched uids
  fail with a `Mkdirs failed` permission error on every write, not just the
  first one, since `chmod` doesn't stick across `overwrite` recreating the
  directory.
- No transformation/job code lives here yet -- this is cluster infrastructure
  only.
