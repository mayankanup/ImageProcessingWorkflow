# AGENTS.md — ImageProcessingWorkflow

Temporal image pipeline (Python 3.11, `temporalio` + `pillow` + `requests` + `pandas`). No tests, lint, CI, or README.

## Architecture

- `worker.py`: defines `ImagePipelineWorkflow` + 4 activities (`download_image` → `resize_image` 200x200 PNG → `apply_grayscale` → `save_image`). Task queue: `image-pipeline-task-queue`. Workflow/activity names are string-matched — keep `client.py`'s `"ImagePipelineWorkflow"` and queue name in sync.
- `client.py`: reads `image_urls.csv` column `url`, submits one workflow per URL with id `image-pipeline-{idx}-{timestamp}`.
- Infra (`docker-compose.yml`): `temporalio/auto-setup:1.25.1` + `postgres:15` (`DB=postgres12`; `latest` auto-setup dropped SQLite support). Temporal on `localhost:7233`, UI on `localhost:8233`; `worker1/2/3` all build the same `Dockerfile` with `TEMPORAL_ADDRESS=temporal:7233` and `restart: unless-stopped` (Temporal takes ~60s to init; workers must retry).

## Commands

```bash
pip install -r requirements.txt
docker compose up --build        # temporal + 3 workers
python worker.py                 # local worker (needs Temporal up first)
python client.py                 # submit workflows (needs worker running)
```

Order matters: Temporal → worker(s) → client, otherwise workflows sit pending.

## Gotchas

- Temporal address comes from `TEMPORAL_ADDRESS` env (default `localhost:7233`); compose sets `temporal:7233`. Never hardcode `localhost` for in-container code.
- Workflow sandbox: keep third-party imports (`requests`, `PIL`) inside activity functions, never at module top-level, or `Worker` fails with `Failed validating workflow ImagePipelineWorkflow` / `RestrictedWorkflowAccessError`. Timeouts must be `timedelta`, not bare ints.
- `image_urls.csv` contains placeholder `example.com` URLs — replace with real image URLs for a real run.
- `save_image` always writes `output.png`, so concurrent workflows overwrite each other; use a unique filename per workflow id when fixing.
- `client.py` uses blocking `time.sleep` inside `async` (should be `await asyncio.sleep`); `worker.py` activities use blocking `requests.get` inside `async defn` (blocks the event loop).
