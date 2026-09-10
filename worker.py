from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from datetime import timedelta
import os

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

# --- Activities ---
# NOTE: third-party imports (requests, PIL) are done inside activities
# so the Temporal workflow sandbox does not try to import them
# when validating ImagePipelineWorkflow.
@activity.defn
async def download_image(url: str) -> bytes:
    import requests

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.content

@activity.defn
async def resize_image(image_bytes: bytes, size=(200, 200)) -> bytes:
    from io import BytesIO
    from PIL import Image

    img = Image.open(BytesIO(image_bytes))
    img = img.resize(size)
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

@activity.defn
async def apply_grayscale(image_bytes: bytes) -> bytes:
    from io import BytesIO
    from PIL import Image, ImageOps

    img = Image.open(BytesIO(image_bytes))
    img = ImageOps.grayscale(img)
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

@activity.defn
async def save_image(image_bytes: bytes, filename="output.png") -> str:
    # Each workflow run gets its own folder named by workflow ID
    # (e.g. output/image-pipeline-0-1789033885/output.png) so concurrent
    # runs never overwrite each other. The ID already embeds the client
    # index + timestamp, which is the race-free equivalent of
    # output1.png, output2.png, ... (a shared counter would race
    # across workers).
    run_dir = os.path.join(OUTPUT_DIR, activity.info().workflow_id)
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, os.path.basename(filename))
    with open(path, "wb") as f:
        f.write(image_bytes)
    return path

# --- Workflow ---
@workflow.defn
class ImagePipelineWorkflow:
    @workflow.run
    async def run(self, url: str) -> str:
        img = await workflow.execute_activity(download_image, url, start_to_close_timeout=timedelta(seconds=30))
        img = await workflow.execute_activity(resize_image, img, start_to_close_timeout=timedelta(seconds=30))
        img = await workflow.execute_activity(apply_grayscale, img, start_to_close_timeout=timedelta(seconds=30))
        filename = await workflow.execute_activity(save_image, img, start_to_close_timeout=timedelta(seconds=30))
        return filename

# --- Worker Setup ---
async def main():
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = Worker(
        client,
        task_queue="image-pipeline-task-queue",
        workflows=[ImagePipelineWorkflow],
        activities=[download_image, resize_image, apply_grayscale, save_image],
    )
    await worker.run()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
