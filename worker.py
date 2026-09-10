from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
from datetime import timedelta
import os

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")

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
    with open(filename, "wb") as f:
        f.write(image_bytes)
    return filename

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
