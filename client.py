import asyncio
import os
import random
import time
import pandas as pd
from temporalio.client import Client

CSV_FILE = "image_urls.csv"   # CSV file with a column 'url'
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")

async def submit_workflows():
    # Connect to Temporal
    client = await Client.connect(TEMPORAL_ADDRESS)

    # Read URLs from CSV
    df = pd.read_csv(CSV_FILE)
    urls = df['url'].tolist()

    for idx, url in enumerate(urls):
        # Random delay between 1–15 seconds
        delay = random.randint(1, 15)
        print(f"[Client] Sleeping {delay}s before submitting workflow for {url}")
        time.sleep(delay)

        # Start workflow
        workflow_id = f"image-pipeline-{idx}-{int(time.time())}"
        handle = await client.start_workflow(
            "ImagePipelineWorkflow",
            url,
            id=workflow_id,
            task_queue="image-pipeline-task-queue",
        )
        print(f"[Client] Started workflow {workflow_id} for {url}")

async def main():
    await submit_workflows()

if __name__ == "__main__":
    asyncio.run(main())
