import json
import pathlib
import os
from datetime import datetime
import requests
import requests.exceptions as requests_exceptions

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["rockets", "space", "downloads"],
)
def download_rocket_launches_dag():

    # Download launch data from API
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -s -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    # Download all rocket images from the launch data
    @task()
    def get_pictures():
        pod_name = os.getenv("HOSTNAME", "unknown-worker")
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        download_dir = f"/tmp/images/{timestamp}"
        pathlib.Path(download_dir).mkdir(parents=True, exist_ok=True)

        print(f"Running on worker pod: {pod_name}")
        print(f"Saving images to: {download_dir}")

        with open("/tmp/launches.json") as f:
            launches = json.load(f)
            image_urls = [launch["image"] for launch in launches["results"]]

            for image_url in image_urls:
                try:
                    response = requests.get(image_url)
                    response.raise_for_status()
                    image_filename = image_url.split("/")[-1]
                    target_file = os.path.join(download_dir, image_filename)
                    with open(target_file, "wb") as img_file:
                        img_file.write(response.content)
                    print(f"✅ Downloaded {image_url} to {target_file}")
                except requests_exceptions.RequestException as e:
                    print(f"⚠️ Failed to download {image_url}: {str(e)}")

        return download_dir

    # Notify how many images were downloaded
    @task()
    def notify(image_dir: str):
        num_files = len(list(pathlib.Path(image_dir).glob("*")))
        print(f"📸 There are now {num_files} images in {image_dir}")

    # Task chaining
    image_dir = get_pictures()
    download_launches >> image_dir >> notify(image_dir)

# Instantiate the DAG
download_rocket_launches_dag()
