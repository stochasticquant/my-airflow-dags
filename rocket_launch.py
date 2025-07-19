import json
import pathlib
from datetime import datetime
import requests
import requests.exceptions as requests_exceptions

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=datetime.utcnow(),  # or use days_ago(n) if you want backfills
    schedule_interval="@daily",
    catchup=False,
    tags=["rockets", "space", "downloads"],
)
def download_rocket_launches_dag():

    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    @task()
    def get_pictures():
        pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

        with open("/tmp/launches.json") as f:
            launches = json.load(f)
            image_urls = [launch["image"] for launch in launches["results"]]

            for image_url in image_urls:
                try:
                    response = requests.get(image_url)
                    image_filename = image_url.split("/")[-1]
                    target_file = f"/tmp/images/{image_filename}"
                    with open(target_file, "wb") as img_file:
                        img_file.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
                except requests_exceptions.MissingSchema:
                    print(f"{image_url} appears to be an invalid URL.")
                except requests_exceptions.ConnectionError:
                    print(f"Could not connect to {image_url}.")

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    )

    # Set dependencies using TaskFlow-style chaining
    download_launches >> get_pictures() >> notify


# Instantiate the DAG
download_rocket_launches_dag()
