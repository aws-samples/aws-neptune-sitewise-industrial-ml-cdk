import os
import time
import csv
import json
import boto3
from datetime import datetime

client = boto3.client("codebuild")


def handler(event, context):
    # Log trigger event and environment
    print(event)
    print(os.environ)

    project_name = event["project_name"]
    site_id = event["site_id"]

    response = client.start_build(
        projectName=project_name,
        environmentVariablesOverride=[
            {"name": "SITE_ID", "value": site_id, "type": "PLAINTEXT"}
        ],
    )

    id = response["build"]["id"]

    is_running = True

    while is_running:
        response = client.batch_get_builds(ids=[id])

        build_status = response["builds"][0]["buildStatus"]
        print(build_status)
        if build_status == "IN_PROGRESS":
            time.sleep(5)
        elif build_status == "SUCCEEDED":
            is_running = False
            doreturn = True
        else:
            raise Exception(
                "codebuild project" + str(response["builds"][0]) + "did not succeed"
            )

    if doreturn:
        return project_name + " for " + site_id + " succeeded."
