import os
import json
import boto3
import pandas as pd

aws_lambda = boto3.client("lambda")
aws_s3 = boto3.client("s3")

# data bucket to read data from
data_bucket = os.environ.get("data_bucket")


def readFromS3(site_id, s3_bucket_name, pipeline_type, event_id):
    """Reads a file from S3

    Args:
        site_id (str): The identifier of the building/site that this model pertains to.
        s3_bucket_name (str): Name of the S3 bucket the object is in.
        pipeline_type (str): Type of the pipeline (either inference or retrain)
        event_id (str): Unique identifier for the stepfunction event

    Returns:
        dataframe: data from s3.
    """
    data = pd.read_csv(
        filepath_or_buffer="s3://"
        + s3_bucket_name
        + "/"
        + pipeline_type
        + "/"
        + event_id
        + "/"
        + site_id
        + ".csv"
    )
    return data


def handler(event, context):
    """Lamda function to read inference data and pass it to the
    inference lambda.
    """
    # Log trigger event and environment
    print(event)
    print(os.environ)

    site_id = event["Payload"]["site_id"]
    pipeline_type = event["Payload"]["pipeline_type"]
    event_id = event["Payload"]["event_id"]

    data_df = readFromS3(site_id, data_bucket, pipeline_type, event_id)
    asset_groups = data_df.groupby(["assetname", "pointname"])

    # Invoke inference Lambda with data
    for key in asset_groups.groups.keys():
        try:
            payload = {
                "site_id": site_id,
                "rtu": key[0],
                "point": key[1],
                "data": asset_groups.get_group(key)
                .reset_index()
                .to_dict(orient="records"),
                "event_id": event_id,
                "pipeline_type": pipeline_type,
            }
            response = aws_lambda.invoke(
                FunctionName=site_id + "-inference-lambda",
                Payload=json.dumps(payload),
            )
            print(response)
            if "FunctionError" in response:
                raise Exception(
                    "There's an error executing " + site_id + " inference lambda"
                )
        except Exception as err:
            print(err)

    print("Completed execution")
