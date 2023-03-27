import os
import boto3
import pandas as pd
from datetime import datetime


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
    filepath = f"s3://{s3_bucket_name}/{pipeline_type}/{event_id}/{site_id}.csv"
    data = pd.read_csv(filepath_or_buffer=filepath)
    return data


def create_model(data_df):
    """calculates the mean and standard deviation for each sensor. Goal
    is to use the mean and std for anomaly detection.

    Args:
        data_df (dataframe): input data.

    Returns:
        dataframe: dataframe containing mean and std for each device
    """
    asset_groups = data_df.groupby(["assetname", "pointname"])
    std_df = asset_groups["value"].std()
    mean_df = asset_groups["value"].mean()

    result_list = []
    for key in asset_groups.groups.keys():
        record = {}
        record["assetname"] = key[0]
        record["pointname"] = key[1]
        record["mean"] = mean_df[key]
        record["std"] = std_df[key]
        result_list.append(record)
    result_df = pd.DataFrame(result_list)
    return result_df


def upload_to_s3(bucket_name, key, file_path):
    """Uploads object to S3.

    Args:
        bucket_name (str): Destination bucket.
        key (str): File key in destination bucket.
        file_path (str): Path to file to upload.
    """

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).upload_file(file_path, key)
    print("Successfully uploaded model to S3")


if __name__ == "__main__":
    print(os.environ)
    site_id = os.environ["site_id"]
    event_id = os.environ["event_id"]
    pipeline_type = os.environ["pipeline_type"]
    data_bucket = os.environ["data_bucket"]
    model_artifact_bucket = os.environ["model_artifact_bucket"]
    time = datetime.now()

    data = readFromS3(site_id, data_bucket, pipeline_type, event_id)
    model_df = create_model(data)
    model_df.to_csv("model.csv")

    upload_path = f"models/{site_id}/model.csv"
    upload_to_s3(model_artifact_bucket, upload_path, "model.csv")
