import os
import time
import boto3
import numpy as np
import pandas as pd
import logging
import tempfile

aws_s3 = boto3.client("s3")

# Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

model_df = pd.read_csv("model.csv")


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


def handler(lambda_event, context):
    """Lambda function to run inference. Predicts if
    values in the data are anomalies.
    """
    print(lambda_event)
    print(os.environ)

    site_id = lambda_event["site_id"]
    rtu = lambda_event["rtu"]
    pointname = lambda_event["point"]
    data_df = pd.DataFrame(lambda_event["data"])
    event_id = lambda_event["event_id"]
    pipeline_type = lambda_event["pipeline_type"]

    # get mean and standard deviation for that asset-pointname combination
    asset_groups = model_df.groupby(["assetname", "pointname"])
    mean_std_df = asset_groups.get_group((rtu, pointname))
    mean = mean_std_df["mean"].values[0]
    std = mean_std_df["std"].values[0]

    # calculate threshold based on mean and standard deviation
    upper_thresh = mean + 2 * std
    lower_thresh = mean - 2 * std

    is_anomaly = []
    for i in range(data_df.shape[0]):
        if lower_thresh < data_df["value"][i] < upper_thresh:
            is_anomaly.append(0)
        else:
            is_anomaly.append(1)

    data_df["is_anomaly"] = is_anomaly
    out_file_name = f"{site_id}_{rtu}_{pointname}.csv"
    
    # create temp directory
    tmpdir = tempfile.mkdtemp()
    try:
        path = f"{tmpdir}/{out_file_name}"
        data_df.to_csv(path)

        # Save output in s3
        bucket_name = os.environ["bucket"]
        key = f"{event_id}/{out_file_name}"

        upload_to_s3(bucket_name, key, path)
    except Exception as err:
        print(err)
    finally:
        os.remove(path)
        os.rmdir(tmpdir)
        
