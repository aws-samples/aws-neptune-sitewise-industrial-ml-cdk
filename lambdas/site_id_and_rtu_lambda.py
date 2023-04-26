import os
import time
import csv
import json
import boto3
import pandas as pd
import numpy as np
import logging
from SPARQLWrapper import SPARQLWrapper2
import datetime
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

aws_s3 = boto3.client("s3")
sitewise_client = boto3.client("iotsitewise")

neptune_cluster_writer_endpoint = os.environ.get("neptune_cluster_writer_endpoint")
sparql = SPARQLWrapper2("https://" + neptune_cluster_writer_endpoint + ":8182/sparql")

data_bucket = os.environ.get("data_bucket")


def getRTUsandPointForAs(site_id):
    # this query returns all RTUs (and their SiteWise ID) for the specified site,
    # as well as all point measurements for the RTU (and their SiteWise IDs).
    query = """
    PREFIX BRICK: <https://brickschema.org/schema/Brick#>
    PREFIX BMS: <http://amazon.bms.com/building-%s#>
    PREFIX RDF: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX RDFS: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT  ?rtuLabel ?rtuSitewiseId ?pointlabel ?pointSitewiseId

    WHERE { 
    BIND ("%s" AS  ?siteLabel)
    ?site RDFS:label ?siteLabel.
    ?s BRICK:hasLocation ?site;
        a BRICK:RTU;
        RDFS:label ?rtuLabel;
        BRICK:timeseries ?rtuSitewiseId;
        BRICK:hasPoint ?point.
    ?point BRICK:timeseries ?pointSitewiseId;
        RDFS:label ?pointlabel.
    ?site BRICK:hasPoint ?point
    }
    """ % (
        site_id,
        site_id,
    )

    sparql.setQuery(query)

    # results list to store dict objects
    results = []

    for bind in sparql.query().bindings:
        # dict object with keys assetName, assetSiteWiseId, pointName, pointSiteWiseId
        result = {}
        if "rtuLabel" in bind:
            result["assetName"] = bind["rtuLabel"].value
        if "rtuSitewiseId" in bind:
            result["assetSiteWiseId"] = bind["rtuSitewiseId"].value
        if "pointlabel" in bind:
            result["pointName"] = bind["pointlabel"].value
        if "pointSitewiseId" in bind:
            result["pointSiteWiseId"] = bind["pointSitewiseId"].value
        results.append(result)
    return results


def getTimeInterval(pipeline_type):
    # REMOVE THIS FUNCTION AND UNCOMMENT OUT THE VERSION ABOVE ONCE WE MOVE TO CONDUIT
    end_time = datetime.datetime.fromtimestamp(1652732267)
    if pipeline_type == "inference":
        start_time = int((end_time + datetime.timedelta(hours=-1)).timestamp())
    if pipeline_type == "retrain":
        start_time = int((end_time + datetime.timedelta(days=-10)).timestamp())
    return end_time, start_time


def getHistoricalDatawithinTimeInterval(assetProperties, start_time, end_time):
    ## Function Inputs:
    #  assetProperties - list of dict objects with keys: assetName, assetSiteWiseId, pointName, pointSiteWiseId
    #  start_time - timestamp value (datetime.datetime type) the exclusive start of the range from which to query historical data, expressed in seconds in Unix epoch time
    #  end_time - timestamp value (datetime.datetime type) the inclusive end of the range from which to query historical data, expressed in seconds in Unix epoch time
    #  end_time must always occur before start_time

    # defining query_results list
    query_results = []

    for i in assetProperties:
        ap = {
            "entryId": i["assetName"] + "_-_" + i["pointName"].replace("brick:", ""),
            "assetId": i["assetSiteWiseId"],
            "propertyId": i["pointSiteWiseId"],
            "startDate": start_time,
            "endDate": end_time,
            "timeOrdering": "ASCENDING",
        }
        response = sitewise_client.batch_get_asset_property_value_history(entries=[ap])

        # the response is paginated. the below logic helps the function work with the paginated response
        if "nextToken" in response:
            nt = response["nextToken"]
        else:
            nt = None

        for entry in response["successEntries"]:
            assetname = entry["entryId"].split("_-_")[0]
            pointname = entry["entryId"].split("_-_")[1]
            for item in entry["assetPropertyValueHistory"]:
                for key in item["value"]:
                    datatype = key

                value = item["value"][datatype]

                timestamp = int(
                    str(item["timestamp"]["timeInSeconds"])
                    + str(item["timestamp"]["offsetInNanos"])
                )
                result = {
                    "assetname": assetname,
                    "pointname": pointname,
                    "value": value,
                    "timestamp": timestamp,
                }
                query_results.append(result)

        while nt is not None:
            response = sitewise_client.batch_get_asset_property_value_history(
                entries=[ap], nextToken=nt
            )
            for entry in response["successEntries"]:
                assetname = entry["entryId"].split("_-_")[0]
                pointname = entry["entryId"].split("_-_")[1]
                for item in entry["assetPropertyValueHistory"]:
                    for key in item["value"]:
                        datatype = key

                    value = item["value"][datatype]

                    timestamp = int(
                        str(item["timestamp"]["timeInSeconds"])
                        + str(item["timestamp"]["offsetInNanos"])
                    )
                    result = {
                        "assetname": assetname,
                        "pointname": pointname,
                        "value": value,
                        "timestamp": timestamp,
                    }
                    query_results.append(result)

            if "nextToken" in response:
                nt = response["nextToken"]
            else:
                nt = None
    return query_results


def s3Writer(event_id, data_frame, s3_bucket_name, pipeline_type, site_id):
    path = (
        # "s3://"
        # + s3_bucket_name
        # + "/"
        pipeline_type
        + "/"
        + event_id
        + "/"
        + site_id
        + ".csv"
    )
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(s3_bucket_name, path).put(Body=csv_buffer.getvalue())


def handler(event, context):
    print(event)
    print(os.environ)

    site_id = event["site_id"]
    pipeline_type = event["pipeline_type"]
    event_id = event["event_id"]
    
    logger.info(f'Starting to get data from Neptune')
    site_asset_data = getRTUsandPointForAs(site_id)
    logger.info(f'Data from Neptune: {site_asset_data[0]}')
    end_time, start_time = getTimeInterval(pipeline_type)
    logger.info(f'Starting to get data from SiteWise')
    data = getHistoricalDatawithinTimeInterval(site_asset_data, start_time, end_time)
    dataframe = pd.DataFrame.from_records(data)
    logger.info(f'Data from SiteWise: {dataframe.head()}')

    logger.info(f'Writing data to S3')
    s3Writer(event_id, dataframe, data_bucket, pipeline_type, site_id)
    logger.info(f'Data written to S3')

    return {"site_id": site_id, "pipeline_type": pipeline_type, "event_id": event_id}
