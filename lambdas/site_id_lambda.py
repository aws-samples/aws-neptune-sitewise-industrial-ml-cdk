import os
import boto3
from datetime import datetime, timedelta
import json
import logging
from SPARQLWrapper import SPARQLWrapper2

# define site_id_and_rtu_lambda_name from environ variables
site_id_and_rtu_lambda_name = os.environ.get("site_id_and_rtu_lambda")

# connect to s3 for file upload
aws_s3 = boto3.client("s3")

# connect to Lambda
aws_lambda = boto3.client("lambda")

# connect to neptune
neptune_cluster_writer_endpoint = os.environ.get("neptune_cluster_writer_endpoint")
sparql = SPARQLWrapper2("https://" + neptune_cluster_writer_endpoint + ":8182/sparql")


def getAllSites():
    # the below query returns all sites from Neptune with which the site is a BRICK:Building

    query = """
    PREFIX BRICK: <https://brickschema.org/schema/Brick#>
    PREFIX BMS: <http://amazon.bms.com/mybuilding#>
    PREFIX RDF: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX RDFS: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT  ?siteLabel

    WHERE   { 
        ?site a BRICK:Building .
        ?site RDFS:label ?siteLabel .
    }
    """
    sparql.setQuery(query)

    # results list holds dict for each site
    results = []

    for bind in sparql.query().bindings:
        result = {}
        if "siteLabel" in bind:
            # creating a dict object with key "site" and value is the sightLabel
            result["site"] = bind["siteLabel"].value
        results.append(result)
    return results


# def startSitewiseQueryLambda(siteID, function_arn, pipeline_type, eventID):
#    # create json object
#    dictionary = {
#        "site_id" : siteID,
#        "pipeline_type": pipeline_type,
#        "event_id": eventID
#    }
#    jsonObj = json.dumps(dictionary, indent = 4)
#    aws_lambda.invoke(
#        FunctionName = function_arn,
#        InvocationType = 'Event',
#        Payload = jsonObj
#    )


def handler(event, context):
    print(event)
    print(os.environ)

    # event ID - used as a unique ID for this invocation
    event_id = event["id"]

    # define pipeline type
    if "inferrule" in event["resources"][0]:
        pipeline_type = "inference"
    elif "retrainrule" in event["resources"][0]:
        pipeline_type = "retrain"

    # get all sites stored in Neptune
    allSites = getAllSites()
    logging.info(f"response from Neptune querying all sites: {allSites}")

    # parse response to get a list of sites. This will be used for the mapping state, where a Lambda will run for each site
    siteList = [i["site"] for i in allSites]
    logging.info(f"all sites in Neptune: {siteList}")

    # build response - step functions uses this in the map state
    response = []
    for i in siteList:
        response.append(
            {"site_id": i, "pipeline_type": pipeline_type, "event_id": event_id}
        )

    return {"data": response}
