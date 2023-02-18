import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi-yf/accelerometer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1676749379315 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi-yf/step-trainer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1676749379315",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1676749379315,
    keys1=["serialNumber", "timeStamp"],
    keys2=["serialNumber", "sensorReadingTime"],
    transformation_ctx="Join_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-stedi-yf/machine-learning-curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
