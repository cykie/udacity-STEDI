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
        "paths": ["s3://udacity-stedi-yf/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1676741419830 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi-yf/customer_trsuted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1676741419830",
)

# Script generated for node Privacy Filtering
PrivacyFiltering_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1676741419830,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFiltering_node2",
)

# Script generated for node Drop Fields
DropFields_node1676741762210 = DropFields.apply(
    frame=PrivacyFiltering_node2,
    paths=[
        "email",
        "phone",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1676741762210",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676741762210,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-stedi-yf/accelerometer-trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
