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
        "paths": ["s3://udacity-stedi-yf/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1676744968004 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi-yf/customer-curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1676744968004",
)

# Script generated for node Privacy Filtering
PrivacyFiltering_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1676744968004,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="PrivacyFiltering_node2",
)

# Script generated for node Drop Fields
DropFields_node1676745283142 = DropFields.apply(
    frame=PrivacyFiltering_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1676745283142",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676745283142,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-stedi-yf/step-trainer-trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
