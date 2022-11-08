import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_TARGET_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1667841046774 = glueContext.create_dynamic_frame.from_catalog(
    database="bbbank-database",
    table_name="stage",
    transformation_ctx="AWSGlueDataCatalog_node1667841046774",
)

# Script generated for node Drop Fields
DropFields_node1667924275279 = DropFields.apply(
    frame=AWSGlueDataCatalog_node1667841046774,
    paths=["nome", "cvv"],
    transformation_ctx="DropFields_node1667924275279",
)

# Script generated for node Aggregate
Aggregate_node1667924310506 = sparkAggregate(
    glueContext,
    parentFrame=DropFields_node1667924275279,
    groups=[
        "bandeira",
        "numero_cartao",
        "exp",
        "tipo_cartao",
        "cor_cartao",
        "tipo_transacao",
        "cidade",
        "latitude",
        "longitude",
        "estado",
    ],
    aggs=[["valor", "sum"]],
    transformation_ctx="Aggregate_node1667924310506",
)

# Script generated for node Amazon S3
AmazonS3_node1667841280999 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1667924310506,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"{args['S3_TARGET_PATH']}",
        "partitionKeys": ["estado"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1667841280999",
)

job.commit()