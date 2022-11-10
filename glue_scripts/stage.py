import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv, [
        "JOB_NAME",
        "S3_TARGET_PATH"
    ])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
bucket_raw = glueContext.create_dynamic_frame.from_catalog(
    database="bbbank-database", table_name="raw", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
data_conversion = ApplyMapping.apply(
    frame=bucket_raw,
    mappings=[
        ("nome", "string", "nome", "string"),
        ("cpf", "string", "cpf", "string"),
        ("valor", "double", "valor", "double"),
        ("bandeira", "string", "bandeira", "string"),
        ("numero_cartao", "string", "numero_cartao", "string"),
        ("cvv", "string", "cvv", "string"),
        ("exp", "string", "exp", "string"),
        ("tipo_cartao", "string", "tipo_cartao", "string"),
        ("cor_cartao", "string", "cor_cartao", "string"),
        ("tipo_transacao", "string", "tipo_transacao", "string"),
        ("localizacao.cidade", "string", "cidade", "string"),
        ("localizacao.estado", "string", "loc-estado", "string"),
        ("localizacao.lat", "string", "latitude", "double"),
        ("localizacao.lng", "string", "longitude", "double"),
        ("horario_transacao", "string", "horario_transacao", "string"),
        ("estado", "string", "estado", "string"),
    ],
    transformation_ctx="data_conversion",
)

unnested_frame = data_conversion.unnest()

# Script generated for node S3 bucket
stage_bucket = glueContext.write_dynamic_frame.from_options(
    frame=unnested_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"{args['S3_TARGET_PATH']}",
        "partitionKeys": ["estado"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()