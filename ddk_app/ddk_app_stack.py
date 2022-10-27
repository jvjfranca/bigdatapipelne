from typing import Any

from aws_cdk.aws_kinesis import IStream, Stream, StreamEncryption, StreamMode
from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import (
    DataBrewFactory as brew,
    GlueFactory as glue,
    KinesisFirehoseFactory as firehose,
    KinesisStreamsFactory as dstream,
    KMSFactory as kms,
    LambdaFactory as lmb,
    S3Factory as s3,
    SQSFactory as sqs
)
from aws_ddk_core.resources.commons import Duration
from constructs import Construct


class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)


        # The code that defines your stack goes here. For example:
        ddk_bucket = s3.bucket(
            self,
            "ddk-bucket",
            environment_id,
        )

        data_stream = dstream.data_stream(
            self,
            "card-stream",
            environment_id,
            encryption=StreamEncryption.MANAGED,
            retention_period=Duration("2629800"),
            stream_mode=StreamMode.ON_DEMAND,
            stream_name="card-stream"
        )