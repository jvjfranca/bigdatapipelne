from typing import Any
from aws_cdk import (
    aws_kinesisfirehose_alpha as firehose,
    aws_kinesis,
    aws_kms as kms,
    RemovalPolicy,
    Duration,
    Size
)
from aws_cdk.aws_kinesisfirehose import (
    CfnDeliveryStream
)
from aws_ddk_core.base import BaseStack

from aws_ddk_core.resources import (
    S3Factory as s3,
    KinesisStreamsFactory as dstream,
    KMSFactory as kms,
)
from aws_ddk_core.stages import (
    KinesisToS3Stage as streams3
)
from constructs import Construct


class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)


        # The code that defines your stack goes here. For example:
        card_data = s3.bucket(
            self,
            "ddk-bucket",
            environment_id,
        )

        key = kms.Key(self, "Key", removal_policy=RemovalPolicy.DESTROY)


        data_stream = dstream.data_stream(
            self,
            "card-stream",
            environment_id,
            encryption=aws_kinesis.StreamEncryption.MANAGED,
            retention_period=Duration.days(30),
            stream_mode=aws_kinesis.StreamMode.ON_DEMAND,
            stream_name="card-stream"
        )

        destination_config = firehose.DestinationConfig(
            extended_s3_destination_configuration=CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=card_data.bucket_arn,
                role_arn="roleArn",
                # the properties below are optional
                buffering_hints=CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=300,
                    size_in_mBs=64
                ),
                cloud_watch_logging_options=CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=False,
                    log_group_name="logGroupName",
                    log_stream_name="logStreamName"
                ),
                compression_format="GZIP",
                enabled=True,
                dynamic_partitioning_configuration=CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True,
                    retry_options=CfnDeliveryStream.RetryOptionsProperty(duration_in_seconds=300)
                ),
                encryption_configuration=CfnDeliveryStream.EncryptionConfigurationProperty(
                    kms_encryption_config=CfnDeliveryStream.KMSEncryptionConfigProperty(
                        awskms_key_arn=key.key_arn
                    )
                ),
                error_output_prefix="errorOutputPrefix",
                prefix="data/UF=!{partitionKeyFromQuery:uf}",
                processing_configuration=CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        CfnDeliveryStream.ProcessorProperty(
                            type="MetadataExtraction",
                            # the properties below are optional
                            parameters=[
                                CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="MetadataExtractionQuery",
                                    parameter_value="{uf:.uf}"
                                ),
                                CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="JsonParsingEngine",
                                    parameter_value="JQ-1.6"
                                )
                            ]
                        )
                    ]
                )
            )
        )

        delivery_stream = firehose.DeliveryStream(
            self,
            'firehose',
            destinations=destination_config,
            source_stream=data_stream
        )

        test_stage = streams3(
            self,
            "card-data-ingestion2",
            environment_id,
            bucket=card_data,
            delivery_stream=delivery_stream,
            data_stream=data_stream
        )


        stage_firehose_s3 = streams3(
            self,
            "card-data-ingestion",
            environment_id,
            delivery_stream_name="card-ingestion",
            bucket=card_data,
            data_output_prefix="raw/",
            data_stream=data_stream
        )