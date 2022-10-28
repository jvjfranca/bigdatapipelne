from typing import Any
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_kinesisfirehose_alpha as alpha_firehose,
    aws_kinesisfirehose_destinations_alpha as destinations,
    aws_kinesis,
    aws_kms as kms,
    aws_iam as iam,
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
    KinesisStreamsFactory as dstream
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
            environment_id
        )
        
        cmk_key = kms.Key(
            self,
            "bbbankkey",
            removal_policy=RemovalPolicy.DESTROY
        )

        data_stream = dstream.data_stream(
            self,
            "card-stream",
            environment_id,
            encryption=aws_kinesis.StreamEncryption.MANAGED,
            retention_period=Duration.days(30),
            stream_mode=aws_kinesis.StreamMode.ON_DEMAND,
            stream_name="card-stream"
        )

        firehose_role = iam.Role(
            self,
            'bbbank-firehose-role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            description='role utilizada pelo firehose do bbbank'
        )
        data_stream.grant_read(firehose_role)
        card_data.grant_read_write(firehose_role)

        firehose_destination=CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=card_data.bucket_arn,
                role_arn=firehose_role.role_arn,
                # the properties below are optional
                buffering_hints=CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=300,
                    size_in_m_bs=64
                ),
                cloud_watch_logging_options=CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name="firehose-bbbank",
                    log_stream_name="cartao"
                ),
                compression_format="GZIP",
                dynamic_partitioning_configuration=CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True,
                    retry_options=CfnDeliveryStream.RetryOptionsProperty(duration_in_seconds=300)
                ),
                encryption_configuration=CfnDeliveryStream.EncryptionConfigurationProperty(
                    kms_encryption_config=CfnDeliveryStream.KMSEncryptionConfigProperty(
                        awskms_key_arn=cmk_key.key_arn
                    )
                ),
                error_output_prefix="error/",
                prefix="data/UF=!{partitionKeyFromQuery:uf}/",
                processing_configuration=CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        CfnDeliveryStream.ProcessorProperty(
                            type="MetadataExtraction",
                            # the properties below are optional
                            parameters=[
                                CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="MetadataExtractionQuery",
                                    parameter_value="{uf:.localizacao.uf}"
                                ),
                                CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="JsonParsingEngine",
                                    parameter_value="JQ-1.6"
                                )
                            ]
                        ),
                        CfnDeliveryStream.ProcessorProperty(
                            type="AppendDelimiterToRecord",
                            parameters=[
                                CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="Delimiter",
                                    parameter_value='\\n'
                                )
                            ]
                        )
                    ]
                )
            )

        firehose_source = CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
            kinesis_stream_arn=data_stream.stream_arn,
            role_arn=firehose_role.role_arn
        )


        delivery_stream = firehose.CfnDeliveryStream(
            self,
            'firehose',
            delivery_stream_type='KinesisStreamAsSource',
            extended_s3_destination_configuration=firehose_destination,
            kinesis_stream_source_configuration=firehose_source
        )


        # stage_firehose_s3 = streams3(
        #     self,
        #     "card-data-ingestion",
        #     environment_id,
        #     delivery_stream_name="card-ingestion",
        #     bucket=card_data,
        #     data_output_prefix="raw/",
        #     data_stream=data_stream
        # )