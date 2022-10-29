from typing import Any
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_kinesis,
    aws_kms as kms,
    aws_iam as iam,
    aws_s3 as cdk_s3,
    aws_logs as logs,
    RemovalPolicy,
    Duration,
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

        cmk_key = kms.Key(
            self,
            "bbbankkey",
            removal_policy=RemovalPolicy.DESTROY
        )

        firehose_log = logs.LogGroup(
            self,
            'firehose-log-group',
            log_group_name='firehose-bbbank',
            encryption_key=cmk_key,
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.TWO_WEEKS
        )

        firehose_log.add_stream(
            'log-stream-cartoes',
            log_stream_name='cartoes'
        )

        props = {
            "encryption_key": cmk_key
        }
        card_data = s3.bucket(
            self,
            "ddk-bucket",
            environment_id,
            encryption_key=cmk_key,
            encryption=cdk_s3.BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY
        )

        data_stream = dstream.data_stream(
            self,
            "card-stream",
            environment_id,
            encryption=aws_kinesis.StreamEncryption.KMS,
            encryption_key=cmk_key,
            retention_period=Duration.days(1),
            stream_mode=aws_kinesis.StreamMode.ON_DEMAND,
            stream_name="card-stream",
        )

        firehose_role = iam.Role(
            self,
            'bbbank-firehose-role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            description='role utilizada pelo firehose do bbbank'
        )
        data_stream.grant_read(firehose_role)
        card_data.grant_read_write(firehose_role)
        firehose_log.grant_write(firehose_role)

        firehose_destination = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=card_data.bucket_arn,
            role_arn=firehose_role.role_arn,
            # the properties below are optional
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=300,
                size_in_m_bs=64
            ),
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name="firehose-bbbank",
                log_stream_name="cartoes"
            ),
            compression_format="GZIP",
            dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                enabled=True,
                retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                    duration_in_seconds=300)
            ),
            encryption_configuration=firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                kms_encryption_config=firehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                    awskms_key_arn=cmk_key.key_arn
                )
            ),
            error_output_prefix="error/",
            prefix="raw/UF=!{partitionKeyFromQuery:uf}/",
            processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                enabled=True,
                processors=[
                    firehose.CfnDeliveryStream.ProcessorProperty(
                        type="MetadataExtraction",
                        # the properties below are optional
                        parameters=[
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="MetadataExtractionQuery",
                                    parameter_value="{uf:.localizacao.uf}"
                                ),
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="JsonParsingEngine",
                                    parameter_value="JQ-1.6"
                            )
                        ]
                    ),
                    firehose.CfnDeliveryStream.ProcessorProperty(
                        type="AppendDelimiterToRecord",
                        parameters=[
                            firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                parameter_name="Delimiter",
                                parameter_value='\\n'
                            )
                        ]
                    )
                ]
            )
        )

        firehose_source = firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
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
