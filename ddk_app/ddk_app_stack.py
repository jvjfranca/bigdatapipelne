from typing import Any

from aws_cdk import (
    aws_kinesis,
    aws_kms as kms,
    aws_iam as iam,
    aws_logs as logs,
    RemovalPolicy,
    Duration
)
from aws_cdk.aws_glue import(
    CfnDatabase,
    CfnCrawler
)
from aws_cdk.aws_s3 import(
    BucketEncryption
)
from aws_cdk.aws_kinesisfirehose import(
    CfnDeliveryStream
)
from aws_cdk.aws_glue_alpha import (
    Code,
    GlueVersion,
    JobExecutable,
    JobLanguage,
    JobType
)
from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import (
    S3Factory as s3,
    KinesisStreamsFactory as dstream,
    GlueFactory
)
from aws_ddk_core.stages import (
    GlueTransformStage,
    S3EventStage
)
from aws_ddk_core.pipelines import DataPipeline
from constructs import Construct


class DdkApplicationStack(BaseStack):

    def __init__(self, scope: Construct, id: str, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        ##### KMS #####

        kms_policy = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "kms:*",
                    ],
                    principals=[iam.AccountRootPrincipal()],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Encrypt*",
                        "kms:Decrypt*",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:Describe*"
                    ],
                    principals=[
                        iam.ServicePrincipal("logs.us-east-1.amazonaws.com"),
                        iam.ServicePrincipal("glue.amazonaws.com")
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt*",
                    ],
                    principals=[
                        f"arn:aws:iam::{self.account}:role/service-role/aws-quicksight-*"
                    ],
                    resources=["*"]
                )
            ]
        )

        cmk_key = kms.Key(
            self,
            "bbbankkey",
            policy=kms_policy,
            removal_policy=RemovalPolicy.DESTROY
        )

        cmk_key.add_alias('cmk-bbbank')

        ##### CloudWatch Log Groups #####

        firehose_log = logs.LogGroup(
            self,
            'firehose-log-group',
            log_group_name='firehose-bbbank',
            encryption_key=cmk_key,
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.TWO_WEEKS
        )

        firehose_log.node.add_dependency(cmk_key)

        firehose_log.add_stream(
            'log-stream-cartoes',
            log_stream_name='cartoes'
        )

        ##### S3 Bucketss #####

        card_data = s3.bucket(
            self,
            "ddk-bucket",
            environment_id,
            encryption_key=cmk_key,
            encryption=BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )

        stage_data = s3.bucket(
            self,
            "transacoes-stage",
            environment_id,
            encryption_key=cmk_key,
            encryption=BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )

        transacoes_stage = S3EventStage(
            self,
            id="transacoes-event-capture",
            environment_id=environment_id,
            event_names=["Object Created"],
            bucket_name=card_data.bucket_name,
            key_prefix="raw"
        )

        ##### Streams #####

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

        firehose_destination = CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=card_data.bucket_arn,
            role_arn=firehose_role.role_arn,
            buffering_hints=CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=300,
                size_in_m_bs=64
            ),
            cloud_watch_logging_options=CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name="firehose-bbbank",
                log_stream_name="cartoes"
            ),
            compression_format="GZIP",
            dynamic_partitioning_configuration=CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                enabled=True,
                retry_options=CfnDeliveryStream.RetryOptionsProperty(
                    duration_in_seconds=300)
            ),
            encryption_configuration=CfnDeliveryStream.EncryptionConfigurationProperty(
                kms_encryption_config=CfnDeliveryStream.KMSEncryptionConfigProperty(
                    awskms_key_arn=cmk_key.key_arn
                )
            ),
            error_output_prefix="error/",
            prefix="raw/UF=!{partitionKeyFromQuery:uf}/",
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

        delivery_stream = CfnDeliveryStream(
            self,
            'firehose',
            delivery_stream_type='KinesisStreamAsSource',
            extended_s3_destination_configuration=firehose_destination,
            kinesis_stream_source_configuration=firehose_source
        )

        delivery_stream.node.add_dependency(firehose_log)

        ##### Glue #####

        glue_role = iam.Role(
            self,
            'bbbank-glue-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            description='role utilizada pelo glue do bbbank',
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSGlueServiceRole'
                )
            ]
        )

        glue_kms_access = iam.Policy(
            self,
            id='AcessoBBBankKMS',
            document=iam.PolicyDocument(
                assign_sids=False,
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "kms:Encrypt*",
                            "kms:Decrypt*",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*",
                            "kms:Describe*"
                        ],
                        resources=[
                            cmk_key.key_arn
                        ]
                    )
                ]
            )
        )

        glue_role.attach_inline_policy(glue_kms_access)

        database_name = 'bbbank-database'
        CfnDatabase(
            self,
            database_name,
            catalog_id=self.account,
            database_input=CfnDatabase.DatabaseInputProperty(
                description='bbbank database',
                name=database_name
            )
        )

        crw_transacoes_raw_name = 'crw-transacoes-raw'
        CfnCrawler(
            self,
            id=crw_transacoes_raw_name,
            name=crw_transacoes_raw_name,
            role=glue_role.role_name,
            database_name=database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{card_data.bucket_name}/raw/"
                    )
                ]
            )
        )
        
        crw_transacoes_stage_name = 'crw-transacoes-stage'
        CfnCrawler(
            self,
            id=crw_transacoes_stage_name,
            name=crw_transacoes_stage_name,
            role=glue_role.role_name,
            database_name=database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{stage_data.bucket_name}/stage/"
                    )
                ]
            )
        )
        
        etl_job_name = "job-transacoes-stage"
        etl_job = GlueFactory.job(
            self,
            id=f"{id}-job",
            job_name=etl_job_name,
            environment_id=environment_id,
            executable=JobExecutable.of(
                glue_version=GlueVersion.V3_0,
                language=JobLanguage.PYTHON,
                script=Code.from_asset("etl/transacoes.py"),
                type=JobType.ETL
            ),
            role=glue_role,

        )

        glue_stage = GlueTransformStage(
            self,
            id='transacoes-cartoes',
            environment_id=environment_id,
            job_name=etl_job_name,
            crawler_name=crw_transacoes_raw_name,
            job_args={
                "--S3_SOURCE_PATH": card_data.arn_for_objects("raw/"),
                "--S3_TARGET_PATH": stage_data.arn_for_objects("stage/"),
            }
        )

        card_data.grant_read(glue_role)
        stage_data.grant_read_write(etl_job)

        glue_stage.state_machine.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:StartCrawler",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:crawler/{crw_transacoes_raw_name}",
                ]
            )
        )

        ##### Data Pipeline #####
        (
            DataPipeline(scope=self, id="transacoes-data-pipeline")
            .add_stage(transacoes_stage)
            .add_stage(glue_stage)
        )
