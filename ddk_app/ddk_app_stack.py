from typing import Any

from aws_cdk import (
    aws_kinesis,
    aws_kms as kms,
    aws_iam as iam,
    aws_logs as logs,
    aws_lambda as lambda__,
    aws_dynamodb as ddb,
    aws_lambda_event_sources as event_source,
    # aws_kinesisanalytics_flink_alpha as flink,
    aws_apigateway,
    RemovalPolicy,
    Duration,
)
from aws_cdk.aws_glue import(
    CfnDatabase,
    CfnCrawler
)
from aws_cdk.aws_s3 import(
    BucketEncryption,
    StorageClass,
    Transition
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
    LambdaFactory,
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
from cdk_watchful import Watchful


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
                        iam.ArnPrincipal(f"arn:aws:iam::{self.account}:role/service-role/aws-quicksight-service-role-v0")
                    ],
                    resources=["*"]
                )
            ]
        )

        kms_cmk_key = kms.Key(
            self,
            "bbbankkey",
            policy=kms_policy,
            removal_policy=RemovalPolicy.DESTROY
        )

        kms_cmk_key.add_alias('cmk-bbbank')

        ##### CloudWatch Log Groups #####

        log_firehose = logs.LogGroup(
            self,
            'firehose-log-group',
            log_group_name='firehose-bbbank',
            encryption_key=kms_cmk_key,
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.TWO_WEEKS
        )

        log_firehose.node.add_dependency(kms_cmk_key)

        log_firehose.add_stream(
            'log-stream-cartoes',
            log_stream_name='cartoes'
        )

        ##### S3 Bucketss #####

        s3_card_data = s3.bucket(
            self,
            "ddk-bucket",
            environment_id,
            encryption_key=kms_cmk_key,
            encryption=BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )

        s3_stage_data = s3.bucket(
            self,
            "transacoes-stage",
            environment_id,
            encryption_key=kms_cmk_key,
            encryption=BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )
        
        s3_spec_data = s3.bucket(
            self,
            "transacoes-spec",
            environment_id,
            encryption_key=kms_cmk_key,
            encryption=BucketEncryption.KMS,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True
        )

        event_transacoes_stage = S3EventStage(
            self,
            id="transacoes-event-capture",
            environment_id=environment_id,
            event_names=["Object Created"],
            bucket_name=s3_card_data.bucket_name,
            key_prefix="raw"
        )

        event_transacoes_spec = S3EventStage(
            self,
            id="transacoes-spec-capture",
            environment_id=environment_id,
            event_names=["Object Created"],
            bucket_name=s3_stage_data.bucket_name,
            key_prefix="stage"
        )

        s3_card_data.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(7),
            enabled=True,
            transitions=[
                Transition(
                    storage_class=StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30)
                    ),
                Transition(
                    storage_class=StorageClass.GLACIER,
                    transition_after=Duration.days(90)
                )
            ]
            
        )

        s3_stage_data.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(7),
            enabled=True,
            transitions=[
                Transition(
                    storage_class=StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30)
                    ),
                Transition(
                    storage_class=StorageClass.GLACIER,
                    transition_after=Duration.days(90)
                )
            ]
            
        )

        s3_spec_data.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(7),
            enabled=True,
            transitions=[
                Transition(
                    storage_class=StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30)
                    ),
                Transition(
                    storage_class=StorageClass.GLACIER,
                    transition_after=Duration.days(90)
                )
            ]
            
        )

        ##### Streams #####

        stream_data_stream = dstream.data_stream(
            self,
            "card-stream",
            environment_id,
            encryption=aws_kinesis.StreamEncryption.KMS,
            encryption_key=kms_cmk_key,
            retention_period=Duration.days(1),
            stream_mode=aws_kinesis.StreamMode.ON_DEMAND,
            stream_name="card-stream",
        )

        iam_firehose_role = iam.Role(
            self,
            'bbbank-firehose-role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            description='role utilizada pelo firehose do bbbank'
        )

        stream_data_stream.grant_read(iam_firehose_role)
        s3_card_data.grant_read_write(iam_firehose_role)
        log_firehose.grant_write(iam_firehose_role)

        firehose_destination = CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=s3_card_data.bucket_arn,
            role_arn=iam_firehose_role.role_arn,
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
                    awskms_key_arn=kms_cmk_key.key_arn
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
            kinesis_stream_arn=stream_data_stream.stream_arn,
            role_arn=iam_firehose_role.role_arn
        )

        delivery_stream = CfnDeliveryStream(
            self,
            'firehose',
            delivery_stream_type='KinesisStreamAsSource',
            extended_s3_destination_configuration=firehose_destination,
            kinesis_stream_source_configuration=firehose_source
        )

        delivery_stream.node.add_dependency(log_firehose)

        ##### Glue #####

        iam_glue_role = iam.Role(
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

        iam_policy_glue_kms_access = iam.Policy(
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
                            kms_cmk_key.key_arn
                        ]
                    )
                ]
            )
        )

        iam_glue_role.attach_inline_policy(iam_policy_glue_kms_access)

        glue_database_name = 'bbbank-database'
        CfnDatabase(
            self,
            glue_database_name,
            catalog_id=self.account,
            database_input=CfnDatabase.DatabaseInputProperty(
                description='bbbank database',
                name=glue_database_name
            )
        )

        glue_crw_transacoes_raw_name = 'crw-transacoes-raw'
        CfnCrawler(
            self,
            id=glue_crw_transacoes_raw_name,
            name=glue_crw_transacoes_raw_name,
            role=iam_glue_role.role_name,
            database_name=glue_database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{s3_card_data.bucket_name}/raw/"
                    )
                ]
            )
        )
        
        glue_crw_transacoes_stage_name = 'crw-transacoes-stage'
        CfnCrawler(
            self,
            id=glue_crw_transacoes_stage_name,
            name=glue_crw_transacoes_stage_name,
            role=iam_glue_role.role_name,
            database_name=glue_database_name,
            targets=CfnCrawler.TargetsProperty(
                s3_targets=[
                    CfnCrawler.S3TargetProperty(
                        path=f"s3://{s3_stage_data.bucket_name}/stage/"
                    )
                ]
            )
        )
        
        glue_etl_job_name = "job-transacoes-stage"
        glue_etl_job = GlueFactory.job(
            self,
            id=f"{id}-job",
            job_name=glue_etl_job_name,
            environment_id=environment_id,
            executable=JobExecutable.of(
                glue_version=GlueVersion.V3_0,
                language=JobLanguage.PYTHON,
                script=Code.from_asset("etl/transacoes.py"),
                type=JobType.ETL
            ),
            role=iam_glue_role
        )

        glue_etl_job_name_spec = "job-transacoes-spec"
        glue_etl_job_spec = GlueFactory.job(
            self,
            id=f"{id}-job-spec",
            job_name=glue_etl_job_name_spec,
            environment_id=environment_id,
            executable=JobExecutable.of(
                glue_version=GlueVersion.V3_0,
                language=JobLanguage.PYTHON,
                script=Code.from_asset("etl/spec.py"),
                type=JobType.ETL
            ),
            role=iam_glue_role
        )

        glue_stage = GlueTransformStage(
            self,
            id='transacoes-cartoes',
            environment_id=environment_id,
            job_name=glue_etl_job_name,
            crawler_name=glue_crw_transacoes_raw_name,
            job_args={
                "--S3_SOURCE_PATH": s3_card_data.arn_for_objects("raw/"),
                "--S3_TARGET_PATH": s3_stage_data.arn_for_objects("stage/"),
            }
        )
        glue_stage_spec = GlueTransformStage(
            self,
            id='transacoes-cartoes-spec',
            environment_id=environment_id,
            job_name=glue_etl_job_name_spec,
            crawler_name=glue_crw_transacoes_stage_name,
            job_args={
                "--S3_SOURCE_PATH": s3_stage_data.arn_for_objects("stage/"),
                "--S3_TARGET_PATH": s3_spec_data.arn_for_objects("spec/"),
            }
        )

        s3_card_data.grant_read(iam_glue_role)
        s3_stage_data.grant_read_write(glue_etl_job)
        s3_spec_data.grant_read_write(glue_etl_job_spec)

        glue_stage.state_machine.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:StartCrawler",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:crawler/{glue_crw_transacoes_raw_name}",
                ]
            )
        )

        glue_stage_spec.state_machine.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:StartCrawler",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:crawler/{glue_crw_transacoes_stage_name}",
                ]
            )
        )

        ##### Data Pipeline #####
        (
            DataPipeline(scope=self, id="transacoes-data-pipeline")
            .add_stage(event_transacoes_stage)
            .add_stage(glue_stage)
            .add_stage(glue_stage_spec)
        )

        wf = Watchful(self, 'ddk-watch')
        wf.watch_scope(self)


        ##### Realtime ###########

        # flink_app = flink.Application(
        #     self,
        #     'realtime-transaction',
        #     runtime=flink.Runtime.FLINK_1_13,
        #     application_name='transcations-realtime-analytics',
        #     code=flink.ApplicationCode.from_asset('flink')
        # )
        stream_realtime = dstream.data_stream(
            self,
            "realtime-stream",
            environment_id,
            encryption=aws_kinesis.StreamEncryption.KMS,
            encryption_key=kms_cmk_key,
            retention_period=Duration.days(1),
            stream_mode=aws_kinesis.StreamMode.ON_DEMAND,
            stream_name="realtime-stream"
        )

        ddb_realtime_table = ddb.Table(
            self,
            id='realtime-table',
            table_name='transacoes-suspeitas',
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=kms_cmk_key,
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute='TTL',
            partition_key=ddb.Attribute(name='CardHolder', type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name='CardNumber', type=ddb.AttributeType.STRING)
        )

        lmb_consumer = LambdaFactory.function(
            self,
            'lmbd-consumer',
            environment_id=environment_id,
            code=lambda__.Code.from_asset('lambda/consumer'),
            handler='function.handler',
            runtime=lambda__.Runtime.PYTHON_3_9,
            function_name='realtime-consumer',
        )

        lmb_consumer_event_source = event_source.KinesisEventSource(
            stream_realtime,
            starting_position=lambda__.StartingPosition.TRIM_HORIZON
        )

        lmb_consumer.add_event_source(lmb_consumer_event_source)

        lmb_consumer.add_environment('TABLE', 'transacoes-suspeitas')

        stream_realtime.grant_read(lmb_consumer)


        ddb_realtime_table.grant_write_data(lmb_consumer)

        lmb_api = LambdaFactory.function(
            self,
            'lmbd-api',
            environment_id=environment_id,
            code=lambda__.Code.from_asset('lambda/api'),
            handler='function.handler',
            runtime=lambda__.Runtime.PYTHON_3_9,
            function_name='api-backend'
        )

        ddb_realtime_table.grant_read_data(lmb_api)

        api_gateway = aws_apigateway.LambdaRestApi(
            self,
            id='bbbank-api',
            handler=lmb_api, 
        )