from constructs import Construct
import builtins

from aws_cdk import (
    aws_kinesis,
    aws_lambda as lambda__,
    aws_dynamodb as ddb,
    aws_apigateway,
    aws_s3_assets,
    aws_iam as iam,
    Duration,
    RemovalPolicy,
    aws_lambda_event_sources as event_source
)

from aws_cdk.aws_kinesisanalyticsv2 import(
    CfnApplication as KDAApp
)

from aws_ddk_core.resources import (
    LambdaFactory,
    KinesisStreamsFactory as dstream,
)

class RealTimeAnalytics(Construct):

    def __init__(self, scope: "Construct", id: builtins.str, environment_id: builtins.str, kms_cmk_key, region, account) -> None:

        super().__init__(scope, id)

        self.account = account
        self.region = region

        ### Kinesis Data Stream Sink ####
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

        ### Apache Flink Code Upload ####

        flink_code = aws_s3_assets.Asset(
            self,
            'flink-app-code',
            path='flink_app/'
        )

        ### Kinesis Analytics Role #### TODO Fechar permissoes
        flink_role = iam.Role(
            self,
            'bbbank-flink-role',
            assumed_by=iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
            description='role utilizada pelo kinesis analytics do bbbank',
            # managed_policies=[
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         'AmazonKinesisFullAccess'
            #     ),
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         'CloudWatchLogsFullAccess'
            #     ),
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         'AmazonS3FullAccess'
            #     )
            # ]
        )

        ### Flink inline policy ####
        flink_policy = iam.Policy(
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
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "kinesis:DescribeStream",
                            "kinesis:GetShardIterator",
                            "kinesis:GetRecords",
                            "kinesis:ListShards"
                        ],
                        resources=[
                            f"arn:aws:kinesis:{self.region}:{self.account}:stream/card-stream"
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "kinesis:DescribeStream",
                            "kinesis:PutRecord",
                            "kinesis:PutRecords"
                        ],
                        resources=[
                            stream_realtime.stream_arn
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "s3:GetObject",
                            "s3:GetObjectVersion"
                        ],
                        resources=[
                            f"arn:aws:s3:::ddk-*-*-assets-{self.account}-{self.region}/*"
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "logs:DescribeLogGroups",
                            "logs:DescribeLogStreams"
                        ],
                        resources=[
                            "*"
                        ]
                    ),
                    iam.PolicyStatement(
                        actions=[
                            "logs:PutLogEvents"
                        ],
                        resources=[
                            f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/kinesis-analytics/*:log-stream:kinesis-analytics-log-stream"
                        ]
                    ),

                ]
            )
        )

        flink_role.attach_inline_policy(flink_policy)

        ### Kinesis Analytics ####
        flink = KDAApp(
            self, 
            'realtime-analitycs',
            runtime_environment='FLINK-1_13',
            service_execution_role=flink_role.role_arn,
            application_configuration=KDAApp.ApplicationConfigurationProperty(
                application_code_configuration=KDAApp.ApplicationCodeConfigurationProperty(
                    code_content=KDAApp.CodeContentProperty(
                        s3_content_location=KDAApp.S3ContentLocationProperty(
                            bucket_arn=flink_code.bucket.bucket_arn,
                            file_key=flink_code.s3_object_key,
                        ),
                    ),
                    code_content_type="ZIPFILE"
                ),
                application_snapshot_configuration=KDAApp.ApplicationSnapshotConfigurationProperty(
                    snapshots_enabled=True
                ),
                environment_properties=KDAApp.EnvironmentPropertiesProperty(
                    property_groups=[
                        KDAApp.PropertyGroupProperty(
                            property_group_id="kinesis.analytics.flink.run.options",
                            property_map={
                                "python": "app.py",
                                "jarfile": "connector.jar",
                            }
                        ),
                        KDAApp.PropertyGroupProperty(
                            property_group_id="consumer.config.0",
                            property_map={
                                "input.stream.name": "card-stream",
                                "flink.stream.initpos": "LATEST",
                                "aws.region": "us-east-1",
                            }
                        ),
                        KDAApp.PropertyGroupProperty(
                            property_group_id="producer.config.0",
                            property_map={
                                "output.stream.name": "realtime-stream",
                                "shard.count": "1",
                                "aws.region": "us-east-1",
                            }
                        )
                    ]
                ),
                flink_application_configuration=KDAApp.FlinkApplicationConfigurationProperty(
                    checkpoint_configuration=KDAApp.CheckpointConfigurationProperty(
                        configuration_type="DEFAULT",
                    ),
                    monitoring_configuration=KDAApp.MonitoringConfigurationProperty(
                        configuration_type="DEFAULT",
                    ),
                    parallelism_configuration=KDAApp.ParallelismConfigurationProperty(
                        configuration_type="DEFAULT",
                    )
                ),
            ),
            application_description="Realtime",
            application_name="cardtransactions"
        )


        ### Tabela transacoes suspeitas ####
        ddb_realtime_table = ddb.Table(
            self,
            id='realtime-table',
            # table_name='transacoes',
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=kms_cmk_key,
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute='TTL',
            partition_key=ddb.Attribute(name='numero_cartao', type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name='transaction_id', type=ddb.AttributeType.STRING)
        )

        ### Funcao lambda consumidora Data Stream Sink
        props = {
            "environment": {
                'TABLE': ddb_realtime_table.table_name
            }
        }
        lmb_consumer = LambdaFactory.function(
            self,
            'lmbd-consumer',
            environment_id=environment_id,
            code=lambda__.Code.from_asset('lambda_app/consumer'),
            handler='function.handler',
            runtime=lambda__.Runtime.PYTHON_3_9,
            function_name='realtime-consumer',
            **props
        )

        lmb_consumer_event_source = event_source.KinesisEventSource(
            stream_realtime,
            starting_position=lambda__.StartingPosition.TRIM_HORIZON
        )

        lmb_consumer.add_event_source(lmb_consumer_event_source)

        stream_realtime.grant_read(lmb_consumer)


        ddb_realtime_table.grant_write_data(lmb_consumer)

        ### Funcao lambda backend api gateway
        lmb_api = LambdaFactory.function(
            self,
            'lmbd-api',
            environment_id=environment_id,
            code=lambda__.Code.from_asset('lambda_app/api'),
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

