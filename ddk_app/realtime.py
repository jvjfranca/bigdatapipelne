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

    def __init__(self, scope: "Construct", id: builtins.str, environment_id: builtins.str, kms_cmk_key) -> None:

        super().__init__(scope, id)

        flink_code = aws_s3_assets.Asset(
            self,
            'flink-app-code',
            path='realtime/'
        )


        flink_role = iam.Role(
            self,
            'bbbank-flink-role',
            assumed_by=iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
            description='role utilizada pelo kinesis analytics do bbbank',
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonKinesisFullAccess'
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'CloudWatchLogsFullAccess'
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3FullAccess'
                )
            ]
        )

        flink = KDAApp(
            self, 
            'realtime-card-analytics',
            runtime_environment='FLINK-1_13',
            service_execution_role=flink_role.role_name,
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
                                "python": "realtime/app.py",
                                "jarfile": "realtime/lib/flink-sql-connector-kinesis_2.12-1.13.2",
                            }
                        ),
                        KDAApp.PropertyGroupProperty(
                            property_group_id="consumer.config.0",
                            property_map={
                                "input.stream.name": "realtime/app.py",
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
                        metrics_level="APPLICATION"
                    ),
                    parallelism_configuration=KDAApp.ParallelismConfigurationProperty(
                        configuration_type="DEFAULT",
                    )
                ),
            ),
            application_description="Realtime Card Transactions Analytics",
            application_name="realtime-cardtransactions-analytics"
        )

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

        props = {
            "environment": {
                'TABLE': ddb_realtime_table.table_name
            }
        }
        lmb_consumer = LambdaFactory.function(
            self,
            'lmbd-consumer',
            environment_id=environment_id,
            code=lambda__.Code.from_asset('lambda/consumer'),
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