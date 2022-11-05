from constructs import Construct
import builtins

from aws_cdk import (
    aws_kinesis,
    aws_lambda as lambda__,
    aws_dynamodb as ddb,
    aws_apigateway,
    Duration,
    RemovalPolicy,
    aws_lambda_event_sources as event_source,
    aws_kinesisanalytics_flink_alpha as flink,
)

from aws_ddk_core.resources import (
    LambdaFactory,
    KinesisStreamsFactory as dstream,
)

class RealTimeAnalytics(Construct):

    def __init__(self, scope: "Construct", id: builtins.str, environment_id: builtins.str, kms_cmk_key) -> None:

        super().__init__(scope, id)


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