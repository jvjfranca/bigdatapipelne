import json
import os

from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.table.window import Tumble

# 1. Creates a Table Environment
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_table(table_name, stream_name):
    return f""" CREATE TABLE {table_name} (
                transaction_id VARCHAR(37),
                valor DOUBLE,
                numero_cartao VARCHAR(16),
                tipo_cartao VARCHAR(16),
                horario_transacao TIMESTAMP(3),
                WATERMARK FOR horario_transacao AS event_time - INTERVAL '2' MINUTES
              )
              PARTITIONED BY (numero_cartao)
              WITH (
                'connector' = 'kinesis',
                'stream' = {stream_name},
                'aws.region' = 'us-east-1',
                'scan.stream.initpos' = 'LATEST',
                'sink.partitioner-field-delimiter' = ';',
                'sink.producer.collection-max-count' = '100',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """

def perform_tumbling_window_aggregation(input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    tumbling_window_table = (
        input_table
        .window(Tumble.over("120.seconds").on("event_time").alias("two_minutes"))
        .group_by("numero_cartao, two_minutes")
        .select("transaction_id, numero_cartao , valor.max as valor, two_minutes.end as event_time")
        .filter(col("valor") > float (5000))
    )
    return tumbling_window_table

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "flink.stream.initpos"

    output_stream_key = "output.stream.name"
    output_region_key = "aws.region"

    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_stream = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(
        create_table(input_table_name, input_stream)
    )

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(
        create_table(output_table_name, output_stream)
    )

    # # 4. Queries from the Source Table and creates a tumbling window over 10 seconds to calculate the cumulative price
    # # over the window.
    # tumbling_window_table = perform_tumbling_window_aggregation(input_table_name)
    # table_env.create_temporary_view("tumbling_window_table", tumbling_window_table)

    # # 5. These tumbling windows are inserted into the sink table
    table_result = table_env.execute_sql(
            f"INSERT INTO {output_table_name} SELECT * FROM {input_table_name}"
        )

    print(table_result.get_job_client().get_job_status())

if __name__ == "__main__":
    main()