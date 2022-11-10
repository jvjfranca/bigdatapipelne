# -*- coding: utf-8 -*-

"""
tumbling-windows.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to a Kinesis Data Stream
    4. Queries from the Source Table and
       creates a tumbling window over 10 seconds to calculate the cumulative PRICE over the window.
    5. These tumbling window results are inserted into the Sink table.
"""

from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Tumble
from pyflink.table.expressions import col
import os
import json

# 1. Creates a Table Environment
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda


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


def create_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                numero_cartao VARCHAR(30),
                valor DOUBLE,
                horario_transacao TIMESTAMP(3),
                WATERMARK FOR horario_transacao AS horario_transacao - INTERVAL '10' SECOND
              )
              PARTITIONED BY (numero_cartao)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'sink.partitioner-field-delimiter' = ';',
                'sink.producer.collection-max-count' = '100',
                'sink.producer.aggregation-enabled' = 'false',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )


def perform_tumbling_window_aggregation(input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    tumbling_window_table = (
        input_table.window(
            Tumble.over("10.seconds").on("horario_transacao").alias("ten_second_window")
        )
        .group_by("numero_cartao, ten_second_window")
        .select("numero_cartao, valor.sum as valor, ten_second_window.end as horario_transacao")
        .filter(col('valor') > float(5000))
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
        create_table(input_table_name, input_stream, input_region, stream_initpos)
    )

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(
        create_table(output_table_name, output_stream, output_region, stream_initpos)
    )

    # 4. Queries from the Source Table and creates a tumbling window over 10 seconds to calculate the cumulative PRICE
    # over the window.
    tumbling_window_table = perform_tumbling_window_aggregation(input_table_name)
    table_env.create_temporary_view("tumbling_window_table", tumbling_window_table)

    # 5. These tumbling windows are inserted into the sink table
    table_result = table_env.execute_sql("INSERT INTO {0} SELECT * FROM {1}"
                                         .format(output_table_name, "tumbling_window_table"))

    # get job status through TableResult
    print(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()