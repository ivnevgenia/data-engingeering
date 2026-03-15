from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_green_trips_source(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime STRING,
            tip_amount DOUBLE,
            event_time AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return "green_trips"

def create_hourly_tips_sink(t_env):
    sink_ddl = """
        CREATE TABLE hourly_tips_sink (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'hourly_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return "hourly_tips_sink"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_green_trips_source(t_env)
    sink_table = create_hourly_tips_sink(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM {source_table}
        GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR)
    """).wait()

if __name__ == "__main__":
    main()