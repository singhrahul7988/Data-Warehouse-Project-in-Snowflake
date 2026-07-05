import os
from collections.abc import Iterator, Sequence
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


POSTGRES_CONN_ID = os.getenv("NOVADRIVE_POSTGRES_CONN_ID", "postgres_default")
SNOWFLAKE_CONN_ID = os.getenv("NOVADRIVE_SNOWFLAKE_CONN_ID", "snowflake")
POSTGRES_SCHEMA = os.getenv("NOVADRIVE_POSTGRES_SCHEMA", "public")

TABLE_PRIMARY_KEYS = {
    "veiculos": "id_veiculos",
    "estados": "id_estados",
    "concessionarias": "id_concessionarias",
    "vendedores": "id_vendedores",
    "clientes": "id_clientes",
    "vendas": "id_vendas",
    "cidades": "id_cidades",
}


def _get_source_columns(postgres_hook: PostgresHook, table_name: str) -> list[str]:
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    records = postgres_hook.get_records(query, parameters=(POSTGRES_SCHEMA, table_name))
    columns = [column_name for (column_name,) in records]

    if not columns:
        raise ValueError(f"No columns found for source table '{POSTGRES_SCHEMA}.{table_name}'.")

    return columns


def _chunked(values: list[int], size: int) -> Iterator[list[int]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


@task
def get_table_watermark(table_name: str, primary_key: str) -> dict[str, str | int | None]:
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    records = snowflake_hook.get_records(
        "SELECT "
        f"COALESCE(MAX({primary_key}), 0), "
        "MAX(COALESCE(data_atualizacao, data_inclusao)) "
        f"FROM {table_name}"
    )
    max_id, max_updated_at = records[0]
    return {
        "max_id": int(max_id),
        "max_updated_at": max_updated_at.isoformat() if max_updated_at else None,
    }


@task
def load_incremental_data(
    table_name: str,
    primary_key: str,
    watermark: dict[str, str | int | None],
) -> int:
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    columns = _get_source_columns(postgres_hook, table_name)
    columns_sql = ", ".join(columns)
    max_id = int(watermark["max_id"])
    max_updated_at = watermark["max_updated_at"]

    if max_updated_at:
        updated_since = datetime.fromisoformat(str(max_updated_at))
        query = (
            f"SELECT {columns_sql} "
            f"FROM {POSTGRES_SCHEMA}.{table_name} "
            f"WHERE {primary_key} > %s "
            "   OR COALESCE(data_atualizacao, data_inclusao) >= %s "
            f"ORDER BY {primary_key}"
        )
        rows: Sequence[tuple] = postgres_hook.get_records(
            query,
            parameters=(max_id, updated_since),
        )
    else:
        query = (
            f"SELECT {columns_sql} "
            f"FROM {POSTGRES_SCHEMA}.{table_name} "
            f"ORDER BY {primary_key}"
        )
        rows = postgres_hook.get_records(query)

    if not rows:
        return 0

    primary_key_index = columns.index(primary_key)
    primary_keys = sorted({int(row[primary_key_index]) for row in rows})

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as cursor:
            for primary_key_batch in _chunked(primary_keys, 1000):
                joined_ids = ", ".join(str(value) for value in primary_key_batch)
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE {primary_key} IN ({joined_ids})"
                )

    snowflake_hook.insert_rows(
        table=table_name,
        rows=rows,
        target_fields=columns,
        commit_every=1000,
    )
    return len(rows)


@dag(
    dag_id="extract_and_load_data_postgresql_to_snowflake",
    description="Load raw data incrementally from PostgreSQL to Snowflake stage tables.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    tags=["postgres", "snowflake", "elt"],
)
def postgres_to_snowflake_etl():
    for table_name, primary_key in TABLE_PRIMARY_KEYS.items():
        watermark = get_table_watermark.override(task_id=f"get_watermark_{table_name}")(
            table_name=table_name,
            primary_key=primary_key,
        )
        load_incremental_data.override(task_id=f"load_data_{table_name}")(
            table_name=table_name,
            primary_key=primary_key,
            watermark=watermark,
        )


postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()
