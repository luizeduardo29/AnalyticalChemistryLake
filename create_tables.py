import sys
import time
from typing import Optional

import clickhouse_connect


CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "user"
CLICKHOUSE_PASSWORD = "Default@2026"
CLICKHOUSE_DATABASE = "analyticalChemistryLake"

RETRY_DELAY = 3
MAX_RETRIES = 5


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,  # ok mesmo antes de existir; criaremos já já
    )


def run_cmd(client, sql: str, label: Optional[str] = None):
    lbl = f"[{label}] " if label else ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client.command(sql)
            print(f"✅ {lbl}OK")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"❌ {lbl}Falhou: {e}\nSQL:\n{sql}\n")
                raise
            print(f"⚠️ {lbl}Erro: {e} (tentativa {attempt}/{MAX_RETRIES}) -> retry em {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)


def main():
    client = get_client()

    run_cmd(
        client,
        "CREATE DATABASE IF NOT EXISTS analyticalChemistryLake",
        "CREATE DATABASE",
    )

    run_cmd(
        client,
        f"USE {CLICKHOUSE_DATABASE}",
        "USE DB",
    )

    run_cmd(
        client,
        """
        CREATE TABLE IF NOT EXISTS analyticalChemistryLake.samples
        (
            sample_id   UUID DEFAULT generateUUIDv4(),
            sample_name String,
            created_at  DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY (sample_id)
        """,
        "CREATE samples",
    )

    run_cmd(
        client,
        """
        CREATE TABLE IF NOT EXISTS analyticalChemistryLake.sample_channels
        (
            channel_id  UUID DEFAULT generateUUIDv4(),
            sample_id   UUID,
            chromatography_technique LowCardinality(String),
            scan_filter  Nullable(String),
            sim_ion_name Nullable(String),
            created_at  DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY (sample_id, channel_id)
        """,
        "CREATE sample_channels",
    )

    run_cmd(
        client,
        """
        CREATE TABLE IF NOT EXISTS analyticalChemistryLake.chromatogram_points
        (
            channel_id UUID,
            rt         Float32 CODEC(ZSTD(3)),
            intensity  Float32 CODEC(ZSTD(3))
        )
        ENGINE = MergeTree
        ORDER BY (channel_id, rt)
        SETTINGS index_granularity = 8192
        """,
        "CREATE chromatogram_points",
    )

    run_cmd(
        client,
        """
        CREATE TABLE IF NOT EXISTS analyticalChemistryLake.lcms_scans
        (
            channel_id  UUID,
            scan_index  UInt32,
            rt          Float32 CODEC(ZSTD(3)),
            ms_level    UInt8
        )
        ENGINE = MergeTree
        ORDER BY (channel_id, scan_index)
        SETTINGS index_granularity = 8192
        """,
        "CREATE lcms_scans",
    )

    run_cmd(
        client,
        """
        CREATE TABLE IF NOT EXISTS analyticalChemistryLake.lcms_spectra_points
        (
            channel_id UUID,
            scan_index UInt32,
            mz         Float32 CODEC(ZSTD(3)),
            intensity  Float32 CODEC(ZSTD(3))
        )
        ENGINE = MergeTree
        ORDER BY (channel_id, scan_index, mz)
        SETTINGS index_granularity = 8192
        """,
        "CREATE lcms_spectra_points",
    )

    run_cmd(
        client,
        """
        ALTER TABLE analyticalChemistryLake.lcms_spectra_points
          ADD INDEX IF NOT EXISTS idx_mz mz TYPE minmax GRANULARITY 1
        """,
        "ADD INDEX idx_mz",
    )

    client.close()
    print("\n🎉 Estrutura criada/validada com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido.")
        sys.exit(1)
