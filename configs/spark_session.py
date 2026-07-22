"""Shared Delta-enabled SparkSession factory for the Lakehouse stage.

On native Windows, PySpark 3.5 needs three things this project provisions
locally (not system-wide) rather than relying on what happens to be
installed:

1. JAVA_HOME pointed at a JDK 17 — a system Java newer than ~21 breaks
   Hadoop's UserGroupInformation (`Subject.getSubject` was removed), so we
   run Spark against its own portable JDK regardless of the system's Java.
2. HADOOP_HOME pointed at a directory containing winutils.exe/hadoop.dll —
   Spark's Hadoop client shells out to these on Windows even for purely
   local file operations.
3. PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON pointed at this project's venv
   interpreter — the bare `python` on PATH resolves to the Windows Store
   app-execution-alias stub, which is not a real interpreter and makes
   worker processes fail to connect back.

If these directories don't exist yet, see README.md "Lakehouse setup" for
the exact download commands (JDK 17 Temurin + cdarlint/winutils).

On Linux (Colab, the Airflow DAG's execution environment, CI, ...) none of
this is needed — the system Java and `sys.executable` already work, exactly
like the Day 1/2 lab notebooks — so this module is a no-op there.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JDK17_DIR = PROJECT_ROOT / "jdk17"
HADOOP_HOME_DIR = PROJECT_ROOT / "hadoop_home"
VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"


def _ensure_windows_spark_env() -> None:
    if not JDK17_DIR.is_dir():
        raise RuntimeError(
            f"Expected a portable JDK 17 at {JDK17_DIR} — see README.md "
            "'Lakehouse setup' for the download command."
        )
    if not (HADOOP_HOME_DIR / "bin" / "winutils.exe").is_file():
        raise RuntimeError(
            f"Expected winutils.exe under {HADOOP_HOME_DIR}\\bin — see "
            "README.md 'Lakehouse setup' for the download command."
        )

    os.environ["JAVA_HOME"] = str(JDK17_DIR)
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME_DIR)
    os.environ["PYSPARK_PYTHON"] = str(VENV_PYTHON)
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(VENV_PYTHON)
    os.environ["PATH"] = (
        f"{JDK17_DIR / 'bin'}{os.pathsep}"
        f"{HADOOP_HOME_DIR / 'bin'}{os.pathsep}"
        f"{os.environ.get('PATH', '')}"
    )


def _ensure_linux_spark_env() -> None:
    # System Java + this interpreter already work on Linux (Colab/CI) — just
    # make sure PySpark's workers use the same interpreter the driver does.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark_session(app_name: str, warehouse_dir: str | Path):
    """Returns a local, Delta-enabled SparkSession configured for this project."""
    if sys.platform == "win32":
        _ensure_windows_spark_env()
    else:
        _ensure_linux_spark_env()

    # Imported after env vars are set, since py4j reads JAVA_HOME at import time.
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
