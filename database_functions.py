"""
database_functions provides functionalities for connection to the database and query from it in the context of the wwtwin package.
Copyright (C) 2025  Saba Daneshgar, BIOMATH, Ghent University

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""

from __future__ import annotations

import sys
import os
from os import listdir
import pandas as pd
# import scipy as sp
import numpy as np
# import matplotlib.pyplot as plt   
# import xlrd
import sqlite3
from sqlite3 import Error
# import pyodbc
# import psycopg2
# import pymysql
from sqlalchemy import create_engine
from sqlalchemy import text
import openpyxl
from typing import List, Sequence, Union, Optional, Any, Dict, Literal
import logging
from pathlib import Path
from sqlalchemy.engine import Engine, Connection
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)
SqlConn = Union[Engine, Connection, Any]  # Any allows a DB-API connection as a fallback


def _ensure_linked_server(
    engine: Engine,
    *,
    linked_server_name: str,
    linked_server_provider: str = "Historian OLE DB Provider",
    linked_server_datasource: Optional[str] = None,
) -> None:
    """
    Ensure a linked server exists on the connected MSSQL instance.

    - No-op if the linked server already exists (checks sys.servers).
    - Creates the linked server if missing.
    """
    if not linked_server_name:
        raise ValueError("linked_server_name must be provided.")

    # Check existence
    exists_sql = text(
        """
        SELECT 1
        FROM sys.servers
        WHERE name = :srvname
        """
    )

    # Build EXEC for sp_addlinkedserver. We can't parameterize identifiers directly,
    # so we use quoted string parameters for provider and datasrc values.
    create_sql = text(
        """
        EXEC master.dbo.sp_addlinkedserver
            @server = :srvname,
            @provider = :provider,
            @datasrc = :datasrc
        """
    )

    with engine.begin() as conn:
        res = conn.execute(exists_sql, {"srvname": linked_server_name}).fetchone()
        if res:
            logger.info("Linked server '%s' already exists.", linked_server_name)
            return

        ds = linked_server_datasource or ""
        conn.execute(
            create_sql,
            {
                "srvname": linked_server_name,
                "provider": linked_server_provider,
                "datasrc": ds,
            },
        )
        logger.info(
            "Created linked server '%s' (provider='%s', datasource='%s').",
            linked_server_name,
            linked_server_provider,
            ds,
        )






def create_database_engine(
    database_type: str,
    database_name: str,
    path_to_database: Optional[Union[str, Path]] = None,
    *,
    server: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    driver: Optional[str] = None,
    readonly: bool = True,
    mode: str = "ro",
    # Historian / linked-server specifics:
    linked_server_name: str = "historian_db",
    linked_server_provider: str = "Historian OLE DB Provider",
    linked_server_datasource: Optional[str] = None,
    **kwargs: Any,
) -> Engine:
    """
    Create a SQLAlchemy Engine for the given database.
    Supports: sqlite, postgres, mysql, mssql, historian (MSSQL + linked server).
    """
    dbt = (database_type or "").strip().lower()
    if dbt not in {"sqlite", "postgres", "mysql", "mssql", "historian"}:
        raise ValueError('database_type must be one of: "sqlite", "postgres", "mysql", "mssql", "historian".')

    server = server or "localhost"
    user = user or ""
    password = password or ""
    connect_args: dict[str, Any] = {}
    url: str

    if dbt == "sqlite":
        base = Path(path_to_database or ".").expanduser().resolve()
        base.mkdir(parents=True, exist_ok=True)
        db_path = base / database_name
        uri = f"{db_path.as_posix()}?mode={mode}&cache=shared"
        url = f"sqlite+pysqlite:///{uri}"
        connect_args["uri"] = True

    elif dbt == "postgres":
        port = port or 5432
        if readonly:
            connect_args["options"] = "-c default_transaction_read_only=on"
        url = f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@{server}:{port}/{database_name}"

    elif dbt == "mysql":
        port = port or 3306
        url = f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{server}:{port}/{database_name}"

    elif dbt in {"mssql", "historian"}:
        port = port or 1433
        driver = driver or "ODBC Driver 17 for SQL Server"
        odbc_params = f"driver={driver.replace(' ', '+')}"
        if readonly:
            odbc_params += "&ApplicationIntent=ReadOnly"
        odbc_params += "&TrustServerCertificate=yes"
        url = (
            f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(password)}@"
            f"{server}:{port}/{database_name}?{odbc_params}"
        )

    else:
        raise ValueError(f"Unsupported database_type: {database_type}")

    engine = create_engine(url, future=True, connect_args=connect_args)
    logger.info("Created SQLAlchemy engine for %s (%s)", database_name, dbt)

    # Historian path: ensure linked server exists
    if dbt == "historian":
        _ensure_linked_server(
            engine,
            linked_server_name=linked_server_name,
            linked_server_provider=linked_server_provider,
            linked_server_datasource=linked_server_datasource,
        )

    return engine












def read_hist_table(engine: Engine, linked_server: str, remote_query: str):
    """
    Example: remote_query might be something like:
      SELECT TagName, DateTime, Value FROM ihRawData WHERE TagName IN ('P1','P2') AND DateTime >= '2024-01-01'
    """
    sql = text(f"SELECT * FROM OPENQUERY([{linked_server}], :q)")
    # Note: some drivers require the remote query string to be single-quoted in T-SQL;
    # parameter binding helps avoid quoting issues.
    with engine.connect() as conn:
        return pd.read_sql_query(sql, conn, params={"q": remote_query})
    


def create_database_connection(
    database_type: str,
    database_name: str,
    path_to_database: Optional[Union[str, Path]],
    readonly: bool = True,
    mode: str = "ro",
    **kwargs: Any,
) -> Engine:
    """
    Backward-compatible shim that now returns a SQLAlchemy Engine.
    If legacy code needs a DB-API connection, use:
        engine = create_database_connection(...);
        conn = engine.raw_connection()     # DB-API connection
    """
    eng = create_database_engine(
        database_type=database_type,
        database_name=database_name,
        path_to_database=path_to_database,
        readonly=readonly,
        mode=mode,
        **kwargs,
    )

    return eng





def _as_list(x: Optional[Union[str, Sequence[str]]]) -> Optional[List[str]]:
    if x is None:
        return None
    return [x] if isinstance(x, str) else list(x)


def _get_sqlalchemy_connection(conn: SqlConn) -> Connection:
    """
    Accept an Engine, Connection, or DB-API connection and return a SQLAlchemy Connection.
    Closes the temporary wrapper automatically at the end of the calling context.
    """
    if isinstance(conn, Connection):
        return conn
    if isinstance(conn, Engine):
        return conn.connect()
    # DB-API fallback: wrap via SQLAlchemy's creator pattern (one-shot)
    from sqlalchemy import create_engine

    raw = conn  # DB-API connection object
    engine = create_engine("sqlite://", creator=lambda: raw)  # URL string is ignored by creator
    return engine.connect()


def create_dataframe(
    conn: SqlConn,
    table_name: Optional[str],
    is_historian: bool,
    time_column: Optional[str],
    variable_columns: Optional[Union[str, Sequence[str]]],
    select_all: bool = True,
    starttime: Optional[Union[str, pd.Timestamp]] = None,
    endtime: Optional[Union[str, pd.Timestamp]] = None,
    filter_tagnames_column: Optional[str] = None,
    filter_eq: Optional[Dict[str, Any]] = None,
    filter_tagnames: Optional[Union[str, Sequence[str]]] = None,
    filter_gt: Optional[Dict[str, Any]] = None,
    filter_lt: Optional[Dict[str, Any]] = None,
    filter_geq: Optional[Dict[str, Any]] = None,
    filter_leq: Optional[Dict[str, Any]] = None,
    *,
    schema: Optional[str] = None,
    limit: Optional[int] = None,
    # Historian OPENQUERY path:
    linked_server_name: Optional[str] = None,
    openquery_sql: Optional[str] = None,
) -> pd.DataFrame:
    """
    Read from a database using an Engine/Connection (or DB-API connection).

    If `is_historian` is True and `openquery_sql` & `linked_server_name` are provided,
    the function will read via:
        SELECT * FROM OPENQUERY([linked_server_name], '<openquery_sql>')
    Otherwise it will build a portable SELECT on `schema.table_name`.

    Returns
    -------
    pd.DataFrame
    """
    # Historian (OPENQUERY) branch
    if is_historian:
        if not linked_server_name or not openquery_sql:
            raise ValueError(
                "Historian mode requires both 'linked_server_name' and 'openquery_sql'."
            )
        # OPENQUERY string literal must be embedded; parameterization is driver-dependent.
        # Safely single-quote the remote SQL:
        remote_sql = openquery_sql.replace("'", "''")
        tsql = f"SELECT * FROM OPENQUERY([{linked_server_name}], '{remote_sql}')"
        with _get_sqlalchemy_connection(conn) as sa_conn:
            df = pd.read_sql_query(text(tsql), sa_conn)
            logger.info("Read %d rows via OPENQUERY on linked server '%s'", len(df), linked_server_name)
            return df

    # Regular RDBMS path
    if not table_name and not select_all:
        raise ValueError("table_name must be provided when not using historian/OPENQUERY.")

    cols_sql: str
    if select_all or not variable_columns:
        cols_sql = "*"
    else:
        cols: List[str] = _as_list(variable_columns) or []
        if time_column and time_column not in cols:
            cols = [time_column] + cols
        # naive quoting (assumes simple identifiers). For mixed-case or reserved words,
        # you can wrap with double quotes or brackets based on dialect if needed.
        cols_sql = ", ".join(cols)

    fq_table = f"{schema}.{table_name}" if schema else table_name

    where_clauses: List[str] = []
    params: Dict[str, Any] = {}

    # Time window
    if time_column:
        if starttime is not None:
            where_clauses.append(f"{time_column} >= :starttime")
            params["starttime"] = pd.to_datetime(starttime)
        if endtime is not None:
            where_clauses.append(f"{time_column} <= :endtime")
            params["endtime"] = pd.to_datetime(endtime)

    # Tag filters
    if filter_tagnames_column and filter_tagnames is not None:
        tags = _as_list(filter_tagnames) or []
        if tags:
            placeholders = ", ".join([f":tag{i}" for i in range(len(tags))])
            where_clauses.append(f"{filter_tagnames_column} IN ({placeholders})")
            params.update({f"tag{i}": t for i, t in enumerate(tags)})

    # Column comparisons
    def _cmp(prefix: str, op: str, mapping: Optional[Dict[str, Any]]):
        if not mapping:
            return
        for i, (col, val) in enumerate(mapping.items()):
            key = f"{prefix}_{i}"
            where_clauses.append(f"{col} {op} :{key}")
            params[key] = val

    _cmp("eq", "=", filter_eq)
    _cmp("gt", ">", filter_gt)
    _cmp("lt", "<", filter_lt)
    _cmp("geq", ">=", filter_geq)
    _cmp("leq", "<=", filter_leq)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    limit_sql = f" LIMIT {int(limit)}" if (limit is not None and int(limit) > 0) else ""

    sql = f"SELECT {cols_sql} FROM {fq_table}{where_sql}{limit_sql}"
    
    with _get_sqlalchemy_connection(conn) as sa_conn:
        df = pd.read_sql_query(text(sql), sa_conn, params=params)
        logger.info("Read %d rows from %s", len(df), fq_table)
        print(df.head())
        return df





def find_data_freq(s):
    for idx, letter in enumerate(s, 0):
        if letter.isalpha():
            freq = int(s[:idx])
            unit = s[idx:]
            return freq, unit
        




def _read_file(
    filepath: str | Path,
    ext: str,
    skiprows: int = 0,
    encoding: str = "utf8",
    decimal: str = ".",
) -> pd.DataFrame:
    """
    Read a file of given extension into a pandas DataFrame.

    Parameters
    ----------
    filepath : str | Path
        Complete path to the file.
    ext : str
        File extension, e.g. ".csv", ".txt", ".xls", ".xlsx", ".parquet", ".feather".
    skiprows : int
        Number of rows to skip at the start of the file.
    encoding : str
        File encoding (for text/CSV).
    decimal : str
        Decimal separator (for CSV/text).

    Returns
    -------
    pd.DataFrame
        Data from the file.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the extension is unsupported.
    """
    path = Path(filepath).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = ext.lower().strip()

    try:
        if ext in (".csv", ".txt"):
            # Auto-detect separator from the first line
            sep: Optional[str] = None
            with path.open("r", encoding=encoding, errors="ignore") as f:
                first_line = f.readline()
                if "\t" in first_line:
                    sep = "\t"
                elif ";" in first_line:
                    sep = ";"
                elif "," in first_line:
                    sep = ","

            df = pd.read_csv(
                path,
                sep=sep,
                skiprows=skiprows,
                encoding=encoding,
                decimal=decimal,
                low_memory=False,
                index_col=None,
            )

        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(path, skiprows=skiprows, index_col=None)

        elif ext == ".parquet":
            df = pd.read_parquet(path)

        elif ext == ".feather":
            df = pd.read_feather(path)

        else:
            raise ValueError(
                f"Unsupported extension: {ext}. "
                "Allowed: .csv, .txt, .xls, .xlsx, .parquet, .feather"
            )

        logger.info("Read %d rows and %d cols from %s", *df.shape, path.name)
        return df

    except Exception as e:
        logger.exception("Failed to read %s: %s", path, e)
        raise

    

def _get_header_length(
    read_file: str | Path,
    ext: str = ".txt",
    comment: str = "#",
) -> int:
    """
    Determine the number of header rows at the top of a file.

    Parameters
    ----------
    read_file : str | Path
        Path to the file.
    ext : str, default ".txt"
        File extension. Supported: ".txt", ".csv", ".xls", ".xlsx", ".zrx".
    comment : str, default "#"
        Comment symbol indicating header rows in text/CSV files.

    Returns
    -------
    int
        Number of header rows.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the extension is unsupported or parsing fails.
    """
    path = Path(read_file).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = ext.lower().strip()
    headerlength = 0
    counter = 0

    try:
        if ext in (".xls", ".xlsx", ".zrx"):
            # Excel-like: inspect first column of successive rows until we hit a non-comment
            wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
            try:
                sheet = wb.worksheets[0]
                while True:
                    value = sheet.cell(row=counter + 1, column=1).value
                    if value is None:
                        break
                    s = str(value)
                    if not s or s[0] != comment:
                        break
                    headerlength += 1
                    counter += 1
            finally:
                wb.close()

        elif ext in (".txt", ".csv"):
            # Text/CSV: count leading lines that start with the comment char
            with path.open("r", encoding="utf8", errors="ignore") as f:
                while True:
                    pos = f.tell()
                    line = f.readline()
                    if not line:
                        break
                    if not line or line[0] != comment:
                        # Rewind so downstream readers can start from the first data line if needed
                        f.seek(pos)
                        break
                    headerlength += 1

        else:
            raise ValueError(
                f"Unsupported extension for header detection: {ext}. "
                "Allowed: .txt, .csv, .xls, .xlsx, .zrx"
            )

        logger.debug("Detected %d header rows in %s", headerlength, path.name)
        return max(0, headerlength)

    except Exception as e:
        logger.exception("Failed to detect header length in %s: %s", path, e)
        raise

# def read_files(path,files,sep=',',comment='#',encoding='utf8',decimal='.', to_csv=False):
def read_files(
        path: Union[str, os.PathLike],
        files: Sequence[str],
        sep: str = ",",
        comment: str = "#",
        encoding: str = "utf8",
        decimal: str = ".",
        to_csv: bool = False,
    ) -> pd.DataFrame:
    """
    Reads all files in a given directory, joins them and returns one pd.dataframe

    Parameters
    ----------
    path : str
	path to the folder that contains the files to be joined
    files : list
        list of files to be joined, must be the same extension
    ext : str
        extention of the files to read; possible: excel, text, csv
    sep : str
        the separating element (e.g. , or \t) necessary when reading csv-files
    comment : str
        comment symbol used in the files
    sort : array of bool and str
        if first element is true, apply the sort function to sort the data
        based on the tags in the column mentioned in the second element of the
        sort array

    Returns
    -------
    pd.dataframe:
        pandas dataframe containin concatenated files in the given directory
    """
    
    base = Path(path).expanduser().resolve()
    if not base.exists() or not base.is_dir():
        raise NotADirectoryError(f"Invalid directory: {base}")

    if not files:
        logger.warning("No files provided to read in %s", base)
        return pd.DataFrame()

    # Ensure deterministic order
    files_sorted: List[str] = sorted(files)

    frames: List[pd.DataFrame] = []
    for file_name in files_sorted:
        file_path = base / file_name
        if not file_path.exists():
            logger.warning("File not found, skipping: %s", file_path)
            continue

        _, ext = os.path.splitext(file_name)
        try:
            header_len = _get_header_length(str(file_path), ext, comment)  # uses your helper
        except Exception as e:
            logger.warning("Failed to detect header length for %s: %s. Assuming 0.", file_path, e)
            header_len = 0

        try:
            df = _read_file(  # your helper (must return a DataFrame)
                str(file_path),
                ext=ext,
                skiprows=header_len,
                decimal=decimal,
                encoding=encoding,
            )
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"_read_file must return a pandas.DataFrame, got {type(df)}")
            frames.append(df)
            logger.info("Read %d rows from %s", len(df), file_path.name)
        except Exception as e:
            logger.exception("Error reading %s: %s. Skipping this file.", file_path, e)
            continue

    if not frames:
        logger.warning("No data frames were read successfully from %s.", base)
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True, sort=False)

    if to_csv:
        out_path = base / "joined_files"
        try:
            data.to_csv(out_path, sep=sep, index=False)
            logger.info("Wrote concatenated CSV to %s", out_path)
        except Exception as e:
            logger.exception("Failed to write concatenated CSV to %s: %s", out_path, e)

    return data