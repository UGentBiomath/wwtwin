"""
Class DataBase provides functionalities for creating local sqlite database
from a raw (waste)water data source.

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


# from data_reading_functions import join_files
from database_functions import create_database_connection, create_dataframe, find_data_freq, read_files
# import wwdata as ww
# import datetime as dt
import pandas as pd
# import os
# from dateutil import parser
import logging

from dateutil import parser as dtparser

import os
import io
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union, List, Dict, Any, Literal, Tuple

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine, Connection

# logger = logging.getLogger(__name__)

IfExists = Literal["fail", "replace", "append"]
Conn = Union[Engine, Connection]
SimulationMode = Literal["real-time", "historical"]
AggName = Literal["mean", "sum", "min", "max", "median", "first", "last"]
FillName = Literal["ffill", "bfill", "interpolate"]


class DataSourceType(str, Enum):
    FILE = "file"
    DATABASE = "database"




class FileType(str, Enum):
    EXCEL = "excel"
    TEXT = "text"
    CSV = "csv"
    PARQUET = "parquet"
    FEATHER = "feather"




class DatabaseType(str, Enum):
    MYSQL = "mysql"
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    HISTORIAN = "historian"
    MSSQL = "mssql"




@dataclass(slots=True)
class _ValidatedPaths:
    raw_data_path: Optional[Path]
    local_db_dir: Path
    local_db_path: Path # full path *without* suffix




@dataclass(slots=True)
class ExtractOptions:
    table_name: Optional[str] = None
    time_column_name: Optional[str] = None
    variable_column_names: Optional[Union[str, Sequence[str]]] = None
    select_all: bool = True
    starttime: Optional[Union[str, pd.Timestamp]] = None
    endtime: Optional[Union[str, pd.Timestamp]] = None
    filter_tagnames_column: Optional[str] = None
    filter_tagnames: Optional[Union[str, Sequence[str]]] = None
    filter_eq: Optional[Dict[str, Any]] = None
    filter_gt: Optional[Dict[str, Any]] = None
    filter_lt: Optional[Dict[str, Any]] = None
    filter_geq: Optional[Dict[str, Any]] = None
    filter_leq: Optional[Dict[str, Any]] = None
    target_table: str = "_raw_data" # local table name




def _as_list(x: Optional[Union[str, Sequence[str]]]) -> Optional[List[str]]:
    if x is None:
        return None
    if isinstance(x, str):
        return [x]
    return list(x)





class DataBase():
    """
    Attributes
    ----------
    data_source : str
        define the source of the data, options: "file" and "database"
    path_to_data_source : str
        path to the directory that contains the raw data file/s
    path_to_local_db : str
        path to the directory for storing the local database
    local_database_name : str
        name of the local database to be stored
    file_names : list
        list of files to be joined, must have the same extension
    file_type : str
        extension of the files to read, options: excel, text, csv
    sep : str
        the separating element necessary for reading text/csv files e.g. \t
    encoding : str
        encoding used for the data files
    database_type : str
        type of the database storing the source raw data, possible options: "mysql", "postgres", "sqlite", "historian" and "mssql"
    database_name : str
        name of the database storing the source raw data
    server : str
        server name for connecting to the database storing the source raw data, "localhost" will be passed if not specified
    user : str
        username for connecting to the source database
    password : str
        password for connecting to the source database
    port : int
        port for connecting to the source database
    driver : str
        driver for connecting to the source database, if not specified, default ODBC values will be passed based on the type of database
    table_name : str
        name of the table in the source database to query data from
    time_column_name : str
        name of the column in the database table containing the time data
    variable_column_names : str or list
        name or list of names for the columns containing time series of the data to be queried from the database
    select_all : bool
        indicate whether all columns should be queried from the database table or not, default is True
    starttime : str
        start time for filtering the query command
    endtime : str
        end time for filtering the query command
    filter_tagnames_column : str
        name of the column to be filtered in the query command
    filter_eq : str, int, float or list
        "equals to" filter. value or a list of values to be used for filtering the columns in the query command
    linked_server_name : str
        name of the linked server to be created on a Microsoft SQL Server for connecting to the historian, only used for database type of "historian"
    linked_server_provider : str
        name of the linked server provider to be created on a Microsoft SQL Server for connecting to the historian, only used for database type of "historian"
    linked_server_datasource : str
        name of the linked server data source to be created on a Microsoft SQL Server for connecting to the historian, only used for database type of "historian"
    
    """
    # --- Public attributes (typed) ---
    data_source: DataSourceType
    path_to_data_source: Optional[Path]
    path_to_local_db: Path
    local_database_name: str
    file_names: Optional[List[str]]
    file_type: FileType
    sep: str
    encoding: str


    database_type: Optional[DatabaseType]
    database_name: Optional[str]


    server: Optional[str]
    user: Optional[str]
    password: Optional[str]
    port: Optional[int]
    driver: Optional[str]


    linked_server_name: Optional[str]
    linked_server_provider: Optional[str]
    linked_server_datasource: Optional[str]


    # Computed
    is_historian: bool
    engine: Engine # SQLAlchemy engine for the *local* SQLite DB


    def __init__(self,
        data_source: Union[DataSourceType, str] = DataSourceType.FILE,
        path_to_data_source: Optional[Union[str, os.PathLike]] = None,
        path_to_local_db: Optional[Union[str, os.PathLike]] = None,
        local_database_name: Optional[str] = None,
        file_names: Optional[Sequence[str]] = None,
        file_type: Union[FileType, str] = FileType.TEXT,
        sep: str = " ",
        encoding: str = "iso-8859-1",
        database_type: Optional[Union[DatabaseType, str]] = None,
        database_name: Optional[str] = None,
        server: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        driver: Optional[str] = None,
        # table/time/filter args intentionally omitted for now (commented in your code)
        linked_server_name: Optional[str] = None,
        linked_server_provider: Optional[str] = None,
        linked_server_datasource: Optional[str] = None,
        *,
        overwrite: bool = False,
        ) -> None:

        """
        initialisation of a DataBase object from a source of raw data.
        create a local database based on the raw data from either file(s) or a database source.

        """
        self.data_source = DataSourceType(str(data_source))

        if isinstance(file_type, FileType):
            self.file_type = file_type
        else:
            self.file_type = FileType(str(file_type).lower())



        self.database_type = DatabaseType(str(database_type)) if database_type else None


        # Assign simple attributes
        self.file_names = list(file_names) if file_names is not None else None
        self.sep = sep
        self.encoding = encoding
        self.database_name = database_name
        self.server = server or "localhost"
        self.user = user
        self.password = password
        self.port = port
        self.driver = driver
        self.linked_server_name = linked_server_name
        self.linked_server_provider = linked_server_provider
        self.linked_server_datasource = linked_server_datasource


        # Validate and prepare paths
        vp = self._validate_and_prepare_paths(path_to_data_source, path_to_local_db, local_database_name)
        self.path_to_data_source = vp.raw_data_path
        self.path_to_local_db = vp.local_db_dir
        self.local_database_name = vp.local_db_path.name


        # Validate source-specific requirements
        self._validate_source_requirements()


        # Historian flag
        self.is_historian = self.database_type == DatabaseType.HISTORIAN


        # Create or reuse local SQLite DB file
        db_file = (vp.local_db_path.with_suffix(".db"))
        if db_file.exists():
            if overwrite:
                db_file.unlink()
            # else: keep existing DB
        else:
            # Ensure directory exists
            db_file.parent.mkdir(parents=True, exist_ok=True)
            # Touch to ensure a file exists (SQLAlchemy will also create as needed)
            db_file.touch()


        # Create SQLAlchemy engine for local SQLite
        self.engine = self._create_local_sqlite_engine(db_file)

        self._setup_logger()
        # # Setup logger
        # self.logger = logging.getLogger('Database')
        # if not self.logger.handlers:
        #     handler = logging.StreamHandler()
        #     formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        #     handler.setFormatter(formatter)
        #     self.logger.addHandler(handler)
        #     self.logger.setLevel(logging.INFO)


    def _setup_logger(self):
        """Configure both in-memory and file logging."""
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", f"database.log")

        # Create a StringIO stream for in-memory logging
        self._log_stream = io.StringIO()

        # Create and configure logger
        self.logger = logging.getLogger("database")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers (prevents duplicates in interactive runs)
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(file_handler)

        # In-memory handler
        memory_handler = logging.StreamHandler(self._log_stream)
        memory_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(memory_handler)

        self.logger.info(f"Logger initialized for database")

    def get_logs(self, as_text=False):
        """Return all logged messages from memory."""
        self._log_stream.seek(0)
        logs = self._log_stream.read()
        return logs if as_text else logs.splitlines()

    def save_logs(self, path: Optional[str] = None):
        """Save logs from memory to a text file."""
        if path is None:
            os.makedirs("logs", exist_ok=True)
            path = os.path.join("logs", f"database_session.log")

        with open(path, "w") as f:
            f.write(self.get_logs(as_text=True))

        self.logger.info(f"Logs saved to {path}")
        return path
     
                
    # --- Helpers ---
    @staticmethod
    def _validate_and_prepare_paths(
        path_to_data_source: Optional[Union[str, os.PathLike]],
        path_to_local_db: Optional[Union[str, os.PathLike]],
        local_database_name: Optional[str],
        ) -> _ValidatedPaths:
        raw_path = Path(path_to_data_source) if path_to_data_source is not None else None
        if raw_path is not None and not raw_path.exists():
            raise FileNotFoundError(f"path_to_data_source does not exist: {raw_path}")


        # Default local DB dir to HOME
        local_dir = Path(path_to_local_db) if path_to_local_db else Path(os.getenv("HOME", "."))
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.exists():
            local_dir.mkdir(parents=True, exist_ok=True)


        db_name = (local_database_name or "local_database").strip()
        if not db_name:
            raise ValueError("local_database_name must be a non-empty string")


        # Store path without suffix; we will append .db later
        local_db_path = (local_dir / db_name)
        return _ValidatedPaths(raw_data_path=raw_path, local_db_dir=local_dir, local_db_path=local_db_path)


    def _validate_source_requirements(self) -> None:
        if self.data_source == DataSourceType.DATABASE:
            if not self.database_type or not self.database_name:
                raise ValueError("For data_source='database', 'database_type' and 'database_name' must be provided")
        elif self.data_source == DataSourceType.FILE:
            if self.path_to_data_source is None or self.file_names is None:
                raise ValueError("For data_source='file', 'path_to_data_source' and 'file_names' must be provided")
        # Optional: check that the files exist (same extension assumed)
            missing = [fn for fn in self.file_names if not (self.path_to_data_source / fn).exists()]
            if missing:
                raise FileNotFoundError(f"The following files were not found: {missing}")
        else:
            raise ValueError("data_source must be 'file' or 'database'")
                
    @staticmethod
    def _create_local_sqlite_engine(db_file: Path) -> Engine:
        # Proper SQLAlchemy SQLite URL
        url = f"sqlite+pysqlite:///{db_file.as_posix()}"
        return create_engine(url, future=True)


    # --- Convenience properties ---
    @property
    def local_db_file(self) -> Path:
        return (self.path_to_local_db / f"{self.local_database_name}.db").resolve()    
        
        
    def extract_data_from_source(
        self,
        table_name: Optional[str] = None,
        time_column_name: Optional[str] = None,
        variable_column_names: Optional[Union[str, Sequence[str]]] = None,
        select_all: bool = True,
        starttime: Optional[Union[str, pd.Timestamp]] = None,
        endtime: Optional[Union[str, pd.Timestamp]] = None,
        filter_tagnames_column: Optional[str] = None,
        filter_tagnames: Optional[Union[str, Sequence[str]]] = None,
        filter_eq: Optional[Dict[str, Any]] = None,
        filter_gt: Optional[Dict[str, Any]] = None,
        filter_lt: Optional[Dict[str, Any]] = None,
        filter_geq: Optional[Dict[str, Any]] = None,
        filter_leq: Optional[Dict[str, Any]] = None,
        *,
        target_table: str = "raw_data",
        if_exists: str = "replace", # or "append"
        chunksize: Optional[int] = None,
        ) -> int:
        """
        Extract data from the configured *source* (files or RDBMS) and load it
        into the local SQLite database table `target_table`.


        Returns number of rows loaded.
        """
        opts = ExtractOptions(
        table_name=table_name,
        time_column_name=time_column_name,
        variable_column_names=variable_column_names,
        select_all=select_all,
        starttime=starttime,
        endtime=endtime,
        filter_tagnames_column=filter_tagnames_column,
        filter_tagnames=filter_tagnames,
        filter_eq=filter_eq,
        filter_gt=filter_gt,
        filter_lt=filter_lt,
        filter_geq=filter_geq,
        filter_leq=filter_leq,
        target_table=target_table,
        )   
        if self.data_source == DataSourceType.FILE:
            df = self._read_from_file()
        elif self.data_source == DataSourceType.DATABASE:
            if not opts.table_name:
                raise ValueError("table_name must be provided for data_source='database'")
            df = self._read_from_database(opts)
        else:
            raise ValueError("Unsupported data_source")


        # Auto-detect time column if not provided
        if not opts.time_column_name:
            detected = self._check_time_column(df)
            if detected:
                opts.time_column_name = detected
                self.logger.info("Detected time column: %s", detected)


        if df.empty:
            self.logger.warning("No data returned from source.")
            if if_exists == "replace":
                pd.DataFrame().to_sql(opts.target_table, self.engine, if_exists="replace", index=False)
            return 0


        # Standardize time dtype if we have a candidate
        if opts.time_column_name and opts.time_column_name in df.columns:
            df = df.copy()
            df[opts.time_column_name] = pd.to_datetime(df[opts.time_column_name], errors="coerce")


        n = int(df.shape[0])
        df.to_sql(opts.target_table, self.engine, if_exists=if_exists, index=False, chunksize=chunksize)
        self.logger.info("Loaded %d rows into local table '%s'", n, opts.target_table)
        
        df = df.set_index(opts.time_column_name)
        self._raw_data = df

        return n
        
    
    # def extract_data_from_source(self, table_name=None, time_column_name=None, variable_column_names=None, select_all=True, starttime=None, endtime=None, 
    #                              filter_tagnames_column=None, filter_tagnames=None, filter_eq=None, filter_gt=None, filter_lt=None, filter_geq=None, filter_leq=None):
    #     """ 
    #     extract data from the specified source (file or database) and 
    #     store them in the _raw_data table in the local database


    #     Returns
    #     -------
    #     None

        
    #     """
    #     self.table_name = table_name
    #     self.time_column = time_column_name
    #     self.time_series_column = variable_column_names
    #     self.select_all = select_all

    #     # query options
    #     self.starttime = starttime
    #     self.endtime = endtime
    #     self.filter_tagnames_column = filter_tagnames_column
    #     self.filter_eq = filter_eq
    #     self.filter_tagnames = filter_tagnames
    #     self.filter_gt = filter_gt
    #     self.filter_lt = filter_lt
    #     self.filter_geq = filter_geq
    #     self.filter_leq = filter_leq
        
    #     if self.data_source == 'file':
    #         self._raw_data = self._read_from_file()
    #     elif self.data_source == 'database':
    #         assert self.table_name is not None,  'The variable "table_name" should be defined for data source of type "database".'
    #         self._raw_data = self._read_from_database()
        
    #     if self.time_column is None:
    #         self._check_time_column()
    #     print(self._raw_data)
    #     write_to_db(self._raw_data, table_name, self.connection)

    #     print(f'Raw data has been succesfully extracted from the source of type "{self.data_source}" and stored in the "{table_name}" table of the "{self.local_database_name}" database!')

    # def _read_from_file(self):
    #     """ 
    #     read the raw data from the specified source files


    #     Returns
    #     -------
    #     a pandas dataframe containing the raw data read from the file source

        
    #     """
        
    #     _raw_data = read_files(self._raw_data_path, self.file_names, self.file_type, self.file_sep, self.file_encoding)

    #     # if self.time_column is not None:    
    #     #     _raw_data = _raw_data.rename(columns={self.time_column : 'Datetime'})

    #     return _raw_data
    def _read_from_file(self) -> pd.DataFrame:
        if not self.path_to_data_source or not self.file_names:
            raise ValueError("path_to_data_source and file_names must be set for file source")


        
        df = read_files(
            self.path_to_data_source,
            list(self.file_names),
            str(self.file_type.value if hasattr(self.file_type, "value") else self.file_type),
            self.sep,
            self.encoding,
            )
        if not isinstance(df, pd.DataFrame):
            raise TypeError("read_files must return a pandas.DataFrame")
        self.logger.info("Read %d rows via legacy read_files()", len(df))
        return df


        

    # def _read_from_database(self):
    #     """ 
    #     read the raw data from the specified source database


    #     Returns
    #     -------
    #     a pandas dataframe containing the raw data read from the database source

        
    #     """
    #     conn_source = create_database_connection(self.database_type, self.database_name, self._raw_data_path, 
    #                                                        server=self.server, user=self.user, password=self.password, port=self.port, driver=self.driver,
    #                                                        linked_server_name=self.linked_server_name, linked_server_provider=self.linked_server_provider, linked_server_datasource=self.linked_server_datasource)
        
    #     _raw_data = create_dataframe(conn_source, self.table_name, self._historian, self.time_column, self.time_series_column, self.select_all, self.starttime, self.endtime, 
    #                                  self.filter_tagnames_column, self.filter_eq, self.filter_tagnames, self.filter_gt, self.filter_lt, self.filter_geq, self.filter_leq)
            
    #     # if self.time_column is not None:
    #     #     _raw_data = _raw_data.rename(columns={self.time_column : 'Datetime'})

    #     return _raw_data
    def _read_from_database(self, opts: ExtractOptions) -> pd.DataFrame:
        conn_source = create_database_connection(
            self.database_type.value if self.database_type else None,
            self.database_name,
            self.path_to_data_source, # aligns with your original _raw_data_path
            server=self.server,
            user=self.user,
            password=self.password,
            port=self.port,
            driver=self.driver,
            linked_server_name=self.linked_server_name,
            linked_server_provider=self.linked_server_provider,
            linked_server_datasource=self.linked_server_datasource,
            )
        
        df = create_dataframe(
            conn_source,
            opts.table_name,
            self.is_historian,
            opts.time_column_name,
            opts.variable_column_names,
            opts.select_all,
            opts.starttime,
            opts.endtime,
            opts.filter_tagnames_column,
            opts.filter_eq,
            opts.filter_tagnames,
            opts.filter_gt,
            opts.filter_lt,
            opts.filter_geq,
            opts.filter_leq,
            )
        if not isinstance(df, pd.DataFrame):
            raise TypeError("create_dataframe must return a pandas.DataFrame")
        self.logger.info("Read %d rows via legacy create_dataframe()", len(df))
        return df
    
    
    # def _check_time_column(self):
    #     """
    #     checks which column in the dataframe contains datetime

    #     """
    #     for col in self._raw_data.columns:
    #         if self._raw_data[col].dtype == 'object':
    #             try:
    #                 _ = pd.to_datetime(self._raw_data[col], format='mixed')
    #                 self.time_column = col
    #             except ValueError:
    #                 pass
    @staticmethod
    def _check_time_column(df: pd.DataFrame, *, min_parse_ratio: float = 0.8) -> Optional[str]:
        """Return the name of a likely time column or None.


        Tries: object columns, then numeric epoch-like columns (s/ms/ns).
        Accepts column if >= `min_parse_ratio` of non-null values parse to datetime.
        """
        candidates = list(df.columns)
        # 1) object-like string timestamps
        for col in candidates:
            if df[col].dtype == "object":
                s = pd.to_datetime(df[col], errors="coerce", format="mixed")
                non_null = s.notna().sum()
                total = df[col].notna().sum()
                if total > 0 and (non_null / total) >= min_parse_ratio:
                    return col
        # 2) numeric epochs
        for col in candidates:
            if pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]):
                s = pd.to_datetime(df[col], errors="coerce", unit="s")
                if s.notna().mean() >= min_parse_ratio:
                    return col
                s = pd.to_datetime(df[col], errors="coerce", unit="ms")
                if s.notna().mean() >= min_parse_ratio:
                    return col
                s = pd.to_datetime(df[col], errors="coerce", unit="ns")
                if s.notna().mean() >= min_parse_ratio:
                    return col
        return None


    
    def write_to_db(
        self,
        data: pd.DataFrame,
        table_name: str,
        method: IfExists = "replace",
        *,
        index: bool = False,
        index_label: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[Dict[str, "sqlalchemy.types.TypeEngine"]] = None,
    ) -> int:
        """
        Write a DataFrame to a table in the *local* SQLite (self.engine).

        Returns the number of rows written.
        """
        with self.engine.begin() as conn:
            data.to_sql(
                name=table_name,
                con=conn,
                if_exists=method,
                index=index,
                index_label=index_label,
                chunksize=chunksize,
                dtype=dtype,
            )
        return int(len(data))
    
    def query_from_db(
        self,
        sql_command: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        index_column: Optional[Union[str, Sequence[str]]] = None,
        date_column_name: Optional[Union[str, Sequence[str]]] = None,
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Run a SELECT against the *local* SQLite and return a DataFrame.

        Use `params` for safe parameter binding: WHERE ts BETWEEN :t0 AND :t1
        """
        with self.engine.connect() as conn:
            return pd.read_sql_query(
                sql=text(sql_command),
                con=conn,
                params=params,
                index_col=index_column,
                parse_dates=date_column_name,
                chunksize=chunksize,
            )
        
    def list_tables(self) -> List[str]:
        """List tables in the local SQLite."""
        insp = inspect(self.engine)
        return insp.get_table_names()
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists in the local SQLite."""
        insp = inspect(self.engine)
        return insp.has_table(table_name, schema=schema)


    def head_table(self, table_name: str, n: int = 5) -> pd.DataFrame:
        """Return the first n rows from a table."""
        sql = f"SELECT * FROM {table_name} LIMIT {int(n)}"
        with self.engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn)


    def count_rows(self, table_name: str) -> int:
        """Return row count for a table."""
        sql = f"SELECT COUNT(*) AS n FROM {table_name}"
        with self.engine.connect() as conn:
            return int(pd.read_sql_query(text(sql), conn)["n"].iat[0])
        
    def ensure_index(
        self,
        table: str,
        index_name: str,
        columns: Sequence[str],
        unique: bool = False,
    ) -> None:
        """Create an index on a table if it does not exist."""
        cols = ", ".join(columns)
        sql = f"CREATE {'UNIQUE ' if unique else ''}INDEX IF NOT EXISTS {index_name} ON {table} ({cols})"
        with self.engine.begin() as conn:
            conn.exec_driver_sql(sql)


    def delete_where(
        self,
        table: str,
        where_sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Delete rows from a table with a WHERE clause.

        Example: delete_where('_raw_data', 'ts < :t', {'t': '2024-01-01'})
        """
        sql = f"DELETE FROM {table} WHERE {where_sql}"
        with self.engine.begin() as conn:
            res = conn.execute(text(sql), params or {})
            return int(res.rowcount or 0)
    
    @property
    def raw_data(self) -> pd.DataFrame:
        """
        returns a dataframe containing the raw data

        """
        if hasattr(self, "_raw_data") and isinstance(self._raw_data, pd.DataFrame):
            return self._coerce_time_index(self._raw_data)

        return AttributeError('Database has no raw data!')
    
    @property
    def simulation_data(self) -> pd.DataFrame:
        """
        returns a dataframe containing the simulation data

        """
        if hasattr(self, "_simulation_data") and isinstance(self._simulation_data, pd.DataFrame):
            return self._coerce_time_index(self._simulation_data)

        return AttributeError('Database has no simulation data!')
    
    @property
    def resampled_data(self) -> pd.DataFrame:
        """
        returns a dataframe containing the resampled data

        """
        if hasattr(self, "_resampled_data") and isinstance(self._resampled_data, pd.DataFrame):
            return self._coerce_time_index(self._resampled_data)

        return AttributeError('Database has no resampled data!')
    
    @property
    def formatted_data(self) -> pd.DataFrame:
        """
        returns a dataframe containing the formatted data

        """

        if hasattr(self, "_formatted_data") and isinstance(self._formatted_data, pd.DataFrame):
            return self._coerce_time_index(self._formatted_data)

        return AttributeError('Database has no formatted data!')
    
    @property
    def index_name(self) -> str:
        """
        returns name of the time column

        """

        return self._raw_data.index.name
    

    # @property
    # def formatted_data(self):
    #     """
    #     returns a dataframe containing the formatted data

    #     """
    #     try:
    #         return self._raw_data_formatted 
    #     except:
    #         raise AttributeError('Formatted data does not exist! First try "format_data".')
        
    # @property
    # def simulation_data(self):
    #     """
    #     returns a dataframe containing the formatted data

    #     """
       
    #     try:
    #         return self._raw_data_simulation
    #     except:
    #         print('The simulation data is the same as the formatted data.')
    #         return self.formatted_data


    # @property
    # def file_source_params(self):
    #     """
    #     returns the dictionary containing the file source parameters
        
    #     """

        
    #     file_params = {'Path to the raw data files' : self._raw_data_path,
    #                     'Data file names' : self.file_names,
    #                     'Data file type' : self.file_type,
    #                     'Data file separator' : self.file_sep,
    #                     'Data file encoding' : self.file_encoding}
        

    #     # if print_to_screen:
    #     #     print(f'List of file data source parameters: \n')
    #     #     for k, v in file_params.items():
    #     #         print(f'{k} = {v}')


    #     return file_params

    # @property
    # def database_query_params(self):
    #     """
    #     returns the dictionary containing the database query parameters
        
    #     """
        
    #     query_params = {'Database type' : self.database_type,
    #                     'Database name' : self.database_name, 
    #                     'Table name' : self.table_name,
    #                     'Query start time' : self.starttime, 
    #                     'Query end time' : self.endtime, 
    #                     'Query filter name' : self.filter_tagnames_column, 
    #                     'Query filter values' : self.filter_eq,  
    #                     'Time column name' : self.time_column, 
    #                     'Time series column name' : self.time_series_column, 
    #                     'Select all' : self.select_all}
        
    #     # if print_to_screen:
    #     #     print(f'List of database query parameters: \n')
    #     #     for k, v in query_params.items():
    #     #         print(f'{k} = {v}')


    #     return query_params


    # @property
    # def database_config_params(self):
    #     """
    #     returns the dictionary containing the database configuration parameters
        
    #     """

    #     config_params = {'Server name' : self.server,
    #                      'Username' : self.user,
    #                      'Password' : self.password,
    #                      'Port' : self.port,
    #                      'Driver' : self.driver}
        
    #     if self._historian:
    #         config_params['Linked server name'] = self.linked_server_name
    #         config_params['Linked server provider'] = self.linked_server_provider
    #         config_params['Linked server datasource'] = self.linked_server_datasource
  

    #     # if print_to_screen:
    #     #     print('List of database configuration parameters: \n')
    #     #     for k, v in config_params.items():
    #     #         print(f'{k} = {v}')

    #     return config_params

    # @property
    # def database_path(self):
    #     """
    #     returns the path to the stored database
        
    #     """
    #     return self.path_to_local_db
    

    # @property
    # def tables(self):

    #     cur = self.connection.cursor()
    #     cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    #     tables = cur.fetchall()

    #     tables_list = []
    #     for table in tables:
    #         tables_list.append(table[0])

    #     cur.close()

    #     return tables_list
    
    # @property
    # def time_column_name(self):
    #     return self.time_column
    
    # @time_column_name.setter
    # def time_column_name(self, name):
    #     self.time_column = name

    
    # def set_time_index(self, idx, name=None):

    #     if isinstance(idx, str):
    #         self.time_column = idx
    #         self._raw_data.set_index(idx, inplace=True, drop=True)
            
    #     elif isinstance(idx, int):
    #         self.time_column = self._raw_data.columns[idx]
    #         self._raw_data.set_index(self._raw_data.iloc[:, idx], inplace=True, drop=True)
            

    #     if name is not None:
    #         self._raw_data.index.names = [name]
    #         self.time_column = name


    # def get_data_from_table(self, table_name):

    #     df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)

    #     return df
    
    # def rename_table(self, old_name, new_name):
    #     cur = self.connection.cursor()
    #     assert old_name in self.tables(), f"{old_name} table does not exist in the database! "
        
    #     cur.execute(f"RENAME TABLE {old_name} TO {new_name}")
    #     print(f'The name of {old_name} has been changed to {new_name}')
            
    #     cur.close()

    
    # def run_query(self, sql_cmd):

    #     df = pd.read_sql_query(f"{sql_cmd}", self.connection)

    #     return df

    def _ensure_dtindex(self, df: pd.DataFrame, *, time_col: str = "Datetime") -> pd.DataFrame:
        """Ensure a DatetimeIndex; if absent, try to build from `time_col`."""
        if isinstance(df.index, pd.DatetimeIndex):
            return df.sort_index()
        if time_col in df.columns:
            out = df.copy()
            out[time_col] = pd.to_datetime(out[time_col], errors="raise")
            return out.set_index(time_col, drop=True).sort_index()
        raise ValueError("Data must have a DatetimeIndex or a time column (default 'ts').")

    def _rule_from_freq_unit(self, resampling_freq: int, resampling_unit: str) -> str:
        unit = resampling_unit.strip().lower()
        if unit in ("minute", "min"):
            suffix = "T"             # minutes
        elif unit == "hour":
            suffix = "H"
        elif unit == "day":
            suffix = "D"
        elif unit == "week":
            suffix = "W"
        elif unit == "month":
            suffix = "M"
        else:
            raise ValueError("Invalid resampling unit. Use: minute/min, hour, day, week, month.")
        return f"{int(resampling_freq)}{suffix}"

    def resample_data(
        self,
        data: pd.DataFrame,
        resampling_freq: Optional[int] = None,
        resampling_unit: Optional[str] = None,
        *,
        rule: Optional[str] = None,                       # e.g. "15min", "1H" (overrides freq+unit if given)
        how: Optional[AggName] = None,                    # aggregation name (None = asfreq)
        agg: Optional[Dict[str, Union[AggName, Sequence[AggName]]]] = None,  # per-column agg
        fill: Optional[FillName] = None,                  # ffill/bfill/interpolate after resample
        limit: Optional[int] = None,                      # max consecutive fill steps (for ffill/bfill)
        origin: Optional[str] = None,                     # pandas resample origin
        offset: Optional[str] = None,                     # "30min" alignment offset
        label: Literal["left", "right"] = "left",
        closed: Literal["left", "right"] = "left",
        time_col: str = "Datetime",
        out_table: str = "resampled_data",
        write_method: Literal["fail", "replace", "append"] = "replace",
    ) -> Tuple[pd.DataFrame, int]:
        """
        Resample a time-indexed DataFrame and persist the result to the local SQLite.

        Returns
        -------
        (resampled_df, rows_written)
        """
        # 1) Build resampling rule
        if rule is None:
            if resampling_freq is None or resampling_unit is None:
                raise ValueError("Provide either `rule='15min'` OR (`resampling_freq`, `resampling_unit`).")
            rule = self._rule_from_freq_unit(resampling_freq, resampling_unit)

        # 2) Ensure DatetimeIndex
        tsdf = self._ensure_dtindex(data, time_col=time_col)

        # 3) Perform resample
        if how is None and agg is None:
            # Pure calendar alignment without aggregation
            resampled = tsdf.asfreq(freq=rule)
        else:
            r = tsdf.resample(
                rule,
                origin=origin,        # None uses timestamp of first row
                offset=offset,        # allows shifting boundaries like "30min"
                label=label,
                closed=closed,
            )
            if agg is not None:
                resampled = r.agg(agg)
            else:
                # Single aggregation name over all numeric columns
                fn = getattr(pd.core.resample.Resampler, how, None)  # type: ignore[attr-defined]
                if how not in {"mean", "sum", "min", "max", "median", "first", "last"}:
                    raise ValueError("Unsupported `how`. Use one of mean,sum,min,max,median,first,last or provide `agg`.")
                resampled = getattr(r, how)()

        # 4) Optional gap filling
        if fill is not None:
            if fill == "ffill":
                resampled = resampled.ffill(limit=limit)
            elif fill == "bfill":
                resampled = resampled.bfill(limit=limit)
            elif fill == "interpolate":
                # numeric interpolation only; adjust method if needed
                resampled = resampled.interpolate(limit=limit)
            else:
                raise ValueError("Unsupported fill. Use: ffill, bfill, interpolate.")

        # 5) Write to DB (ensure a time column exists for storage)
        _to_write = resampled.copy()
        # if time_col not in _to_write.columns:
        #     _to_write[time_col] = _to_write.index
        to_write = _to_write.reset_index()
        # to_write = self._normalize_time_column(to_write, time_col=time_col, to_utc=False)
        self._resampled_data = to_write
        rows = self.write_to_db(
            data=to_write,
            table_name=out_table,
            method=write_method,
            index=False,
        )

        self.logger.info("Resampled to rule=%s, rows=%d, written to '%s'", rule, len(to_write), out_table)
        return resampled, rows

    # def resample_data(self, data, resampling_freq, resampling_unit):
    #     """
    #     resamples the raw data with the given frequency

    #     Parameters
    #     ----------
    #     data : pd.DataFrame
    #         a pandas dataframe containing the raw data 
    #     resampling_freq : int
    #         the frequency of resampling
    #     resampling_unit : str
    #         unit for resampling data, options: minute, hour, day, week or month
    #     Returns
    #     -------
    #     a pandas dataframe containing the resampled data, creates a new table in the database
    #     """

    #     assert data.index.inferred_type == 'datetime64', 'index of the data should be of datetime64 format!'
    
    #     if resampling_unit == 'minute' or resampling_unit == 'min':
    #         self._resampling_rule = str(resampling_freq) + resampling_unit[:3]
    #     elif resampling_unit in ['hour', 'day', 'week','month']:
    #         self._resampling_rule = str(resampling_freq) + resampling_unit[0].upper()
    #     else:
    #         print('Resampling unit is not valid! Please choose a valid unit \
    #                 from minute, hour, day, week or month')

    #     #data_resampled = data[::resampling_freq]
    #     self.data_resampled = data.asfreq(freq=self._resampling_rule)
    #     # write_to_db(data_resampled, 'formatted_data_resampled', self._conn, index=True)
    #     print("Formatted data have been successfully resampled to {} {}!".format(resampling_freq, resampling_unit))
        
    #     return self.data_resampled

    # def extract_simulation_data(self, data, start_date=None, end_date=None, simulation_period=None, simulation_mode='historical'):
    #     """
    #     returns the simulation data for the given period

    #     Parameters
    #     ----------
        
        
    #     Returns
    #     -------
    #     None
        
    #     """
    #     self.simulation_mode = simulation_mode
        
        
    #     try:
    #         if self.simulation_mode == 'real-time':
    #             self._simulation_period = simulation_period
    #             freq, unit = find_data_freq(self._simulation_period)
    #             last_idx = data.index[-1]
    #             first_idx = last_idx - pd.Timedelta(freq, unit)
    #             self._raw_data_simulation = data[data.index > first_idx]
    #         elif self.simulation_mode == 'historical':
    #             self._simulation_hist_start = parser.parse(start_date)
    #             self._simulation_hist_end = parser.parse(end_date)
    #             self._raw_data_simulation = data.loc[self._simulation_hist_start:self._simulation_hist_end]
    #     except:
    #         print('Either simulation mode is not valid or data is not available! Please choose "real-time" or "historical" \
    #             as the simulation mode or check the time frame of the data.')

    #     # self._dataset_sim = query_from_db("SELECT * from _raw_data_formatted WHERE strftime('%W', datetime) == '{}'".format(week_number), self._conn, None, None)

    #     write_to_db(self._raw_data_simulation, 'raw_data_simulation', self.connection)
        
    #     if simulation_mode == 'real-time':
    #         print(f'Simulation is in "{simulation_mode}" mode. Data for the last {simulation_period} has been successfully extracted and stored in the database in "raw_data_simulation" table.')
    #     elif simulation_mode == 'historical':
    #         print(f'Simulation is in "{simulation_mode}" mode. Data from {start_date} to {end_date} has been successfully extracted and stored in the database in "raw_data_simulation" table.')
    #     else:
    #         raise ValueError('Simulation mode is invalid! Options: "real-time", "historical".')
    def _normalize_time_column(
        self,
        df: pd.DataFrame,
        time_col: str = "Datetime",
        *,
        to_utc: bool = False,
        allow_na: bool = False,
    ) -> pd.DataFrame:
        """
        Ensure `time_col` is a datetime, optionally convert to UTC, and serialize to ISO8601 string
        so SQLite stores it losslessly and can parse it back.
        """
        if time_col not in df.columns:
            raise ValueError(f"time column '{time_col}' not in DataFrame")

        out = df.copy()
        ts = pd.to_datetime(out[time_col], errors="coerce", utc=to_utc)
        if not allow_na and ts.isna().any():
            n = int(ts.isna().sum())
            raise ValueError(f"{n} rows have unparsable '{time_col}' values")

        # Store as ISO string; pandas + sqlite is happiest with TEXT timestamps
        out[time_col] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z").str.replace(r"(\+00:00)?$", "", regex=True)
        return out
    
    def _coerce_time_index(
        self,
        data: pd.DataFrame,
        time_col: str = "Datetime",
    ) -> pd.DataFrame:
        """
        Ensure `data` has a DatetimeIndex. If not, try to build one from `time_col`.
        """
        if isinstance(data.index, pd.DatetimeIndex):
            if data.index.tz is not None:
                # keep tz-aware; you can localize/convert upstream as needed
                return data.sort_index()
            return data.sort_index()

        if time_col in data.columns:
            out = data.copy()
            out[time_col] = pd.to_datetime(out[time_col], errors="raise")
            out = out.set_index(time_col, drop=True).sort_index()
            if time_col in out.columns:
                out = out.drop(columns=[time_col])
            return out

        raise ValueError(
            "Data must have a DatetimeIndex or a time column named "
            f"'{time_col}' to build the index."
        )
    
    def _parse_lookback(self, period: str) -> pd.Timedelta:
        """
        Parse common look-back notations to Timedelta:
        - '24h', '7d', '30m', '1h30m', 'PT15M' (ISO8601-ish), 'P1D'
        Falls back to pandas Timedelta if exact parsing not matched.
        """
        if not isinstance(period, str) or not period.strip():
            raise ValueError("simulation_period must be a non-empty string (e.g., '24h', '7d').")
        s = period.strip().lower()
        # Try pandas first: handles '7d', '24h', '30m', '1h30m', '2D', etc.
        try:
            return pd.Timedelta(s)
        except Exception:
            pass
        # ISO8601-ish (very light support)
        if s.startswith("pt") or s.startswith("p"):
            try:
                # let pandas try again; it actually handles many ISO forms
                return pd.Timedelta(s.upper())
            except Exception:
                pass
        raise ValueError(f"Unrecognized simulation_period format: {period!r}")



    def extract_simulation_data(
        self,
        data: pd.DataFrame = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        simulation_period: Optional[str] = None,
        simulation_mode: SimulationMode = "historical",
        out_table: str = "simulation_data",
        time_col: str = "Datetime",
        write_method: Literal["fail", "replace", "append"] = "replace",
    ) -> Tuple[pd.DataFrame, int]:
        """
        Slice a time series for simulation and persist it to the local DB.

        Parameters
        ----------
        data : pd.DataFrame
            Source data. Must have a DatetimeIndex or a time column (default 'ts').
        start_date, end_date : str | None
            Historical window bounds (inclusive). Used when simulation_mode='historical'.
            Parsed with dateutil.parser.parse.
        simulation_period : str | None
            Look-back window (e.g., '24h', '7d', '90m'). Used when simulation_mode='real-time'.
        simulation_mode : {'real-time','historical'}
            Selection mode.
        out_table : str
            Destination table name in local SQLite.
        time_col : str
            Name of the time column (used if DataFrame has no DatetimeIndex).
        write_method : {'fail','replace','append'}
            pandas.to_sql if_exists behavior for writing the slice.

        Returns
        -------
        (df_slice, rows_written)
            The sliced DataFrame and number of rows written to `out_table`.
        """

        if data is None:
            data = self._raw_data

        # 1) Ensure a DatetimeIndex
        tsdf = self._coerce_time_index(data, time_col=time_col)

        # 2) Slice by mode
        if simulation_mode == "real-time":
            if not simulation_period:
                raise ValueError("simulation_period is required for simulation_mode='real-time'.")
            lookback = self._parse_lookback(simulation_period)
            if tsdf.empty:
                raise ValueError("Input data is empty; cannot compute real-time window.")
            last_idx = tsdf.index[-1]
            first_idx = last_idx - lookback
            df_slice = tsdf.loc[tsdf.index > first_idx]
            self.logger.info(
                'Real-time slice: last=%s, lookback=%s, rows=%d',
                last_idx, lookback, len(df_slice)
            )

        elif simulation_mode == "historical":
            if not start_date or not end_date:
                raise ValueError("start_date and end_date are required for 'historical' mode.")
            start = dtparser.parse(start_date)
            end = dtparser.parse(end_date)
            if start > end:
                raise ValueError(f"start_date {start} is after end_date {end}.")
            # Pandas loc on DateTimeIndex is inclusive for strings; ensure both datetimes
            df_slice = tsdf.loc[start:end]
            self.logger.info(
                'Historical slice: start=%s, end=%s, rows=%d',
                start, end, len(df_slice)
            )

        else:
            raise ValueError("simulation_mode must be 'real-time' or 'historical'.")

        # 3) Persist to local DB
        if df_slice.empty:
            self.logger.warning("Simulation slice produced 0 rows; table '%s' will be replaced with empty set.", out_table)

        # write as a regular table; ensure time column exists for storage
        _to_write = df_slice.copy()
        # if time_col not in _to_write.columns:
        #     _to_write[time_col] = _to_write.index

        # Optional: enforce normalized time strings for SQLite friendliness
        to_write = _to_write.reset_index()
        # to_write = self._normalize_time_column(to_write, time_col=time_col, to_utc=False)
        self._simulation_data = to_write
        rows = self.write_to_db(
            data=to_write,
            table_name=out_table,
            method=write_method,
            index=False,              # don't persist the pandas index as an 'index' column
            index_label=None,
        )

        self.logger.info("Wrote %d simulation rows to table '%s'.", rows, out_table)
        return df_slice, rows 

    def format_data(
        self,
        data: pd.DataFrame,
        *,
        time_col: str = "Datetime",
        # keep behavior when dropping duplicate timestamps
        keep: Literal["first", "last"] = "last",
        # optionally restrict which columns to convert; by default convert all non-datetime columns
        numeric_columns: Optional[Sequence[str]] = None,
        out_table: str = "formatted_data",
        write_method: Literal["fail", "replace", "append"] = "replace",
    ) -> Tuple[pd.DataFrame, int]:
        """
        Format a time series DataFrame and persist to the local SQLite.

        Steps:
        1) Ensure DatetimeIndex (from existing index or `time_col`)
        2) Drop duplicate index entries
        3) Convert selected columns to float, coercing non-numeric -> NaN
        4) Ensure index name is 'Datetime'
        5) Write to `out_table` (with index_label='Datetime')
        Returns (formatted_df, rows_written)
        """
        if data.empty:
            self.logger.warning("format_data: received empty DataFrame.")
            # still create/replace an empty table with only the Datetime column
            empty = pd.DataFrame(index=pd.DatetimeIndex([], name="Datetime"))
            rows = self.write_to_db(empty.reset_index(), out_table, method=write_method, index=False)
            return empty, rows

        # 1) Ensure a DatetimeIndex (use existing index or build from `time_col`)
        if isinstance(data.index, pd.DatetimeIndex):
            tsdf = data.copy()
        else:
            if time_col not in data.columns:
                raise ValueError(
                    f"format_data: DataFrame has no DatetimeIndex and missing time_col '{time_col}'."
                )
            tsdf = data.copy()
            tsdf[time_col] = pd.to_datetime(tsdf[time_col], errors="coerce")
            if tsdf[time_col].isna().any():
                bad = int(tsdf[time_col].isna().sum())
                raise ValueError(f"format_data: {bad} rows have invalid '{time_col}' values.")
            tsdf = tsdf.set_index(time_col)

        # 2) Drop duplicate timestamps (keep=first/last)
        if tsdf.index.has_duplicates:
            before = len(tsdf)
            tsdf = tsdf[~tsdf.index.duplicated(keep=keep)]
            logger.info("format_data: dropped %d duplicate timestamps (keep=%s).", before - len(tsdf), keep)

        # 3) Convert data columns to float (coerce non-numeric -> NaN)
        # Decide which columns to convert
        if numeric_columns is None:
            # all non-datetime columns (index holds the datetime already)
            cols_to_convert = list(tsdf.columns)
        else:
            cols_to_convert = list(numeric_columns)

        # Apply conversion
        for col in cols_to_convert:
            if col in tsdf.columns:
                tsdf[col] = pd.to_numeric(tsdf[col], errors="coerce").astype(float)

        # 4) Ensure index name is 'Datetime'
        tsdf.index = pd.DatetimeIndex(tsdf.index, name="Datetime")

        # 5) Write to DB; store index as a proper Datetime column
        to_write = tsdf.reset_index()  # brings 'Datetime' out as a column
        self._formatted_data = to_write
        rows = self.write_to_db(
            data=to_write,
            table_name=out_table,
            method=write_method,
            index=False,                  # we already turned index into the 'Datetime' column
            index_label=None,
        )

        self.logger.info("format_data: wrote %d rows to table '%s'.", rows, out_table)
        return tsdf, rows

    # def format_data(self, resample=False, resampling_freq=5, \
    #     resampling_unit='minute', keep_original_freq=False, different_sim_period=False, simulation_mode='historical', \
    #     simulation_period=None, start_date=None, end_date=None):
    #     """
    #     formats the raw data

    #     Parameters
    #     ----------
    #     resample : bool
    #         indicates if there is a need to resample data
    #     resampling_freq : int
    #         frequency of the resampling 
    #     resampling_unit : str
    #         unit for resampling data, options: minute, hour, day, week or month
    #     different_sim_period : bool
    #         indicates whether simulation period is different than raw data time frame
    #     simulation_mode : str
    #         indicates the simulation mode, options: real-time or historical
    #     simulation_period : str
    #         defines the period for real time simulation
    #     start_date : list
    #         start date for the historical simulation, format: [Y,M,D,H,m]
    #     end_date : list
    #         end date for the historical simulation, format: [Y,M,D,H,m]
    #     Returns
    #     ------
    #     None, replaces the data with the resampled data

    #     """
    #     self.resample = resample
    #     self.resampling_freq = resampling_freq
    #     self.resampling_unit = resampling_unit
    #     self.different_sim_period = different_sim_period
    #     self.keep_original_freq = keep_original_freq
        
    #     # if self._resample:
    #     #     self.__raw_data_resampled = self._resample_data(self.__raw_data, self._resampling_freq, self._resampling_unit)
    #     #     self._dataset = ww.OnlineSensorBased(self.__raw_data_resampled, self._timecolumn)
    #     # else:
    #     #     self._dataset = ww.OnlineSensorBased(self.__raw_data, self._timecolumn) 
    #     self._raw_data.replace(r'^([A-Za-z]|_)+$', 'NaN', regex=True, inplace=True)
    #     self._dataset = ww.OnlineSensorBased(self._raw_data, self.time_column) 
    #     self._dataset.set_tag('_raw_data')

    #     #self.dataset.set_units()
    #     # self._dataset.replace(r'^([A-Za-z]|_)+$', 'NaN', regex=True, inplace=True)
    #     # self._dataset.to_datetime(self.time_column, time_format='%d-%m-%y %H:%M')
    #     self._dataset.to_datetime(self.time_column, time_format='mixed')

    #     self._dataset.set_index(self.time_column, key_is_time=True, drop=True, inplace=True)
    #     if self.time_column != 'Datetime':
    #         self._dataset.data.rename_axis('Datetime', axis='index', inplace=True)
    #     self._dataset.drop_index_duplicates()
    #     self._dataset.to_float()
    #     # print('Raw data have been successfully formatted!')
        
        
    #     if self.resample:
    #          self._raw_data_formatted = self.resample_data(self._dataset.data, self.resampling_freq, self.resampling_unit)
    #     else:
    #         self._raw_data_formatted = self._dataset.data
        
    #     write_to_db(self._raw_data_formatted, 'raw_data_formatted', self.connection, index=True)

    #     if self.keep_original_freq:
    #         if self.resample == True:
    #             write_to_db(self._dataset.data, 'raw_data_formatted_original_freq', self.connection, index=True)
    #             print('A copy of the formatted data with original frequency is stored in the database!')
    #         else:
    #             print('Data are not resampled yet! Try resampling first and change resample option to True.')
                
        
    #     print("Formatted data have been successfully stored in the database!")

    #     if self.different_sim_period:
    #         self.simulation_data(simulation_mode, simulation_period, start_date, end_date)

    #     return None


    # def add_external_data(self, path, file_name, table_name, time_column=None,
    #                       file_type='text', sep='\t', encoding='iso-8859-1'):
    #     """
    #     read external data from file(s) and store them in a new table in the database

    #     Parameters
    #     ----------
    #     path : str
    #         path to the data file
    #     file_name : list
    #         list of the name of the files to read
    #     table_name : str
    #         name of the table to store the new data in the database
    #     time_column : str
    #         the name of the column to be used as time
    #     file_type : str
    #         extension of the files to read, options: excel, text, csv
    #     sep : str
    #         the separating element necessary for reading text/csv files e.g. \t
    #     encoding : str
    #         encoding used for the data files
        
    #     Return
    #     ------
    #     None
    #     """

    #     _data = read_files(path, file_name, file_type, sep, encoding)

    #     if time_column is not None:
    #         _data.set_index(time_column, inplace=True)

    #     write_to_db(_data, table_name, self.connection, index=True)

    #     print(f"External data from {file_name} have been successfully load and stored in the database table '{table_name}'!")
    
    def add_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        time_col: Optional[str] = None,
        normalize_time: bool = True,
        method: Literal["fail", "replace", "append"] = "append",
        index: bool = False,
        index_label: Optional[str] = None,
        # --- indexing options ---
        create_index: bool = True,
        index_name: Optional[str] = None,
        index_columns: Optional[Sequence[str]] = None,
        unique: bool = False,
    ) -> Tuple[pd.DataFrame, int]:
        """
        Add an external DataFrame to the local SQLite database under `table_name`, with optional
        time normalization and index creation.

        Parameters
        ----------
        df : pd.DataFrame
            Data to write.
        table_name : str
            Destination table name.
        time_col : str | None, default None
            If provided and present in df, treated as the time column.
        normalize_time : bool, default True
            If True and `time_col` is given, convert to datetime and serialize to ISO8601 strings.
        method : {'fail','replace','append'}, default 'append'
            Behavior when table already exists.
        index : bool, default False
            Whether to persist the pandas index as a column.
        index_label : str | None, default None
            Column name for the persisted index if `index=True`.
        create_index : bool, default True
            Whether to create an index on the table after writing.
        index_name : str | None, default None
            Name of the index to create. If None, a sensible default is chosen.
        index_columns : Sequence[str] | None, default None
            Columns to index. If None and `time_col` is provided (and present), uses `[time_col]`.
        unique : bool, default False
            Create a UNIQUE index (fails on duplicates).

        Returns
        -------
        (df_written, rows_written)
            The DataFrame that was written (after optional normalization) and the row count.
        """
        if df.empty:
            self.logger.warning("add_dataframe: received empty DataFrame, nothing written.")
            return df, 0

        out = df.copy()

        # Normalize time column if requested
        if normalize_time and time_col and time_col in out.columns:
            out = self._normalize_time_column(out, time_col=time_col, to_utc=False)

        # Write to DB
        rows = self.write_to_db(
            data=out,
            table_name=table_name,
            method=method,
            index=index,
            index_label=index_label,
        )
        self.logger.info("add_dataframe: wrote %d rows to table '%s'.", rows, table_name)

        # Create index if requested
        if create_index:
            cols = list(index_columns) if index_columns else (
                [time_col] if time_col and time_col in out.columns else []
            )
            if cols:
                ix_name = index_name or f"ix_{table_name}_{'_'.join(cols)}"
                try:
                    self.ensure_index(table=table_name, index_name=ix_name, columns=cols, unique=unique)
                    self.logger.info("add_dataframe: created %sindex '%s' on %s(%s).",
                                "UNIQUE " if unique else "", ix_name, table_name, ", ".join(cols))
                except Exception as e:
                    self.logger.exception("add_dataframe: failed to create index '%s' on %s: %s",
                                    ix_name, table_name, e)

        return out, rows


