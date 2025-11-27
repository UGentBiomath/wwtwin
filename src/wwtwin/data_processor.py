"""
Class DataProcessor provides functionalities for automatic filtering
and filling the raw data and generating compatible input files for
model simulation.

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


import pandas as pd
import numpy as np
import datetime as dt
import copy
import warnings as wn

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union, Any, Iterable, Callable
import logging
import matplotlib.pyplot as plt
import json
import inspect

import os, io, zipfile, toml
from datetime import datetime

from wwdata import OnlineSensorBased




# logger = logging.getLogger(__name__)

FILTER_METHODS: Dict[str, str] = {
    "tag_nan": "tag_nan",
    "tag_doubles": "tag_doubles",
    "tag_extremes": "tag_extremes",
    "moving_slope": "moving_slope_filter",
    "moving_average": "moving_average_filter",
    "zscore": "zscore_filter",
    "stl_residual": "stl_filter",
    "iqr": "iqr_filter",
    "rolling_iqr": "rolling_iqr_filter",
    "hampel": "hampel_filter",
    "isolation_forest": "isolation_forest_filter",
    "pca": "pca_outlier_filter",
}

FILL_METHODS: Dict[str, str] = {
    "interpolation": "fill_missing_interpolation",
    "ratio": "fill_missing_ratio",
    "standard": "fill_missing_standard",
    "model": "fill_missing_model",
    "day_before": "fill_missing_daybefore",
    "kalman": "fill_missing_kalman",
    "arima": "fill_missing_arima",
    "gaussian": "fill_missing_gaussian",
}

StepKind = Literal["filter", "fill"]


@dataclass
class PipelineStep:
    kind: StepKind                
    key: str                      
    method: str                   
    column: str
    params: Dict[str, Any]
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PipelineStep":
        return PipelineStep(**d)


class DataProcessor:
    """
    Orchestrates filtering & gap-filling pipelines on an OnlineSensorBased instance.

    Default pipeline (per column):
      tag_nan → moving_average → zscore → iqr → interpolation (small gaps only)
    Remaining large gaps are left unfilled and reported.

    Extras:
      - Auto parameter suggestions (window, z-cutoff, iqr-factor, interpolation limit)
      - Brute-force tuning for filter and fill steps
      - Fill scoring uses OnlineSensorBased.check_filling_error()
      - Method maps via FILTER_METHODS / FILL_METHODS
    """
    

    def __init__(self, 
                 data: Union[pd.DataFrame, "OnlineSensorBased"],
                 *,
                 timedata_column: str = 'index',
                 data_type: str = 'WWTP',
                 experiment_tag: str = "pipeline",
                 time_unit: Optional[str] = None,
                 copy_input: bool = True,
                 
                 
                 ):
        if isinstance(data, OnlineSensorBased): 
            self.osb = data  
        else:
            df = data.copy() if (isinstance(data, pd.DataFrame) and copy_input) else data
            self.osb = OnlineSensorBased(data=df, timedata_column=timedata_column, data_type=data_type,
                                         experiment_tag=experiment_tag, time_unit=time_unit)
        
        
        self.osb.drop_index_duplicates(print_number=False)


        if not hasattr(self, "_original_data"):
            self._original_data = self.osb.data.copy()

        if not hasattr(self, "_original_osb"):
            self._original_osb = copy.deepcopy(self.osb)

        self.pipeline: List[PipelineStep] = []
        self.param_summaries: Dict[str, Dict[str, Any]] = {}


        self._setup_logger()

    
        

    def _setup_logger(self):
        """Configure both in-memory and file logging."""
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", f"processor.log")

        self._log_stream = io.StringIO()

        self.logger = logging.getLogger("processor")
        self.logger.setLevel(logging.INFO)

        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(file_handler)

        memory_handler = logging.StreamHandler(self._log_stream)
        memory_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        self.logger.addHandler(memory_handler)

        self.logger.info(f"Logger initialized for processor")

    def get_logs(self, as_text=False):
        """Return all logged messages from memory."""
        self._log_stream.seek(0)
        logs = self._log_stream.read()
        return logs if as_text else logs.splitlines()

    def save_logs(self, path: Optional[str] = None):
        """Save logs from memory to a text file."""
        if path is None:
            os.makedirs("logs", exist_ok=True)
            path = os.path.join("logs", f"processor_session.log")

        with open(path, "w") as f:
            f.write(self.get_logs(as_text=True))

        self.logger.info(f"Logs saved to {path}")
        return path


        

    # =========================
    # Heuristics / auto params
    # =========================
    @staticmethod
    def _sampling_interval_seconds(index: pd.DatetimeIndex) -> float:
        if not isinstance(index, pd.DatetimeIndex) or len(index) < 2:
            return np.nan
        diffs = np.diff(index.view("int64"))
        med_ns = np.median(diffs)
        return float(med_ns) / 1e9 if med_ns > 0 else np.nan

    @staticmethod
    def _suggest_window(series: pd.Series, ac_threshold: float = 0.2, max_lags: int = 240) -> int:
        try:
            from statsmodels.tsa.stattools import acf
        except Exception:
            return 11
        s = pd.to_numeric(series, errors="coerce").dropna()
        if len(s) < 20:
            return 7
        nlags = min(max_lags, max(20, len(s)//4))
        ac = acf(s, nlags=nlags, fft=True)
        below = np.where(ac < ac_threshold)[0]
        if len(below) == 0:
            return max(7, int(nlags * 0.1))
        return max(7, int(below[0]))

    @staticmethod
    def _suggest_z_cutoff(series: pd.Series, target_fraction: float = 0.01) -> float:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return 3.0
        med = s.median()
        mad = np.median(np.abs(s - med))
        if mad == 0:
            return 3.0
        z = (s - med) / (1.4826 * mad)
        return float(np.quantile(np.abs(z), 1 - target_fraction))

    @staticmethod
    def _suggest_iqr_factor(series: pd.Series, target_fraction: float = 0.99) -> float:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return 1.5
        q1, q3 = np.percentile(s, [25, 75])
        iqr = q3 - q1
        if iqr <= 0:
            return 1.5
        lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
        covered = ((s >= lower) & (s <= upper)).mean()
        if 0 < covered < target_fraction:
            return float(max(1.5 * (target_fraction / covered), 1.0))
        return 1.5

    @staticmethod
    def _suggest_interp_limit(series: pd.Series, index: pd.Index, max_seconds: float = 2*3600) -> int:
        is_na = series.isna()
        if is_na.sum() == 0:
            return 4
        grp = (is_na != is_na.shift()).cumsum()
        gap_len = is_na.groupby(grp).transform("size")[is_na]
        p90 = np.percentile(gap_len, 90) if len(gap_len) else 4
        limit_time = None
        if isinstance(index, pd.DatetimeIndex):
            delta = DataProcessor._sampling_interval_seconds(index)
            if np.isfinite(delta) and delta > 0:
                limit_time = max_seconds / delta
        limit = int(round(min(p90, limit_time))) if limit_time else int(round(p90))
        return max(2, limit)

    def auto_analyse_parameters(self, column: str) -> Dict[str, Any]:
        if column not in self.osb.data.columns:
            raise KeyError(f"Column '{column}' not in data.")
        s = self.osb.data[column]
        window = self._suggest_window(s)
        z_cut = self._suggest_z_cutoff(s)
        iqr_k = self._suggest_iqr_factor(s)
        interp_limit = self._suggest_interp_limit(s, self.osb.data.index)
        out = dict(
            window=int(window),
            z_cutoff=float(np.round(z_cut, 2)),
            iqr_factor=float(np.round(iqr_k, 2)),
            interp_limit=int(interp_limit),
        )
        self.param_summaries[column] = out
        return out

    # =========================
    # Utilities for gaps
    # =========================
    @staticmethod
    def _gap_clusters(mask: pd.Series) -> List[Tuple[int, int]]:
        mask = mask.fillna(False).to_numpy()
        if mask.size == 0:
            return []
        starts = np.flatnonzero(np.diff(np.r_[False, mask]) == 1)
        ends = np.flatnonzero(np.diff(np.r_[mask, False]) == -1) - 1
        return list(zip(starts, ends))

    def _warn_large_gaps(self, column: str, small_gap_limit: int, arange: Optional[Tuple[Any, Any]]) -> List[Tuple[pd.Timestamp, pd.Timestamp, int]]:
        mv = self.osb.meta_valid.reindex(self.osb.data.index)
        if column not in mv:
            return []
        scope = self.osb.data.index if arange is None else self.osb.data.loc[arange[0]:arange[1]].index
        mask = (mv.loc[scope, column] == "filtered")
        clusters = self._gap_clusters(mask)
        large: List[Tuple[pd.Timestamp, pd.Timestamp, int]] = []
        for s_i, e_i in clusters:
            length = e_i - s_i + 1
            if length > small_gap_limit:
                idx = scope
                large.append((idx[s_i], idx[e_i], length))
        if large:
            print(f"[warning] {len(large)} large gap(s) detected for '{column}' (> {small_gap_limit} pts):")
            for a, b, L in large:
                print(f"  - {a} → {b}  (len={L})")
        return large

    # =========================
    # Pipeline functions
    # =========================
    def clear_pipeline(self, reset_data: bool = True) -> None:
        self.pipeline.clear()

        if not reset_data or not hasattr(self, "osb"):
            return

    

        if hasattr(self, "_original_osb"):
            self.osb = copy.deepcopy(self._original_osb)
        else:
            self._original_osb = copy.deepcopy(self.osb)

        self.osb._filling_warning_issued = False
        self.osb._rain_warning_issued = False
        if hasattr(self.osb, "daily_profile"):
            self.osb.daily_profile.clear()

        if hasattr(self.osb, "_update_time"):
            self.osb._update_time()

        print("OnlineSensorBased object reset to original state.")


    def add_step(self, kind: StepKind, key: str, column: str, **params: Any) -> None:
        if kind == "filter":
            if key not in FILTER_METHODS:
                raise KeyError(f"Unknown filter key '{key}'.")
            method = FILTER_METHODS[key]
        else:
            if key not in FILL_METHODS:
                raise KeyError(f"Unknown fill key '{key}'.")
            method = FILL_METHODS[key]
        self.pipeline.append(PipelineStep(kind=kind, key=key, method=method, column=column, params=params))

    def enable_step(self, idx: int, enabled: bool = True) -> None:
        self.pipeline[idx].enabled = enabled

    def summary(self) -> pd.DataFrame:
        rows = []
        for i, s in enumerate(self.pipeline):
            rows.append({"idx": i, "enabled": s.enabled, "kind": s.kind, "key": s.key, "method": s.method, "column": s.column, "params": s.params})
        return pd.DataFrame(rows).set_index("idx")

    # =========================
    # Default pipeline
    # =========================
    def build_default_pipeline(
        self,
        columns: Optional[Sequence[str]] = None,
        *,
        arange: Optional[Tuple[Any, Any]] = None,
        small_gap_limit: Optional[int] = None,   
        interpolation_method: str = "time",      
    ) -> None:
        """
        Default (per column):
          tag_nan → moving_average → zscore → iqr → interpolation (small gaps only)
        """
        self.clear_pipeline()
        if columns is None:
            columns = [c for c in self.osb.data.columns if pd.api.types.is_numeric_dtype(self.osb.data[c])]

        for col in columns:
            params = self.auto_analyse_parameters(col)
            gap_limit = params["interp_limit"] if small_gap_limit is None else int(small_gap_limit)

            # tag NaNs
            self.add_step("filter", "tag_nan", col, arange=arange, clear=False)

            # moving average (smooth + dev filter)
            self.add_step(
                "filter", "moving_average", col,
                window=int(max(3, params["window"])),
                cutoff_frac=0.25,
                arange=arange,
                clear=False, final=False, inplace=False, plot=False
            )

            # z-score
            self.add_step(
                "filter", "zscore", col,
                k=float(params["z_cutoff"]),
                arange=arange,
                final=False, inplace=False, plot=False
            )

            # iqr
            self.add_step(
                "filter", "iqr", col,
                k=float(params["iqr_factor"]),
                arange=arange,
                final=False, inplace=False, plot=False
            )

            # fill small gaps by interpolation
            self.add_step(
                "fill", "interpolation", col,
                range_=int(gap_limit),
                arange=arange,
                method=interpolation_method if isinstance(self.osb.data.index, pd.DatetimeIndex) else "index",
                clear=False, plot=True
            )


    def _ensure_meta_column(self, column: str) -> None:
        """Make sure meta_valid exists, is aligned to data.index, and has a baseline 'original' column."""
        mv = getattr(self.osb, "meta_valid", None)
        if not isinstance(mv, pd.DataFrame):
            mv = pd.DataFrame(index=self.osb.data.index)

        mv = mv.reindex(self.osb.data.index)

        if column not in mv.columns:
            mv[column] = "original"
        else:
            mv[column] = mv[column].where(mv[column].isin(
                ["original", "filtered"] + [c for c in mv[column].unique() if str(c).startswith("filled_")]
            ), "original")

        mv[column] = mv[column].astype(object)

        self.osb.meta_valid = mv



    # =============================
    # Run data processing pipeline
    # =============================
    def run(self, dry_run: bool = False) -> List[Tuple[int, str, Dict[str, Any], Optional[str]]]:
        """
        Execute steps in order. Returns [(idx, method, params, error_or_none), ...].
        Warns about large gaps before the first fill for each column.
        """
        results: List[Tuple[int, str, Dict[str, Any], Optional[str]]] = []
        touched_cols = {s.column for s in self.pipeline if s.enabled}
        if touched_cols:
            self.osb.add_to_meta_valid(list(touched_cols))

        warned_cols: set = set()

        

        for i, step in enumerate(self.pipeline):
            if step.kind in ("filter", "fill"):
                self._ensure_meta_column(step.column)
            if not step.enabled:
                results.append((i, step.method, step.params, "disabled"))
                continue
            if step.column not in self.osb.data.columns:
                results.append((i, step.method, step.params, f"column '{step.column}' not found"))
                continue

            if step.kind == "fill":
                self.osb._add_to_meta(step.column)
                if not dry_run and step.column not in warned_cols:
                    gap_limit = int(step.params.get("range_", 4))
                    self._warn_large_gaps(step.column, gap_limit, step.params.get("arange"))
                    warned_cols.add(step.column)

            if dry_run:
                results.append((i, step.method, step.params, None))
                continue

            try:
                fn = getattr(self.osb, step.method)
            except AttributeError:
                results.append((i, step.method, step.params, "method not found on OnlineSensorBased"))
                continue

            call = dict(step.params)
            # normalize common flags your methods accept
            # call.setdefault("inplace", True)
            # if step.kind == "filter":
            #     call.setdefault("final", True)
            # try:
            #     fn(step.column, **call)
            #     results.append((i, step.method, step.params, None))
            # except Exception as e:
            #     results.append((i, step.method, step.params, f"error: {e}"))
            sig = inspect.signature(fn)
            supported = set(sig.parameters.keys())

            # Add defaults only if the method supports these flags
            if "inplace" in supported and "inplace" not in call:
                call["inplace"] = False
            if step.kind == "filter" and "final" in supported and "final" not in call:
                call["final"] = False

            try:
                fn(step.column, **call)
                results.append((i, step.method, step.params, None))
            except Exception as e:
                results.append((i, step.method, step.params, f"error: {e}"))


        self.logger.info("Data processing run is finished!")

        return results

    # =========================
    # Brute-force: filters
    # =========================
    def brute_force_filter_params(
        self,
        column: str,
        key: str,                                  
        param_grid: Dict[str, Sequence[Any]],
        *,
        base_kwargs: Optional[Dict[str, Any]] = None,
        score_fn: Optional[Callable[["OnlineSensorBased", str], float]] = None,
        score_mode: Literal["lower_is_better", "higher_is_better"] = "lower_is_better",
    ) -> Dict[str, Any]:
        """
        Brute-force a filter's params. Default score = fraction tagged 'filtered' (lower=better).
        Returns {"best_params": {...}, "score": float|None}
        """
        if key not in FILTER_METHODS:
            raise KeyError(f"Unknown filter key '{key}'.")
        method = FILTER_METHODS[key]
        base_kwargs = base_kwargs or {}

        if score_fn is None:
            def score_fn(osb: "OnlineSensorBased", col: str) -> float:
                mv = osb.meta_valid.reindex(osb.data.index)
                if col in mv:
                    return float((mv[col] == "filtered").mean())
                return 1.0

        data0 = self.osb.data.copy()
        mv0 = self.osb.meta_valid.copy()

        from itertools import product
        keys = list(param_grid.keys())
        vals = [list(v) for v in param_grid.values()]
        best, best_params = (np.inf if score_mode == "lower_is_better" else -np.inf), None

        for combo in product(*vals):
            self.osb.data = data0.copy()
            self.osb.meta_valid = mv0.copy()
            try:
                getattr(self.osb, method)(column, **{**base_kwargs, **dict(zip(keys, combo)), "final": True, "inplace": True, "plot": False})
                s = float(score_fn(self.osb, column))
                better = (s < best) if score_mode == "lower_is_better" else (s > best)
                if better:
                    best, best_params = s, dict(zip(keys, combo))
            except Exception:
                continue

        self.osb.data = data0
        self.osb.meta_valid = mv0

        return {"best_params": best_params or {}, "score": None if best_params is None else float(best)}

    # =========================
    # Brute-force: fills (uses check_filling_error)
    # =========================
    def brute_force_fill_params_with_reliability(
        self,
        column: str,
        key: str,                                   
        param_grid: Dict[str, Sequence[Any]],
        *,
        test_data_range: Tuple[Any, Any],
        nr_iterations: int = 5,
        nr_small_gaps: int = 4,
        max_size_small_gaps: int = 3,
        nr_large_gaps: int = 0,
        max_size_large_gaps: int = 0,
        base_options: Optional[Dict[str, Any]] = None,
        score_mode: Literal["lower_is_better", "higher_is_better"] = "lower_is_better",
    ) -> Dict[str, Any]:
        """
        Brute-force a filling method’s params using OnlineSensorBased.check_filling_error() as score.
        Returns {"best_params": {...}, "score": float|None}
        """
        if key not in FILL_METHODS:
            raise KeyError(f"Unknown fill key '{key}'.")
        method = FILL_METHODS[key]

        base_options = {"to_fill": column, **(base_options or {})}
        if "arange" not in base_options:
            base_options["arange"] = test_data_range

        data0 = self.osb.data.copy()
        mv0 = self.osb.meta_valid.copy()
        filled0 = getattr(self.osb, "filled", pd.DataFrame(index=self.osb.data.index)).copy()
        mf0 = getattr(self.osb, "meta_filled", pd.DataFrame(index=self.osb.data.index)).copy()

        from itertools import product
        keys = list(param_grid.keys())
        vals = [list(v) for v in param_grid.values()]
        best, best_params = (np.inf if score_mode == "lower_is_better" else -np.inf), None

        for combo in product(*vals):
            self.osb.data = data0.copy()
            self.osb.meta_valid = mv0.copy()
            self.osb.filled = filled0.copy()
            self.osb.meta_filled = mf0.copy()

            options = {**base_options, **dict(zip(keys, combo))}
            try:
                err = self.osb.check_filling_error(
                    nr_iterations=nr_iterations,
                    data_name=column,
                    filling_function=method,
                    test_data_range=test_data_range,
                    nr_small_gaps=nr_small_gaps,
                    max_size_small_gaps=max_size_small_gaps,
                    nr_large_gaps=nr_large_gaps,
                    max_size_large_gaps=max_size_large_gaps,
                    **options,
                )
                score_val = float(err) if isinstance(err, (int, float, np.floating)) else float(getattr(err, "avg_error", np.nan))
                if not np.isfinite(score_val):
                    continue
                better = (score_val < best) if score_mode == "lower_is_better" else (score_val > best)
                if better:
                    best, best_params = score_val, dict(zip(keys, combo))
            except Exception:
                continue

        self.osb.data = data0
        self.osb.meta_valid = mv0
        self.osb.filled = filled0
        self.osb.meta_filled = mf0

        return {"best_params": best_params or {}, "score": None if best_params is None else float(best)}
    
    


    def export_data(
        self,
        path: str,
        *,
        data_format: str = "csv",
        meta_format: str = "csv",
        summary_format: str = "toml",
        pipeline_meta_format: str = "toml",
        include_meta: bool = True,
        include_original: bool = False,
        include_summary: bool = True,
        include_pipeline_meta: bool = True,
        timestamp: bool = True,
        compress: bool = True,
    ):
        """
        Export processed data, metadata, summary statistics, and pipeline info.

        By default, exports all tables to CSV and summary/pipeline to TOML,
        packaged into a single ZIP archive.

        Parameters
        ----------
        path : str
            File path or directory for export (no extension required).
        data_format : {"csv","xlsx","parquet","json","pickle"}, default "csv"
            Format for filled and original datasets.
        meta_format : {"csv","xlsx","parquet","json","pickle"}, default "csv"
            Format for metadata (meta_valid, meta_filled).
        summary_format : {"toml","csv","json","xlsx"}, default "toml"
            Format for summary statistics.
        pipeline_format : {"toml","csv","json","xlsx"}, default "toml"
            Format for pipeline metadata.
        include_meta : bool, default True
            Include meta_valid and meta_filled.
        include_original : bool, default False
            Include the original (raw) dataset.
        include_summary : bool, default True
            Include summary statistics table.
        include_pipeline_info : bool, default True
            Include pipeline configuration and parameters.
        timestamp : bool, default True
            Append timestamp to output file name.
        zip_all : bool, default True
            Bundle all exported files into one ZIP archive.

        Returns
        -------
        str
            Path to exported ZIP or single file.
        """
        

        osb = self.osb
        df_out = getattr(osb, "filled", None)
        if df_out is None or df_out.empty:
            df_out = osb.data.copy()
            print("No filled data found; exporting raw dataset instead.")

        if os.path.isdir(path):
            base = os.path.join(path, "processed_data")
        else:
            base, _ = os.path.splitext(path)
        if timestamp:
            base += "_" + datetime.now().strftime("%Y%m%d_%H%M%S")

        outfile_zip = base + ".zip"
        os.makedirs(os.path.dirname(outfile_zip), exist_ok=True)

        def df_to_bytes(df, fmt: str):
            fmt = fmt.lower()
            bio = io.BytesIO()
            if fmt == "csv":
                df.to_csv(bio, index=True)
            elif fmt == "xlsx":
                with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                    df.to_excel(writer)
            elif fmt == "parquet":
                df.to_parquet(bio, index=True)
            elif fmt == "json":
                df.to_json(bio, orient="records", indent=2)
            elif fmt == "pickle":
                pd.to_pickle(df, bio)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
            return bio.getvalue()

        def to_bytes_toml(obj):
            return toml.dumps(obj).encode("utf-8")

        buffer_dict = {}

        buffer_dict[f"filled_data.{data_format}"] = df_to_bytes(df_out, data_format)

        if include_meta:
            buffer_dict[f"meta_valid.{meta_format}"] = df_to_bytes(osb.meta_valid, meta_format)
            buffer_dict[f"meta_filled.{meta_format}"] = df_to_bytes(osb.meta_filled, meta_format)

        if include_original:
            buffer_dict[f"original_data.{data_format}"] = df_to_bytes(osb.data, data_format)

        if include_summary:
            summary_rows = []
            for col in df_out.columns:
                meta = osb.meta_filled[col] if col in osb.meta_filled else None
                counts = meta.value_counts().to_dict() if meta is not None else {}
                total = len(df_out)
                filled = sum(v for k, v in counts.items() if str(k).startswith("filled"))
                filtered = counts.get("filtered", 0)
                orig = counts.get("original", 0)
                vals = df_out[col].dropna()
                summary_rows.append({
                    "column": col,
                    "count": total,
                    "original": orig,
                    "filtered": filtered,
                    "filled": filled,
                    "filled_percent": 100 * filled / total if total else np.nan,
                    "mean": vals.mean(),
                    "std": vals.std(),
                    "min": vals.min(),
                    "max": vals.max(),
                })
            summary_df = pd.DataFrame(summary_rows)

            if summary_format == "toml":
                summary_toml = summary_df.set_index("column").to_dict(orient="index")
                buffer_dict["summary.toml"] = to_bytes_toml({"summary": summary_toml})
            else:
                buffer_dict[f"summary.{summary_format}"] = df_to_bytes(summary_df, summary_format)

        if include_pipeline_meta and hasattr(self, "pipeline") and self.pipeline:
            pipeline_records = [
                {
                    "step": i + 1,
                    "kind": step.kind,
                    "method": step.method,
                    "column": step.column,
                    "parameters": step.params,
                }
                for i, step in enumerate(self.pipeline)
            ]

            if pipeline_meta_format == "toml":
                buffer_dict["pipeline.toml"] = to_bytes_toml({"pipeline": pipeline_records})
            else:
                pipeline_df = pd.DataFrame(pipeline_records)
                buffer_dict[f"pipeline.{pipeline_meta_format}"] = df_to_bytes(pipeline_df, pipeline_meta_format)

        if compress:
            with zipfile.ZipFile(outfile_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for name, content in buffer_dict.items():
                    zf.writestr(name, content)
            print(f"Export complete: {outfile_zip}")
            return outfile_zip
        else:
            main_file = base + f".{data_format}"
            with open(main_file, "wb") as f:
                f.write(buffer_dict[f"filled_data.{data_format}"])
            print(f"Exported main data only: {main_file}")
            return main_file


