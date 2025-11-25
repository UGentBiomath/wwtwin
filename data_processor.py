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


from tkinter import E
from tokenize import Name
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
    kind: StepKind                # "filter" | "fill"
    key: str                      # friendly key (e.g., "moving_average", "interpolation")
    method: str                   # actual OSB method name (e.g., "moving_average_filter")
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
    
#         self,
#         data: Union[pd.DataFrame, "OnlineSensorBased"],
#         *,
#         timedata_column: str = "index",
#         data_type: str = "WWTP",
#         experiment_tag: str = "pipeline",
#         time_unit: Optional[str] = None,
#         columns: Optional[Iterable[str]] = None,
#         copy_input: bool = True,
#     ) -> None:
#         """
#         Parameters
#         ----------
#         data : DataFrame or OnlineSensorBased
#             If DataFrame, we will construct an OnlineSensorBased around it.
#         columns : iterable[str], optional
#             Subset of columns to consider as default targets. Steps can still
#             override with their own `target`.
#         copy_input : bool
#             If True and data is a DataFrame, copy it to avoid mutation.
#         """
#         # Bring/Wrap OnlineSensorBased
#         if hasattr(data, "data") and hasattr(data, "meta_valid"):
#             # assume it's already an OnlineSensorBased
#             self.osb = data  # type: ignore[assignment]
#         else:
#             df = data.copy() if (isinstance(data, pd.DataFrame) and copy_input) else data
#             # You already defined OnlineSensorBased elsewhere; we just construct it:
#             self.osb = OnlineSensorBased(
#                 data=df, timedata_column=timedata_column, data_type=data_type,
#                 experiment_tag=experiment_tag, time_unit=time_unit
#             )
    def __init__(self, 
                 data: Union[pd.DataFrame, "OnlineSensorBased"],
                 *,
                 timedata_column: str = 'index',
                 data_type: str = 'WWTP',
                 experiment_tag: str = "pipeline",
                 time_unit: Optional[str] = None,
                 #  columns: Optional[Iterable[str]] = None,
                 copy_input: bool = True,
                 
                 
                 ):
        if isinstance(data, OnlineSensorBased): #hasattr(data, "data") and hasattr(data, "meta_valid"):
            # assume it's already an OnlineSensorBased
            self.osb = data  # type: ignore[assignment]
        else:
            df = data.copy() if (isinstance(data, pd.DataFrame) and copy_input) else data
            # You already defined OnlineSensorBased elsewhere; we just construct it:
            self.osb = OnlineSensorBased(data=df, timedata_column=timedata_column, data_type=data_type,
                                         experiment_tag=experiment_tag, time_unit=time_unit)
        
        # self.columns: List[str] = list(columns) if columns is not None else list(self.osb.data.columns)
        # if not hasattr(data, "data") or not isinstance(osb.data, pd.DataFrame):
        #     raise TypeError("`osb` must be an OnlineSensorBased/HydroData-like object with a .data DataFrame.")
        # self.osb = osb
        self.osb.drop_index_duplicates(print_number=False)


        if not hasattr(self, "_original_data"):
            self._original_data = self.osb.data.copy()

        if not hasattr(self, "_original_osb"):
            self._original_osb = copy.deepcopy(self.osb)

        self.pipeline: List[PipelineStep] = []
        self.param_summaries: Dict[str, Dict[str, Any]] = {}


        self._setup_logger()

    
        # # Setup logger
        # self.logger = logging.getLogger('Processor')
        # if not self.logger.handlers:
        #     handler = logging.StreamHandler()
        #     formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        #     handler.setFormatter(formatter)
        #     self.logger.addHandler(handler)
        #     self.logger.setLevel(logging.INFO)

    def _setup_logger(self):
        """Configure both in-memory and file logging."""
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", f"processor.log")

        # Create a StringIO stream for in-memory logging
        self._log_stream = io.StringIO()

        # Create and configure logger
        self.logger = logging.getLogger("processor")
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
    # Pipeline DSL
    # =========================
    def clear_pipeline(self, reset_data: bool = True) -> None:
        self.pipeline.clear()

        if not reset_data or not hasattr(self, "osb"):
            return

        # osb = self.osb

        # # --- full OSB reset ---
        # if hasattr(self, "_original_data"):
        #     # if we stored a deep copy of the raw data initially
        #     self.osb.data = self._original_data.copy()
        # else:
        #     # fallback: copy current data as baseline
        #     self._original_data = self.osb.data.copy()

        # # rebuild meta tables and filled/filling states
        # self.osb.meta_valid = pd.DataFrame(index=self.osb.data.index)
        # for c in self.osb.data.columns:
        #     self.osb.meta_valid[c] = "original"

        # self.osb.meta_filled = self.osb.meta_valid.copy()
        # self.osb.filled = pd.DataFrame(index=self.osb.data.index)
        # self.osb.filling_error = pd.DataFrame(index=self.osb.data.columns, columns=["imputation error [%]"])

        if hasattr(self, "_original_osb"):
            # if we stored a deep copy of the raw data initially
            self.osb = copy.deepcopy(self._original_osb)
        else:
            self._original_osb = copy.deepcopy(self.osb)

        # reset internal warning flags and daily profiles
        self.osb._filling_warning_issued = False
        self.osb._rain_warning_issued = False
        if hasattr(self.osb, "daily_profile"):
            self.osb.daily_profile.clear()

        # refresh cached index/time info
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
        small_gap_limit: Optional[int] = None,   # in samples
        interpolation_method: str = "time",      # for DatetimeIndex
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

        # Align index (fill new rows as 'original')
        mv = mv.reindex(self.osb.data.index)

        # Create the column if missing
        if column not in mv.columns:
            mv[column] = "original"
        else:
            # Replace any placeholders like '!!' with 'original'
            mv[column] = mv[column].where(mv[column].isin(
                ["original", "filtered"] + [c for c in mv[column].unique() if str(c).startswith("filled_")]
            ), "original")

        # Ensure dtype is string/object to avoid categorical surprises
        mv[column] = mv[column].astype(object)

        self.osb.meta_valid = mv



    # =========================
    # Run pipeline
    # =========================
    def run(self, dry_run: bool = False) -> List[Tuple[int, str, Dict[str, Any], Optional[str]]]:
        """
        Execute steps in order. Returns [(idx, method, params, error_or_none), ...].
        Warns about large gaps before the first fill for each column.
        """
        results: List[Tuple[int, str, Dict[str, Any], Optional[str]]] = []
        # make sure meta_valid exists for all touched columns
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

            # setup for fill steps
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
        key: str,                                  # e.g. "iqr", "zscore", "moving_average"
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

        # snapshot
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

        # restore
        self.osb.data = data0
        self.osb.meta_valid = mv0

        return {"best_params": best_params or {}, "score": None if best_params is None else float(best)}

    # =========================
    # Brute-force: fills (uses check_filling_error)
    # =========================
    def brute_force_fill_params_with_reliability(
        self,
        column: str,
        key: str,                                   # e.g. "interpolation", "kalman", ...
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
            # scorer needs arange to place synthetic gaps—default to test window
            base_options["arange"] = test_data_range

        # snapshot
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
                # normalize to float
                score_val = float(err) if isinstance(err, (int, float, np.floating)) else float(getattr(err, "avg_error", np.nan))
                if not np.isfinite(score_val):
                    continue
                better = (score_val < best) if score_mode == "lower_is_better" else (score_val > best)
                if better:
                    best, best_params = score_val, dict(zip(keys, combo))
            except Exception:
                continue

        # restore
        self.osb.data = data0
        self.osb.meta_valid = mv0
        self.osb.filled = filled0
        self.osb.meta_filled = mf0

        return {"best_params": best_params or {}, "score": None if best_params is None else float(best)}
    
    # def export_data(
    #     self,
    #     path: str,
    #     *,
    #     format: str = "csv",
    #     include_meta: bool = True,
    #     include_original: bool = False,
    #     include_summary: bool = True,
    #     include_pipeline_meta: bool = True,
    #     timestamp: bool = True,
    #     compress: bool = False,
    # ) -> str:
    #     """
    #     Export the processed dataset and optional metadata, summaries, and pipeline info.

    #     Parameters
    #     ----------
    #     path : str
    #         File path or directory for export.
    #     format : {"csv", "xlsx", "parquet", "json", "pickle"}, default "xlsx"
    #         Export format.
    #     include_meta : bool, default True
    #         Export meta_valid and meta_filled information.
    #     include_original : bool, default False
    #         Export raw/original dataset before processing.
    #     include_summary : bool, default True
    #         Export summary table of per-variable statistics.
    #     include_pipeline_info : bool, default True
    #         Include a record of filtering/filling methods and their parameters.
    #     timestamp : bool, default True
    #         Append a timestamp to the output file name.
    #     compress : bool, default False
    #         Compress file (ZIP for CSV/Excel, gzip for Parquet).

    #     Returns
    #     -------
    #     str
    #         Path to the exported file.
    #     """
     

    #     osb = self.osb
    #     df_out = getattr(osb, "filled", None)
    #     if df_out is None or df_out.empty:
    #         df_out = osb.data.copy()
    #         print("⚠️ No filled data found; exporting raw dataset instead.")

    #     # --- filename handling ---
    #     if os.path.isdir(path):
    #         base = os.path.join(path, "processed_data")
    #     else:
    #         base, _ = os.path.splitext(path)

    #     if timestamp:
    #         base += "_" + datetime.now().strftime("%Y%m%d_%H%M%S")

    #     fmt = format.lower()
    #     ext_map = {"csv": ".csv", "xlsx": ".xlsx", "parquet": ".parquet",
    #             "json": ".json", "pickle": ".pkl"}
    #     ext = ext_map.get(fmt, f".{fmt}")
    #     outfile = base + ext
    #     os.makedirs(os.path.dirname(outfile), exist_ok=True)

    #     # --- summary statistics ---
    #     summary_df = None
    #     if include_summary:
    #         summary_rows = []
    #         for col in df_out.columns:
    #             meta = osb.meta_filled[col] if col in osb.meta_filled else None
    #             counts = meta.value_counts().to_dict() if meta is not None else {}
    #             total = len(df_out)
    #             filled = sum(v for k, v in counts.items() if str(k).startswith("filled"))
    #             filtered = counts.get("filtered", 0)
    #             orig = counts.get("original", 0)
    #             vals = df_out[col].dropna()
    #             summary_rows.append({
    #                 "Column": col,
    #                 "Count": total,
    #                 "Original": orig,
    #                 "Filtered": filtered,
    #                 "Filled": filled,
    #                 "Filled_%": 100 * filled / total if total else np.nan,
    #                 "Mean": vals.mean(),
    #                 "Std": vals.std(),
    #                 "Min": vals.min(),
    #                 "Max": vals.max(),
    #             })
    #         summary_df = pd.DataFrame(summary_rows)

    #     # --- pipeline info table ---
    #     pipeline_df = None
    #     if include_pipeline_meta and hasattr(self, "pipeline") and self.pipeline:
    #         pipeline_df = pd.DataFrame([
    #             {
    #                 "Step": i + 1,
    #                 "Kind": step.kind,
    #                 "Method": step.method,
    #                 "Column": step.column,
    #                 "Parameters": step.params,
    #             }
    #             for i, step in enumerate(self.pipeline)
    #         ])

    #     # --- EXPORT ---
    #     if fmt == "xlsx":
    #         with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    #             df_out.to_excel(writer, sheet_name="filled_data")
    #             if include_meta:
    #                 osb.meta_valid.to_excel(writer, sheet_name="meta_valid")
    #                 osb.meta_filled.to_excel(writer, sheet_name="meta_filled")
    #             if include_original:
    #                 osb.data.to_excel(writer, sheet_name="original_data")
    #             if include_summary and summary_df is not None:
    #                 summary_df.to_excel(writer, sheet_name="summary", index=False)
    #             if include_pipeline_meta and pipeline_df is not None:
    #                 pipeline_df.to_excel(writer, sheet_name="pipeline", index=False)

    #     elif fmt == "csv":
    #         # multi-file export (and optionally zip)
    #         tmpdir = base + "_export"
    #         os.makedirs(tmpdir, exist_ok=True)
    #         df_out.to_csv(os.path.join(tmpdir, "filled_data.csv"))
    #         if include_meta:
    #             osb.meta_valid.to_csv(os.path.join(tmpdir, "meta_valid.csv"))
    #             osb.meta_filled.to_csv(os.path.join(tmpdir, "meta_filled.csv"))
    #         if include_original:
    #             osb.data.to_csv(os.path.join(tmpdir, "original_data.csv"))
    #         if include_summary and summary_df is not None:
    #             summary_df.to_csv(os.path.join(tmpdir, "summary.csv"), index=False)
    #         if include_pipeline_meta and pipeline_df is not None:
    #             pipeline_df.to_csv(os.path.join(tmpdir, "pipeline.csv"), index=False)
    #         if compress:
    #             import shutil
    #             shutil.make_archive(base, "zip", tmpdir)
    #             shutil.rmtree(tmpdir)
    #             outfile = base + ".zip"

    #     elif fmt == "parquet":
    #         df_out.to_parquet(outfile, compression="gzip" if compress else None)

    #     elif fmt == "json":
    #         export_dict = {"data": df_out.to_dict(orient="index")}
    #         if include_meta:
    #             export_dict["meta_valid"] = osb.meta_valid.to_dict(orient="index")
    #             export_dict["meta_filled"] = osb.meta_filled.to_dict(orient="index")
    #         if include_summary and summary_df is not None:
    #             export_dict["summary"] = summary_df.to_dict(orient="records")
    #         if include_pipeline_meta and pipeline_df is not None:
    #             export_dict["pipeline"] = pipeline_df.to_dict(orient="records")
    #         import json
    #         with open(outfile, "w", encoding="utf-8") as f:
    #             json.dump(export_dict, f, indent=2, ensure_ascii=False)

    #     elif fmt == "pickle":
    #         package = {
    #             "data": df_out,
    #             "meta_valid": osb.meta_valid if include_meta else None,
    #             "meta_filled": osb.meta_filled if include_meta else None,
    #             "summary": summary_df,
    #             "pipeline": pipeline_df,
    #         }
    #         pd.to_pickle(package, outfile)

    #     else:
    #         raise ValueError(f"Unsupported format: {format}")

    #     print(f"✅ Export complete: {outfile}")
    #     return outfile


    # def export_data(
    #     self,
    #     path: str,
    #     *,
    #     format: str = "csv",
    #     include_meta: bool = True,
    #     include_original: bool = False,
    #     timestamp: bool = False,
    #     compress: bool = False,
    # ) -> str:
    #     """
    #     Export the processed data to a file in a chosen format.

    #     Parameters
    #     ----------
    #     path : str
    #         File path or directory where the exported file should be saved.
    #         If a directory is given, a file name will be autogenerated.
    #     format : {"csv", "xlsx", "parquet", "json", "pickle"}, default "csv"
    #         Format of the output file.
    #     include_meta : bool, default True
    #         If True, export meta_valid and meta_filled alongside the data (multi-sheet or zipped).
    #     include_original : bool, default False
    #         If True, include the raw/original dataset before filtering.
    #     timestamp : bool, default False
    #         If True, append a timestamp to the file name for versioning.
    #     compress : bool, default False
    #         If True, compress the output file (ZIP for csv/xlsx/json, gzip for parquet).

    #     Returns
    #     -------
    #     str
    #         The full path to the exported file.
    #     """
       

    #     # Determine base data to export
    #     osb = self.osb
    #     df_out = getattr(osb, "filled", None)
    #     if df_out is None or df_out.empty:
    #         df_out = osb.data.copy()
    #         print("No filled data available, exporting original dataset instead.")

    #     # Compose filename
    #     if os.path.isdir(path):
    #         base = os.path.join(path, "processed_data")
    #     else:
    #         base, _ = os.path.splitext(path)
    #     if timestamp:
    #         base += "_" + datetime.now().strftime("%Y%m%d_%H%M%S")

    #     fmt = format.lower()
    #     ext_map = {
    #         "csv": ".csv",
    #         "xlsx": ".xlsx",
    #         "parquet": ".parquet",
    #         "json": ".json",
    #         "pickle": ".pkl",
    #     }
    #     ext = ext_map.get(fmt, f".{fmt}")
    #     outfile = base + ext
    #     os.makedirs(os.path.dirname(outfile), exist_ok=True)

    #     # Optional compression
    #     if compress:
    #         if fmt in ("csv", "json", "xlsx"):
    #             outfile += ".zip"
    #         elif fmt == "parquet":
    #             outfile += ".gz"

    #     # --- Export logic ---
    #     if fmt == "csv":
    #         if include_meta:
    #             # Export all in one zip folder
    #             tmpdir = base + "_export"
    #             os.makedirs(tmpdir, exist_ok=True)
    #             df_out.to_csv(os.path.join(tmpdir, "data.csv"))
    #             if include_meta:
    #                 osb.meta_valid.to_csv(os.path.join(tmpdir, "meta_valid.csv"))
    #                 osb.meta_filled.to_csv(os.path.join(tmpdir, "meta_filled.csv"))
    #             if include_original:
    #                 osb.data.to_csv(os.path.join(tmpdir, "original.csv"))
    #             import shutil
    #             shutil.make_archive(base, "zip", tmpdir)
    #             shutil.rmtree(tmpdir)
    #             outfile = base + ".zip"
    #         else:
    #             df_out.to_csv(outfile, index=True)

    #     elif fmt == "xlsx":
    #         with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    #             df_out.to_excel(writer, sheet_name="filled_data")
    #             if include_meta:
    #                 osb.meta_valid.to_excel(writer, sheet_name="meta_valid")
    #                 osb.meta_filled.to_excel(writer, sheet_name="meta_filled")
    #             if include_original:
    #                 osb.data.to_excel(writer, sheet_name="original_data")

    #     elif fmt == "parquet":
    #         df_out.to_parquet(outfile, compression="gzip" if compress else None)

    #     elif fmt == "json":
    #         df_out.to_json(outfile, orient="table", date_format="iso", indent=2)

    #     elif fmt == "pickle":
    #         df_out.to_pickle(outfile)

    #     else:
    #         raise ValueError(f"Unsupported format: {format}")

    #     print(f"Data exported successfully to: {outfile}")
    #     return outfile


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

        # --- Setup filename ---
        if os.path.isdir(path):
            base = os.path.join(path, "processed_data")
        else:
            base, _ = os.path.splitext(path)
        if timestamp:
            base += "_" + datetime.now().strftime("%Y%m%d_%H%M%S")

        outfile_zip = base + ".zip"
        os.makedirs(os.path.dirname(outfile_zip), exist_ok=True)

        # --- Helper functions for in-memory export ---
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

        # --- Collect all files to write ---
        buffer_dict = {}

        # Main data
        buffer_dict[f"filled_data.{data_format}"] = df_to_bytes(df_out, data_format)

        # Metadata
        if include_meta:
            buffer_dict[f"meta_valid.{meta_format}"] = df_to_bytes(osb.meta_valid, meta_format)
            buffer_dict[f"meta_filled.{meta_format}"] = df_to_bytes(osb.meta_filled, meta_format)

        if include_original:
            buffer_dict[f"original_data.{data_format}"] = df_to_bytes(osb.data, data_format)

        # --- Summary statistics ---
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

        # --- Pipeline info ---
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

        # --- Write everything to ZIP ---
        if compress:
            with zipfile.ZipFile(outfile_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for name, content in buffer_dict.items():
                    zf.writestr(name, content)
            print(f"Export complete: {outfile_zip}")
            return outfile_zip
        else:
            # export only main data (uncompressed)
            main_file = base + f".{data_format}"
            with open(main_file, "wb") as f:
                f.write(buffer_dict[f"filled_data.{data_format}"])
            print(f"Exported main data only: {main_file}")
            return main_file

############ VER 0

# @dataclass(slots=True)
# class DataProcessorConfig:
#     time_col: str = "Datetime"     # will be used if there is no DatetimeIndex
#     copy: bool = True              # copy input df on init
#     # default interpolation args (only used if clean(strategy="interpolate"))
#     interpolate_method: str = "time"
#     interpolate_limit: Optional[int] = None
#     interpolate_direction: Optional[str] = None  # "forward", "backward", "both"
# StepType = Literal["filter", "fill", "custom"]
# Arange = Optional[Tuple[Any, Any]]

# @dataclass
# class PipelineStep:
#     kind: StepType
#     name: str
#     target: str                               # column to operate on (for multivariate, use comma-separated or list in params)
#     params: Dict[str, Any] = field(default_factory=dict)
#     enabled: bool = True
#     description: Optional[str] = None

# class DataProcessor():
#     """
#     Orchestrates automated pipelines for data filtering & gap filling
#     using an OnlineSensorBased instance (composition, not inheritance).

#     Key ideas:
#     - Build steps declaratively, then `run()` them in order.
#     - Each step wraps an OnlineSensorBased method (or a custom callable).
#     - All parameters are captured for full reproducibility.
#     """

#     # Friendly aliases -> OnlineSensorBased method names
#     FILTER_METHODS: Dict[str, str] = {
#         # univariate filters
#         "tag_nan": "tag_nan",
#         "tag_doubles": "tag_doubles",
#         "tag_extremes": "tag_extremes",
#         "moving_slope": "moving_slope_filter",
#         "moving_average": "moving_average_filter",
#         "zscore": "zscore_filter",
#         "stl_residual": "stl_filter",
#         "iqr": "iqr_filter",
#         "rolling_iqr": "rolling_iqr_filter",
#         "hampel": "hampel_filter",
#         # multivariate
#         "isolation_forest": "isolation_forest_filter",
#         "pca": "pca_outlier_filter",
#     }

#     FILL_METHODS: Dict[str, str] = {
#         "interpolation": "fill_missing_interpolation",
#         "ratio": "fill_missing_ratio",
#         "standard": "fill_missing_standard",
#         "model": "fill_missing_model",
#         "day_before": "fill_missing_daybefore",
#         "kalman": "fill_missing_kalman",
#         "arima": "fill_missing_arima",
#         "gaussian": "fill_missing_gaussian",
#     }

#     # df: pd.DataFrame
#     # cols: List[str]
#     # cfg: DataProcessorConfig
#     # masks: Dict[str, pd.DataFrame]        # method -> bool DataFrame (index aligned with df)
#     # scores: Dict[str, pd.DataFrame] 

#     def __init__(
#         self,
#         data: Union[pd.DataFrame, "OnlineSensorBased"],
#         *,
#         timedata_column: str = "index",
#         data_type: str = "WWTP",
#         experiment_tag: str = "pipeline",
#         time_unit: Optional[str] = None,
#         columns: Optional[Iterable[str]] = None,
#         copy_input: bool = True,
#     ) -> None:
#         """
#         Parameters
#         ----------
#         data : DataFrame or OnlineSensorBased
#             If DataFrame, we will construct an OnlineSensorBased around it.
#         columns : iterable[str], optional
#             Subset of columns to consider as default targets. Steps can still
#             override with their own `target`.
#         copy_input : bool
#             If True and data is a DataFrame, copy it to avoid mutation.
#         """
#         # Bring/Wrap OnlineSensorBased
#         if hasattr(data, "data") and hasattr(data, "meta_valid"):
#             # assume it's already an OnlineSensorBased
#             self.osb = data  # type: ignore[assignment]
#         else:
#             df = data.copy() if (isinstance(data, pd.DataFrame) and copy_input) else data
#             # You already defined OnlineSensorBased elsewhere; we just construct it:
#             self.osb = OnlineSensorBased(
#                 data=df, timedata_column=timedata_column, data_type=data_type,
#                 experiment_tag=experiment_tag, time_unit=time_unit
#             )

#         # Default column set
#         self.columns: List[str] = list(columns) if columns is not None else list(self.osb.data.columns)

#         # Pipeline store
#         self.steps: List[PipelineStep] = []

#         # Checkpoints (for undo/inspect)
#         self._snapshots: List[Dict[str, Any]] = []

#         # Registry for custom callables (returns None or modifies self.osb in place)
#         self._custom_registry: Dict[str, Callable[[OnlineSensorBased, PipelineStep], Any]] = {}

#     # ---------------------------
#     # Pipeline construction
#     # ---------------------------
#     def add_filter(
#         self,
#         name: str,
#         *,
#         target: Optional[str] = None,
#         description: Optional[str] = None,
#         enabled: bool = True,
#         **params: Any,
#     ) -> "DataProcessor":
#         """Add a filtering step by friendly `name` (see FILTER_METHODS)."""
#         if name not in self.FILTER_METHODS:
#             raise KeyError(f"Unknown filter '{name}'. Known: {list(self.FILTER_METHODS)}")
#         step = PipelineStep(kind="filter", name=name, target=target or self._default_target(), params=params, enabled=enabled, description=description)
#         self.steps.append(step)
#         return self

#     def add_fill(
#         self,
#         name: str,
#         *,
#         target: Optional[str] = None,
#         description: Optional[str] = None,
#         enabled: bool = True,
#         **params: Any,
#     ) -> "DataProcessor":
#         """Add a filling step by friendly `name` (see FILL_METHODS)."""
#         if name not in self.FILL_METHODS:
#             raise KeyError(f"Unknown fill '{name}'. Known: {list(self.FILL_METHODS)}")
#         step = PipelineStep(kind="fill", name=name, target=target or self._default_target(), params=params, enabled=enabled, description=description)
#         self.steps.append(step)
#         return self

#     def add_custom(
#         self,
#         name: str,
#         func: Callable[[OnlineSensorBased, PipelineStep], Any],
#         *,
#         target: Optional[str] = None,
#         description: Optional[str] = None,
#         enabled: bool = True,
#         **params: Any,
#     ) -> "DataProcessor":
#         """
#         Add a custom callable step. The callable receives (osb, step) and
#         can mutate osb or return a result. Register is kept for export/import.
#         """
#         self._custom_registry[name] = func
#         step = PipelineStep(kind="custom", name=name, target=target or self._default_target(), params=params, enabled=enabled, description=description)
#         self.steps.append(step)
#         return self

#     # ---------------------------
#     # Execution
#     # ---------------------------
#     def snapshot(self, label: Optional[str] = None) -> None:
#         """Save a deep snapshot of the OSB state (data + meta frames)."""
#         snap = {
#             "label": label or f"step{len(self._snapshots)}",
#             "data": self.osb.data.copy(deep=True),
#             "meta_valid": getattr(self.osb, "meta_valid", pd.DataFrame()).copy(deep=True),
#             "meta_filled": getattr(self.osb, "meta_filled", pd.DataFrame()).copy(deep=True),
#             "filled": getattr(self.osb, "filled", pd.DataFrame()).copy(deep=True),
#         }
#         self._snapshots.append(snap)

#     def undo(self) -> None:
#         """Revert to the last snapshot."""
#         if not self._snapshots:
#             return
#         snap = self._snapshots.pop()
#         self.osb.data = snap["data"]
#         self.osb.meta_valid = snap["meta_valid"]
#         self.osb.meta_filled = snap["meta_filled"]
#         self.osb.filled = snap["filled"]
#         # Ensure time cache stays in sync
#         if hasattr(self.osb, "_update_time"):
#             self.osb._update_time()

#     def run(self, *, take_snapshots: bool = True, stop_on_error: bool = True) -> List[Dict[str, Any]]:
#         """
#         Execute all enabled steps in order.

#         Returns a per-step summary list.
#         """
#         summaries: List[Dict[str, Any]] = []
#         for i, step in enumerate(self.steps, start=1):
#             if not step.enabled:
#                 summaries.append({"step": i, "name": step.name, "skipped": True})
#                 continue

#             if take_snapshots:
#                 self.snapshot(label=f"pre:{i}:{step.name}")

#             try:
#                 res = self._execute_step(step)
#             except Exception as e:
#                 summaries.append({"step": i, "name": step.name, "target": step.target, "error": repr(e)})
#                 if stop_on_error:
#                     break
#                 else:
#                     continue

#             post = self._post_step_summary(step)
#             summaries.append({"step": i, "name": step.name, "target": step.target, "result": res, **post})

#         return summaries

#     def run_on_window(self, arange: Tuple[Any, Any], *, take_snapshots: bool = True, stop_on_error: bool = True) -> List[Dict[str, Any]]:
#         """
#         Run all steps but inject/override `arange` for any step that accepts it.
#         """
#         injected = []
#         for s in self.steps:
#             s2 = copy.deepcopy(s)
#             # inject only if not explicitly provided
#             if "arange" not in s2.params:
#                 s2.params["arange"] = arange
#             injected.append(s2)

#         original = self.steps
#         self.steps = injected
#         try:
#             return self.run(take_snapshots=take_snapshots, stop_on_error=stop_on_error)
#         finally:
#             self.steps = original

#     # ---------------------------
#     # Serialization
#     # ---------------------------
#     def export_config(self) -> str:
#         """
#         Export the pipeline to a JSON string (does not serialize custom callables,
#         only their registered names and parameters).
#         """
#         blob = {
#             "columns": self.columns,
#             "steps": [
#                 {
#                     "kind": s.kind,
#                     "name": s.name,
#                     "target": s.target,
#                     "params": s.params,
#                     "enabled": s.enabled,
#                     "description": s.description,
#                 }
#                 for s in self.steps
#             ],
#         }
#         return json.dumps(blob, indent=2, default=str)

#     @classmethod
#     def from_config(
#         cls,
#         config_json: str,
#         osb: OnlineSensorBased,
#         *,
#         custom_registry: Optional[Dict[str, Callable[[OnlineSensorBased, PipelineStep], Any]]] = None,
#     ) -> "DataProcessor":
#         """
#         Recreate a DataProcessor from JSON (binds to an existing OnlineSensorBased).
#         """
#         blob = json.loads(config_json)
#         dp = cls(osb, columns=blob.get("columns"))
#         if custom_registry:
#             dp._custom_registry.update(custom_registry)

#         for s in blob["steps"]:
#             step = PipelineStep(
#                 kind=s["kind"],
#                 name=s["name"],
#                 target=s["target"],
#                 params=s.get("params", {}),
#                 enabled=s.get("enabled", True),
#                 description=s.get("description"),
#             )
#             dp.steps.append(step)
#         return dp

#     # ---------------------------
#     # Utilities / Internals
#     # ---------------------------
#     def _default_target(self) -> str:
#         if not self.columns:
#             raise ValueError("No default columns available; please pass `columns` or specify `target` per step.")
#         return self.columns[0]

#     def _execute_step(self, step: PipelineStep) -> Any:
#         """
#         Dispatch to OnlineSensorBased (filters/fills) or custom callable.
#         """
#         # Normalize multivariate target
#         target = step.target
#         params = dict(step.params)

#         if step.kind == "custom":
#             func = self._custom_registry.get(step.name)
#             if func is None:
#                 raise KeyError(f"Custom step '{step.name}' is not registered.")
#             return func(self.osb, step)

#         if step.kind == "filter":
#             method_name = self.FILTER_METHODS[step.name]
#         elif step.kind == "fill":
#             method_name = self.FILL_METHODS[step.name]
#         else:
#             raise ValueError(f"Unknown step kind: {step.kind}")

#         method = getattr(self.osb, method_name, None)
#         if method is None:
#             raise AttributeError(f"OnlineSensorBased has no method '{method_name}'")

#         # Many of your methods expect `data_name` or `to_fill`. Map automatically:
#         # - filters commonly: data_name=...
#         # - fills commonly: to_fill=...
#         if step.kind == "filter":
#             if "data_name" not in params:
#                 params["data_name"] = target
#         else:  # fill
#             if "to_fill" not in params:
#                 params["to_fill"] = target

#         # If the OSB method requires mandatory args (like `arange`) and it's not in params,
#         # it will raise — that’s OK; user can add it on the step.
#         return method(**params)

#     def _post_step_summary(self, step: PipelineStep) -> Dict[str, Any]:
#         """
#         Lightweight, non-invasive summary after a step.
#         """
#         col = step.target
#         df = self.osb.data
#         mv = getattr(self.osb, "meta_valid", pd.DataFrame(index=df.index))
#         mf = getattr(self.osb, "meta_filled", pd.DataFrame(index=df.index))
#         filled = getattr(self.osb, "filled", pd.DataFrame(index=df.index))

#         out: Dict[str, Any] = {}

#         if col in df:
#             out["non_null_data"] = int(df[col].notna().sum())
#         if col in mv:
#             out["filtered_count"] = int((mv[col] == "filtered").sum())
#         if col in mf:
#             out["filled_interpol"] = int((mf[col] == "filled_interpol").sum())
#             out["filled_ratio"] = int((mf[col] == "filled_ratio").sum())
#             out["filled_average_profile"] = int((mf[col] == "filled_average_profile").sum())
#             out["filled_infl_model"] = int((mf[col] == "filled_infl_model").sum())
#             out["filled_profile_day_before"] = int((mf[col] == "filled_profile_day_before").sum())
#             out["filled_gaussian"] = int((mf[col] == "filled_gaussian").sum())
#             out["filled_kalman"] = int((mf[col] == "filled_kalman").sum())
#             out["filled_arima"] = int((mf[col] == "filled_arima").sum())

#         if col in filled:
#             out["filled_non_null"] = int(filled[col].notna().sum())

#         return out

#     # ---------------------------
#     # Convenience builders
#     # ---------------------------
#     def quick_clean_and_fill(
#         self,
#         column: str,
#         *,
#         # filtering
#         zscore_cutoff: float = 3.5,
#         iqr_factor: float = 1.5,
#         # filling
#         interp_limit: int = 3,
#         interp_method: str = "time",
#         # windows
#         arange: Arange = None,
#     ) -> "DataProcessor":
#         """
#         Opinionated small pipeline:
#         1) tag NaNs
#         2) z-score filter
#         3) iqr filter
#         4) interpolate small gaps
#         """
#         self.add_filter("tag_nan", target=column)
#         self.add_filter("zscore", target=column, cutoff=zscore_cutoff, arange=arange)
#         self.add_filter("iqr", target=column, factor=iqr_factor, arange=arange)
#         self.add_fill("interpolation", target=column, range_=interp_limit, method=interp_method, arange=arange)
#         return self
    

    ########### OLD VERSION

    # def __init__(self,
    #              data: pd.DataFrame,
    #              columns: Sequence[str],
    #              cfg: Optional[DataProcessorConfig] = None,
    #     ) -> None:
    #     """
    #     initialisation of a Processor object with
    #     the data to be used for simulation

    #     """
    #     if data.empty:
    #         raise ValueError("DataProcessor requires a non-empty DataFrame.")
    #     self.cfg = cfg or DataProcessorConfig()
    #     self.df = data.copy() if self.cfg.copy else data
    #     self.df = self._ensure_dtindex(self.df, time_col=self.cfg.time_col)
    #     self.cols = [c for c in columns if c in self.df.columns]
    #     missing = set(columns) - set(self.cols)
    #     if missing:
    #         logger.warning("Ignoring missing columns: %s", ", ".join(sorted(missing)))
    #     if not self.cols:
    #         raise ValueError("No valid columns to process after filtering against DataFrame.")
    #     # normalize numeric dtype (safe: coerce to floats)
    #     self.df[self.cols] = self.df[self.cols].apply(pd.to_numeric, errors="coerce").astype(float)
    #     self.masks = {}
    #     self.scores = {}
        
    # @staticmethod
    # def _ensure_dtindex(df: pd.DataFrame, *, time_col: str) -> pd.DataFrame:
    #     if isinstance(df.index, pd.DatetimeIndex):
    #         out = df.sort_index()
    #         if out.index.name is None:
    #             out.index = out.index.set_names(time_col)
    #         return out
    #     if time_col in df.columns:
    #         out = df.copy()
    #         out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    #         out = out.dropna(subset=[time_col]).set_index(time_col).sort_index()
    #         out.index = pd.DatetimeIndex(out.index, name=time_col)
    #         if time_col in out.columns:
    #             out = out.drop(columns=[time_col])
    #         return out
    #     raise ValueError(f"Data must have a DatetimeIndex or a '{time_col}' column.")
    
    # def _register_mask(self, name: str, mask: pd.DataFrame, score: Optional[pd.DataFrame] = None) -> None:
    #     # align to df index/columns, fill missing with False/NaN
    #     mask = mask.reindex(index=self.df.index, columns=self.cols, fill_value=False)
    #     self.masks[name] = mask.astype(bool)
    #     if score is not None:
    #         self.scores[name] = score.reindex(index=self.df.index, columns=self.cols)

        
        
    # @property
    # def data(self):
    #     """get the dataset loaded into the data processor"""
    #     return self._data
        
        
    #     # self.database_name = database_name
    #     # self.path_to_db = path_to_db
    #     # self._conn = create_database_connection(database_type='sqlite', path_to_db=self.path_to_db, 
    #     #                                         database_name=self.database_name)
    # def load_data(self, data, variable_names=None, subrange=None):
        
    #     if variable_names is not None:
    #         _unprocessed_data = data[variable_names]
    #     else:
    #         _unprocessed_data = data
        
        
        
    #     self.dataset = ww.OnlineSensorBased(_unprocessed_data, 'index')
    #     self.dataset.set_tag('unprocessed_data')

    #     if subrange is None:
    #         self.subset_range = [self.dataset.index()[0].to_pydatetime(), self.dataset.index()[-1].to_pydatetime()]
    #     else:
    #         self.subset_range = [subrange[0].to_pydatetime(), subrange[1].to_pydatetime()]

    #     self._data = self.dataset.data

    

    # def tag_rain_events(self, flow_column_name='Q', rain_filter_method='percentile', rain_event_threshold=0.95, plot=False):



    #     if flow_column_name not in self.dataset.data.columns:
    #         raise NameError('The name of the flow column is not in the dataset columns!')
        

    #     self.rain_filter_method = rain_filter_method
    #     self.rain_event_value = rain_event_threshold
    #     self.flow_column_name = flow_column_name

    #     self.dataset.get_highs(self.flow_column_name,self.rain_event_value,arange=[self.subset_range[0], self.subset_range[1]], 
    #                            method=self.rain_filter_method,plot=plot)
        
    #     print(f'Rain events are filtered based on the data in the column "{self.flow_column_name}" with method="{self.rain_filter_method}" and the threshold="{self.rain_event_value}".')

    #     # return self.dataset.highs


    

    # def set_typical_dry_profile(self, variable, initial_day, duration, plot=False, plot_method='quantile'):
        
    #     self.dry_profile_duration = duration
    #     self.dry_profile_initial_day = dt.datetime(initial_day[0], initial_day[1], initial_day[2])
        
        
    #     try:
    #         self.dataset.highs
    #     except:
    #         self.tag_rain_events()
        
    #     _range = [self.dry_profile_initial_day, self.dry_profile_initial_day + dt.timedelta(days=self.dry_profile_duration)]
    #     print(type(_range[0]))
        
    #     self._calc_daily_profile(dataset_obj=self.dataset, column_name=variable,
    #                             arange=_range,
    #                             clear=True,quantile=0.95, plot=plot, plot_method=plot_method)

                
    #     # # TODO: customize the query for subset of data
    #     # self._formatted_data = query_from_db("SELECT * from raw_data_formatted", self._conn, None, None)
    #     # self._formatted_data['Datetime'] = pd.to_datetime(self._formatted_data['Datetime'],format='%Y-%m-%d %H:%M')
    #     # self._formatted_data.drop_duplicates('Datetime', inplace=True)
    #     # self._formatted_data.set_index('Datetime', inplace=True, drop=True)
        
    #     # self._formatted_dataset = ww.OnlineSensorBased(self._formatted_data, 'index')
    #     # self._formatted_dataset.set_tag('raw_data')

    #     # try:
    #     #     self._sim_data = query_from_db("SELECT * from raw_data_simulation", self._conn, None, None)
    #     #     self._sim_data['Datetime'] = pd.to_datetime(self._sim_data['Datetime'],format='%Y-%m-%d %H:%M')
    #     #     self._sim_data.drop_duplicates('Datetime', inplace=True)
    #     #     self._sim_data.set_index('Datetime', inplace=True, drop=True)
            
    #     #     self._sim_dataset = ww.OnlineSensorBased(self._sim_data, 'index')
    #     #     self._sim_dataset.set_tag('raw_simulation_data')
    #     # except:
    #     #     print('The period of simulation dataset is the same as the original dataset!')
    #     #     self._sim_dataset = self._formatted_dataset
    #     #     self._sim_dataset.set_tag('raw_simulation_data')
        
        

    #     # self._subset_range = [self._sim_dataset.index()[0].to_pydatetime(), self._sim_dataset.index()[-1].to_pydatetime()]

  
    # def set_influent_model_data(self):
    #     pass
        


    # def filter_data(self, constant_signal_threshold=0.005, max_slope=None, extreme_limit=None, extreme_method=None,
    #                 moving_ave_window=None, moving_ave_cutoff=None):

    #     self.constant_signal_threshold = constant_signal_threshold
    #     self.max_slope = max_slope
    #     self.moving_ave_window = moving_ave_window 
    #     self.moving_ave_cuttoff = moving_ave_cutoff
    #     self.extreme_limit = extreme_limit
    #     self.extreme_method = extreme_method

    #     for col in self.dataset.data.columns:
    #         self.dataset.tag_nan(col, arange=self.subset_range, clear=True)
    #         self.dataset.tag_doubles(col, bound=self.dataset.data[col].mean()*self.constant_signal_threshold, arange=self.subset_range)
    #         self.dataset.tag_extremes(col, arange=self.subset_range, limit=0, method='below')
    #         # moving average filter
    #         self.dataset.moving_average_filter(col, window=4, cutoff_frac=0.2, arange=self.subset_range, plot=False)
    #         # moving slope filter

    #     if extreme_method is not None:
    #         for col in self.dataset.data.columns:
    #             self.dataset.tag_extremes(col, arange=self.subset_range, limit=self.extreme_limit, method=self.extreme_method)
        
    #     return self.dataset.meta_valid
    

    # def fill_data(self):
    #     pass


    # def _set_data_columns(self, variable_names):
    #     """
    #     set names of the columns containing the
    #     data to be processed

    #     Parameters
    #     ----------
    #     variable_names: dict
    #         name of the data columns and their corresponding variables, possible variable options:
    #         flow/Q, COD/total COD/tCOD/CODt, soluble COD/sCOD/CODs, TSS/solids, ammonia/NH4, phosphate/PO4.
        
        
    #     Returns
    #     ------
    #     None

    #     """

       
    #     for var in list(variable_names.keys()):
    #         if var == 'flow' or var == 'Q':
    #             self._flow_column =  variable_names[var]
    #         elif var == 'COD' or var == 'total COD' or var == 'tCOD' or var =='CODt':
    #             self._cod_column = variable_names[var]
    #         elif var == 'soluble COD' or var =='CODs' or var == 'sCOD':
    #             self._cods_column = variable_names[var]
    #         elif var == 'TSS' or var == 'solids':
    #             self._tss_column =  variable_names[var]
    #         elif var == 'ammonia' or var =='NH4':
    #             self._amm_column =  variable_names[var]
    #         elif var == 'phosphate' or var == 'PO4':
    #             self._phos_column =  variable_names[var]
        

    # def _check_index(self):
    #     # check index of the raw data
    #     # need to start at 00:00
    #     # need to be rounded to the closest 05:00
    #     pass





    # def _calc_daily_profile(self,dataset_obj, column_name,arange,quantile=0.9,
    #                     clear=False,only_checked=False,plot=False,plot_method='quantile'):
    #     """
    #     -------------------
    #     Modified from wwdata package, Copyright (C) 2016 Chaim De Mulder
    #     -------------------

    #     Calculates a typical daily profile based on data from the indicated
    #     consecutive days. Also saves this average day, along with standard
    #     deviation and lower and upper percentiles as given in the arguments.
    #     Plotting is possible.

    

    #     Parameters
    #     ----------
    #     dataset_obj: wwdata OnlineSensorBased object
    #         an instance of the wwdata OnlineSensorBased object
    #     column_name : str
    #         name of the column containing the data to calculate an average day
    #         for
    #     arange : 2-element array of ints
    #         contains the beginning and end day of the period to use for average
    #         day calculation
    #     quantile : float between 0 and 1
    #         value to use for the calculation of the quantiles
    #     clear : bool
    #         wether or not to clear the key in the dataset_obj.daily_profile dictionary
    #         that is already present

    #     Returns
    #     -------
    #     None
    #         creates a dictionary dataset_obj.daily_profile containing information
    #         on the average day as calculated.
    #     """
    #     # several checks to make sure the right types, columns... are used
    #     try:
    #         if not isinstance(dataset_obj.daily_profile,dict):
    #             dataset_obj.daily_profile = {}
    #     except AttributeError:
    #         dataset_obj.daily_profile = {}

    #     if clear:
    #         try:
    #             dataset_obj.daily_profile.pop(column_name, None)
    #         except KeyError:
    #             pass

    #     if column_name in dataset_obj.daily_profile.keys():
    #         raise KeyError('dataset_obj.daily_profile dictionary already contains a ' +\
    #         'key ' + column_name + '. Set argument "clear" to True to erase the ' + \
    #         'key and create a new one.')

    #     # Give warning when replacing data from rain events and at the same time
    #     # check if arange has the right type
    #     try:
    #         rain = (dataset_obj.data_type == 'WWTP') and \
    #                (dataset_obj.highs['highs'].loc[arange[0]:arange[1]].sum() > 1)
    #     except TypeError:
    #         raise TypeError("Slicing not possible for index type " + \
    #         str(type(dataset_obj.data.index[0])) + " and arange argument type " + \
    #         str(type(arange[0])) + ". Try changing the type of the arange " \
    #         "values to one compatible with " + str(type(dataset_obj.data.index[0])) + \
    #         " slicing.")
    #     except AttributeError:
    #         raise AttributeError('OnlineSensorBased instance has no attribute "highs". '\
    #         'run .get_highs to tag the peaks in the dataset.')

    #     if rain :
    #         wn.warn("Data points obtained during a rain event will be used for" \
    #         " the calculation of an average day. This might lead to a not-" \
    #         "representative average day and/or high standard deviations.")

    #     daily_profile = pd.DataFrame()

    #     if not isinstance(arange[0],int) and not isinstance(arange[0],dt.datetime):
    #         raise TypeError('The values of arange must be of type int or dt.datetime')

    #     if isinstance(dataset_obj.data.index[0],dt.datetime):
    #         range_days = pd.date_range(arange[0],arange[1])
    #         indexes = [dataset_obj.data.index[dataset_obj.data.index == range_days[0]].date[0], \
    #     dataset_obj.data.index[dataset_obj.data.index == range_days[0]+dt.timedelta(1)].date[0]]
    #         # indexes = [dataset_obj.data.index[0],dataset_obj.data.index[0]+dt.timedelta(1)]
    #     else :
    #         range_days = range(arange[0],arange[1])
    #         indexes = [0,1]
    #     #if isinstance(arange[0],dt.datetime):
    #     #    range_days = pd.date_range(arange[0],arange[1])


    #     #if only_checked:
    #     #    for i in range_days:
    #     #        daily_profile = pd.merge(daily_profile,
    #     #                        pd.DataFrame(self.data[column_name][i:i+1]\
    #     #                        [self.meta_valid[column_name]=='original'].values),
    #     #                        left_index=True, right_index=True,how='outer')
    #     #    mean_day = pd.DataFrame(index=daily_profile.index)
    #     #    self.data.loc[indexes[0]:indexes[1]].index)#\
    #     #    [self.meta_valid[column_name]=='original'].index)
    #     #    if isinstance(self.data.index[0],dt.datetime):
    #     #        mean_day.index = mean_day.index.time
    #     #else:


    #     if only_checked and column_name in dataset_obj.meta_valid:
    #         for i in range_days:
    #             if isinstance(i,dt.datetime) or isinstance(i,np.datetime64) or isinstance(i,pd.Timestamp):
    #                 name = str(i.month) + '-' + str(i.day)
    #             else:
    #                 name = str(i)
    #             mask_valid = pd.DataFrame((dataset_obj.meta_valid[column_name][i:i+1] == 'original').values,columns=[name])
    #             daily_profile = pd.merge(daily_profile,
    #                                      pd.DataFrame(dataset_obj.data[column_name][i:i+1*range_days.freq][:-1].values,
    #                                                   columns=[name]).where(mask_valid),
    #                                      left_index=True, right_index=True,how='outer')
    #     else:
    #         if only_checked:
    #             wn.warn('No values of selected column were filtered yet. All values '+ \
    #             'will be displayed.')
    #         for i in range_days:
    #             if isinstance(i,dt.datetime) or isinstance(i,np.datetime64) or isinstance(i,pd.Timestamp):
    #                 name = str(i.month) + '-' + str(i.day)
    #             else:
    #                 name = str(i)
    #             daily_profile = pd.merge(daily_profile,
    #                                      pd.DataFrame(dataset_obj.data[column_name][i:i+1*range_days.freq][:-1].values,
    #                                                   columns=[name]),
    #                                      left_index=True, right_index=True,how='outer')

    #     daily_profile['index'] = (dataset_obj.data.loc[indexes[0]:indexes[1]].index.time)[:-1]
        
    #     daily_profile = daily_profile.drop_duplicates(subset='index', keep='first')\
    #                                  .set_index('index').sort_index()

    #     mean_day = pd.DataFrame(index=daily_profile.index.values)
    #     mean_day['avg'] = daily_profile.mean(axis=1).values
    #     mean_day['std'] = daily_profile.std(axis=1).values
    #     mean_day['Qupper'] = daily_profile.quantile(quantile,axis=1).values
    #     mean_day['Qlower'] = daily_profile.quantile(1-quantile,axis=1).values

    #     dataset_obj.daily_profile[column_name] = mean_day
    #     self._range_days = range_days
        
    #     if plot:
    #         fig = plt.figure(figsize=(12,4))
    #         ax = fig.add_subplot(111)
    #         x_axis = mean_day.index.map(lambda x: x.isoformat('minutes'))
    #         # x_axis = mean_day.index.astype(str)
    #         ax.plot(x_axis,mean_day['avg'],'g')
    #         if plot_method == 'quantile':
    #             ax.plot(x_axis,mean_day['Qupper'],'g',alpha=0.2)
    #             ax.plot(x_axis,mean_day['Qlower'],'g',alpha=0.2)
    #             ax.fill_between(x_axis,mean_day['avg'],mean_day['Qupper'],
    #                         color='green', alpha=0.2)
    #             ax.fill_between(x_axis,mean_day['avg'],mean_day['Qlower'],
    #                         color='green', alpha=0.2)
    #         elif plot_method == 'stdev':
    #             ax.plot(x_axis,mean_day['avg']+mean_day['std'],'g',alpha=0.2)
    #             ax.plot(x_axis,mean_day['avg']-mean_day['std'],'g',alpha=0.2)
    #             ax.fill_between(x_axis,mean_day['avg'],
    #                             mean_day['avg']+mean_day['std'],
    #                             color='green', alpha=0.2)
    #             ax.fill_between(x_axis,mean_day['avg'],
    #                             mean_day['avg']-mean_day['std'],
    #                             color='green', alpha=0.2)
    #         ax.tick_params(labelsize=14)
    #         ax.set_xticks(x_axis[::10])
    #         ax.set_xlim(x_axis[0],x_axis[-1])
    #         ax.set_ylabel(column_name,size=16)
    #         ax.set_xlabel('Time',size=16)
    #         return fig,ax
        

    # def get_formatted_data_head(self, n=5):
    #     """
    #     returns the first n rows of the dataframe containing the raw data for the whole period

    #     Parameters
    #     ----------
    #     n : int
    #         number of rows to be returned

    #     Returns
    #     ------
    #     a pandas dataframe

    #     """

    #     return self._formatted_dataset.head(n)


    # def get_sim_data_head(self, n=5):
    #     """
    #     returns first n rows of the dataframe containing the data to be used for simulation
        
    #     Parameters
    #     ----------
    #     n : int
    #         number of rows to be returned

    #     Returns
    #     ------
    #     a pandas dataframe

    #     """

    #     return self._sim_dataset.head(n)

    # def _set_typical_dry_profile(self, typical_dry_data):
    #     """
    #     calculate the average daily profile
    #     for a dry weather period

    #     Parameters
    #     ----------
    #     typical_dry_data: dict
    #         dictionary containing the data for calculating typical dry weather profile, possible keys: 'initial_day', 'duration'.
    #         initial_day: list
    #             start date of the dry weather period
    #         duration: int
    #             duration of the dry weather period in days

    #     Returns
    #     ------
    #     None

    #     """
       

    #     self._initial_day = dt.datetime(typical_dry_data['initial_day'][0], typical_dry_data['initial_day'][1], typical_dry_data['initial_day'][2])
    #     self._duration = typical_dry_data['duration']
        
    #     self._sim_dataset.get_highs(self._flow_column,0.95,arange=[self._subset_range[0], self._subset_range[1]], method='percentile',plot=False)
        
    #     self._calc_daily_profile(self._sim_dataset, self._flow_column,
    #                             [self._initial_day, self._initial_day + dt.timedelta(days=self._duration)],
    #                             clear=True,quantile=0.95)
        
    #     # self._daily_profile = self._sim_dataset.calc_daily_profile(self._flow_column,
    #     #                                                            [self._initial_day, self._initial_day + dt.timedelta(days=self._duration)],
    #     #                                                            clear=True,quantile=0.95)

    #     # self._sim_dataset.daily_profile = self._daily_profile

    #     if self._advanced_log:
    #         print('Data from the period between {} and {} have been selected as average typical dry weather profile!'.format(str(self._initial_day), str(self._initial_day + dt.timedelta(days=self._duration))))

    #     return None

    # def _set_influent_model(self, path_to_inf_mod):
    #     """
    #     set the influent generator model dataset based 
    #     on the provided model output file

    #     Parameters
    #     ----------
    #     path_to_inf_mod: str
    #         path to the file for influent model output
        
    #     Returns
    #     ------
    #     None
    #     """

    #     self._path_to_inf_mod = path_to_inf_mod
    #     self._inf_mod_dataset = pd.read_csv(self._path_to_inf_mod,sep='\t')
    #     self._units_inf_mod = self._inf_mod_dataset.iloc[0]
    #     self._inf_mod_dataset = self._inf_mod_dataset.drop(0,inplace=False).reset_index(drop=True)
    #     self._inf_mod_dataset = self._inf_mod_dataset.astype(float)
    #     self._inf_mod_dataset.set_index('#.t',drop=True,inplace=True)
        
    #     # self._inf_mod_dataset.reset_index(inplace=True)

    #     _var_names = ['Q_in', 'COD', 'CODs', 'TSS', 'NH4', 'PO4']
    #     self._model_col_names = {}
    #     for var in _var_names:
    #         self._model_col_names[var] = [s for s in self._inf_mod_dataset.columns if var in s][0]
        

        
    # def _rain_events(self, threshold=6, plot=False):
        
    #     self._sim_dataset.get_highs(self._flow_column,0.95,arange=[self._subset_range[0], self._subset_range[1]], method='percentile',plot=plot)
        
    #     _consequtive_repeats = self._sim_dataset.highs['highs'].diff().ne(0).cumsum()
    #     _selected_repeats =  _consequtive_repeats.groupby( _consequtive_repeats).transform('size') < threshold
    #     _first_values = ~_consequtive_repeats.duplicated()
    #     _repeated_events = self._sim_dataset.highs[_selected_repeats | _first_values]
    #     _rain_events = []
    #     for i in range(len(_repeated_events.index)-1):
    #         if _repeated_events['highs'][i] == 1 and (_repeated_events.index[i+1] - _repeated_events.index[i]) >= dt.timedelta(seconds=threshold*5*60):
    #             _rain_events.append((_repeated_events.index[i], _repeated_events.index[i+1]))
        
    #     return _rain_events



    # def process(self, variable_names, filter_rain=True, rain_filter_method='percentile', rain_event_value=0.95 
    #             , typical_dry_profile=False, typical_dry_data=None, 
    #             calibrated_inf_model=False, path_to_inf_model=None):
    #     """
    #     automatic filtering and filling of
    #     the simulation data

    #     Parameters
    #     ----------
    #     variable_names: dict
    #         name of the data columns and their corresponding variables, possible variable options: 
    #         flow/Q, COD/total COD/CODt/tCOD, soluble COD/sCOD/CODs, TSS/solids, ammonia/NH4, phosphate/PO4.
    #     filter_rain: bool
    #         indicates whether rain events should be filtered or not based on filtering method and value
    #     rain_filter_method : str
    #         method based on which rain events will be filtered, options: "value", "percentile"
    #     rain_event_value : float
    #         a value to be used for filtering rain events
    #     typical_dry_profile: bool
    #         indicates if a typical dry weather profile is available
    #     typical_dry_data: dict
    #         dictionary containing the initial day and duration for the typical dry weather period
    #     calibrated_inf_model: bool
    #         indicates if a calibrated influet model data is available
    #     path_to_inf_model: str
    #         path to the calibrated influent model output file

    #     """


    #     self._set_data_columns(variable_names)
    #     self._typical_dry_profile = typical_dry_profile
    #     self._calibrated_inf_model = calibrated_inf_model
        

    #     if self._calibrated_inf_model:
    #         self._set_influent_model(path_to_inf_model)
        
    #     if self._typical_dry_profile:
        
    #         self._set_typical_dry_profile(typical_dry_data)
    #         # except Exception as e:
    #         #     print(e)
    #         #     print('Flow data is not available!')



    #     # check if data is available
        
    #     print('-----------------------------')
    #     print('Checking data availability...')
    #     print('-----------------------------')

    #     try:
    #         _null_Q_perc = self._sim_dataset.data[self._flow_column].isnull().sum() / len(self._sim_dataset.data[self._flow_column])*100
    #         _zero_Q_perc = (self._sim_dataset.data[self._flow_column] == 0).sum() / len(self._sim_dataset.data[self._flow_column])*100
    #         print('Percentage of Null in Flow dataset: ', round(_null_Q_perc,2), '%')
    #         print('Percentage of Zero in Flow dataset: ', round(_zero_Q_perc,2), '%')
    #         print('----------')
    #     except:
    #         print('Flow data is not available!')
    #     else:
    #         if _null_Q_perc >= 50 or _zero_Q_perc >= 50:
    #             _flow_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of Flow data are missing!')
    #         else:
    #             _flow_data_is_available = True

    #     try:
    #         _null_cod_perc = self._sim_dataset.data[self._cod_column].isnull().sum() / len(self._sim_dataset.data[self._cod_column])*100
    #         _zero_cod_perc = (self._sim_dataset.data[self._cod_column] == 0).sum() / len(self._sim_dataset.data[self._cod_column])*100
    #         print('Percentage of Null in COD dataset: ', round(_null_cod_perc,2), '%')
    #         print('Percentage of Zero in COD dataset: ', round(_zero_cod_perc,2), '%')
    #         print('----------')
    #     except:
    #         print('COD data is not available!')
    #     else:
    #         if _null_cod_perc >= 50 or _zero_cod_perc >= 50:
    #             _cod_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of COD data are missing! Only influent model will be used for data filling!')
    #         else:
    #             _cod_data_is_available = True
        
            
    #     try:
    #         _null_cods_perc = self._sim_dataset.data[self._cods_column].isnull().sum() / len(self._sim_dataset.data[self._cods_column])*100
    #         _zero_cods_perc = (self._sim_dataset.data[self._cods_column] == 0).sum() / len(self._sim_dataset.data[self._cods_column])*100
    #         print('Percentage of Null in CODs dataset: ', round(_null_cods_perc,2), '%')
    #         print('Percentage of Zero in CODs dataset: ', round(_zero_cods_perc,2), '%')
    #         print('----------')
    #     except:
    #         print('Soluble COD data is not available!')
    #     else:
    #         if _null_cods_perc >= 50 or _zero_cods_perc >=50:
    #             _cods_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of soluble COD data are missing! Only influent model will be used for data filling!')
    #         else:
    #             _cods_data_is_available = True
    #     try:
    #         _null_tss_perc = self._sim_dataset.data[self._tss_column].isnull().sum() / len(self._sim_dataset.data[self._tss_column])*100
    #         _zero_tss_perc = (self._sim_dataset.data[self._tss_column] == 0).sum() / len(self._sim_dataset.data[self._tss_column])*100
    #         print('Percentage of Null in TSS dataset: ', round(_null_tss_perc,2), '%')
    #         print('Percentage of Zero in TSS dataset: ', round(_zero_tss_perc, 2), '%')
    #         print('----------')
    #     except:
    #         print('TSS data is not available!')
    #     else:
    #         if _null_tss_perc >= 50 or _zero_tss_perc >= 50:
    #             _tss_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of TSS data are missing! Only influent model will be used for data filling!')
    #         else:
    #             _tss_data_is_available = True
    #     try:    
    #         _null_amm_perc = self._sim_dataset.data[self._amm_column].isnull().sum() / len(self._sim_dataset.data[self._amm_column])*100
    #         _zero_amm_perc = (self._sim_dataset.data[self._amm_column] == 0).sum() / len(self._sim_dataset.data[self._amm_column])*100
    #         print('Percentage of Null in NH4 dataset: ', round(_null_amm_perc, 2), '%')
    #         print('Percentage of Zero in NH4 dataset: ', round(_zero_amm_perc, 2), '%')
    #         print('----------')
    #     except:
    #         print('Ammonia data is not available!')
    #     else:
    #         if _null_amm_perc >= 50 or _zero_amm_perc >= 50:
    #             _amm_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of Ammonia data are missing! Only influent model will be used for data filling!')
    #         else:
    #             _amm_data_is_available = True
    #     try:
    #         _null_phos_perc = self._sim_dataset.data[self._phos_column].isnull().sum() / len(self._sim_dataset.data[self._phos_column])*100
    #         _zero_phos_perc = (self._sim_dataset.data[self._phos_column] == 0).sum() / len(self._sim_dataset.data[self._phos_column])*100
    #         print('Percentage of Null in PO4 dataset: ', round(_null_phos_perc, 2), '%')
    #         print('Percentage of Zero in PO4 dataset: ', round(_zero_phos_perc, 2), '%')
    #         print('----------')
    #     except:
    #         print('Phosphate data is not available!')
    #     else:
    #         if _null_phos_perc >= 50 or _zero_phos_perc >= 50:
    #             _phos_data_is_available = False
    #             if self._advanced_log:
    #                 print('More than 50% of Phosphate data are missing! Only influent model will be used for data filling!')
    #         else:
    #             _phos_data_is_available = True
        

    #     # _null_perc_values = {'COD': _null_cod_perc, 'CODs': _null_cods_perc, 'TSS': _null_tss_perc, 'AMM': _null_amm_perc, 'PHOS': _null_phos_perc}
    #     # _data_is_available = {}
    #     # for comp , perc in _null_perc_values.items():
    #     #     if perc >= 0.5:
    #     #         _data_is_available[comp] = False
    #     #         if self._advanced_log:
    #     #             print('More than 50% of {} data are missing! Only influent model will be used for data filling!'.format(comp))

    #     #     else:
    #     #         _data_is_available[comp] = True
    #     #         if self._advanced_log:
    #     #             print('Sufficient {} data are available!'.format(comp))


    #     #############################
    #     ##### filter rain event #####
    #     #############################
    #     if filter_rain:
    #         try:
    #             self._sim_dataset.get_highs(self._flow_column,rain_event_value,arange=[self._subset_range[0], self._subset_range[1]], method=rain_filter_method,plot=False)
    #         except:
    #             print('Flow data is not available!')
        
        
    #     # self._rain_events_ = self._rain_events(threshold=6)
        
    #     ######################
    #     ##### flow data #####
    #     ######################
    #     print('########################')
    #     print('###### Flow data #######')
    #     print('########################')
        
    #     try:
    #         if self._advanced_log:
    #             print('--------------------')
    #             print('Filtering and filling flow data...')
    #             print('--------------------')
            

    #         self._sim_dataset.tag_nan(self._flow_column, arange=self._subset_range, clear=True)
    #         self._sim_dataset.tag_doubles(self._flow_column, bound=50, arange=self._subset_range)

          

    #         self._sim_dataset.fill_missing_interpolation(self._flow_column, 12, arange=self._subset_range, clear=False)
            
    #         # TO DO
    #         # if rainfall data is available, check for dry weather period
    #         # if dry: missing_standard: 
    #         if self._typical_dry_profile:
    #             self._sim_dataset.fill_missing_standard(self._flow_column,arange=self._subset_range,only_checked=True,clear=False)
            
    #         #if not and inf model is available:
    #         # missing_model


    #         # if not and inf model is not available:
    #         # missing_standard with warning
            
            
    #         self._sim_dataset.filled['Q_final'] = self._sim_dataset.filled[self._flow_column]

    #         print('Flow data has been succesfully filtered and filled!')
    #     except:
    #         print('Flow data is not available!')


       
        
    #     ######################
    #     ## COD and TSS data ##
    #     ######################
    #     print('########################')
    #     print('##### COD/TSS data #####')
    #     print('########################')
        
    #     try:
    #         if _cod_data_is_available:
    #             # if data is available
    #             # filtering and filling
                
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filtering and filling COD data...')
    #                 print('--------------------')
                
    #             self._sim_dataset.tag_doubles(self._cod_column, clear=True, bound=0.1)
    #             self._sim_dataset.moving_slope_filter(self._sim_dataset.timename, self._cod_column, 90000, arange=self._subset_range, time_unit='min', plot=False)
    #             self._sim_dataset.moving_average_filter(self._cod_column, 12, 0.2, arange=self._subset_range, plot=False)
                
                
    #             self._sim_dataset.fill_missing_interpolation(self._cod_column, 12, arange=self._subset_range)
    #             self._sim_dataset.fill_missing_model(self._cod_column,self._inf_mod_dataset[self._model_col_names['COD']],self._subset_range,only_checked=True,clear=False)
    #             self._sim_dataset.filled['COD_final'] = self._sim_dataset.filled[self._cod_column]
    #             print('COD  data have been succesfully filtered and filled!')
            
    #         else:
    #             # if data is not available
    #             # filling dataset with inf model
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filling COD data...')
    #                 print('--------------------')
    #             self._sim_dataset.meta_valid[['TOT_COD']] = pd.DataFrame(np.array([['original']*len(self._sim_dataset.meta_valid)]).T,index=self._sim_dataset.index())
    #             self._sim_dataset.data['TOT_COD'] = pd.DataFrame(np.array([[np.nan]*len(self._sim_dataset.meta_valid)]).T, index=self._sim_dataset.index())
    #             self._sim_dataset.fill_missing_model('TOT_COD',self._inf_mod_dataset[self._model_col_names['COD']],self._subset_range,only_checked=False,clear=True)
    #             self._sim_dataset.filled['COD_final'] = self._sim_dataset.filled['TOT_COD']
    #             print('COD  data have been succesfully filled!')
    #     except:
    #         print('COD data cannot be analyzed!')   


    #     try:
    #         if _cods_data_is_available:
    #             # if data is available
    #             # filtering and filling
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filtering and filling soluble COD data...')
    #                 print('--------------------')
                
    #             self._sim_dataset.tag_doubles(self._cods_column, clear=True, bound=5)
    #             self._sim_dataset.moving_slope_filter(self._sim_dataset.timename, self._cods_column, 30000, arange=self._subset_range, time_unit='min', plot=False)
    #             self._sim_dataset.moving_average_filter(self._cods_column, 12, 0.2, arange=self._subset_range, plot=False)

    #             self._sim_dataset.fill_missing_interpolation(self._cods_column, 12, arange=self._subset_range)
    #             self._sim_dataset.fill_missing_model(self._cods_column,self._inf_mod_dataset[self._model_col_names['CODs']],self._subset_range,only_checked=True,clear=False)
    #             self._sim_dataset.filled['CODs_final'] = self._sim_dataset.filled[self._cods_column]
    #             print('Soluble COD  data have been succesfully filtered and filled!')
    #         else:
    #             # if data is not available
    #             # filling dataset with inf model
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filling soluble COD data...')
    #                 print('--------------------')
    #             self._sim_dataset.meta_valid[['TOT_CODs']] = pd.DataFrame(np.array([['original']*len(self._sim_dataset.meta_valid)]).T,index=self._sim_dataset.index())
    #             self._sim_dataset.data['TOT_CODs'] = pd.DataFrame(np.array([[np.nan]*len(self._sim_dataset.meta_valid)]).T, index=self._sim_dataset.index())
    #             self._sim_dataset.fill_missing_model('TOT_CODs',self._inf_mod_dataset[self._model_col_names['CODs']],self._subset_range,only_checked=False,clear=False)
    #             self._sim_dataset.filled['CODs_final'] = self._sim_dataset.filled['TOT_CODs']
                
    #             print('COD  data have been succesfully filled!')
    #     except:
    #         print('Soluble COD data cannot be analyzed!')
        
    #     try:
    #         if _tss_data_is_available:
    #             # if data is available
    #             # filtering and filling
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filtering and filling TSS data...')
    #                 print('--------------------')
                
    #             self._sim_dataset.tag_doubles(self._tss_column, clear=True, bound=5)
    #             self._sim_dataset.moving_slope_filter(self._sim_dataset.timename, self._tss_column, 60000, arange=self._subset_range, time_unit='min', plot=False)
    #             self._sim_dataset.moving_average_filter(self._tss_column, 12, 0.2, arange=self._subset_range, plot=False)

    #             self._sim_dataset.fill_missing_interpolation(self._tss_column, 12, arange=self._subset_range)
    #             self._sim_dataset.fill_missing_model(self._tss_column,self._inf_mod_dataset[self._model_col_names['TSS']],self._subset_range,only_checked=True,clear=False)
    #             self._sim_dataset.filled['TSS_final'] = self._sim_dataset.filled[self._tss_column]

    #             print('TSS  data have been succesfully filtered and filled!')
                
    #         else:
    #             # if data is not available
    #             # filling dataset with inf model
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filling TSS data...')
    #                 print('--------------------')
    #             self._sim_dataset.meta_valid[['TOT_TSS']] = pd.DataFrame(np.array([['original']*len(self._sim_dataset.meta_valid)]).T,index=self._sim_dataset.index())
    #             self._sim_dataset.data['TOT_TSS'] = pd.DataFrame(np.array([[np.nan]*len(self._sim_dataset.meta_valid)]).T, index=self._sim_dataset.index())
    #             self._sim_dataset.fill_missing_model('TOT_TSS',self._inf_mod_dataset[self._model_col_names['TSS']],self._subset_range,only_checked=False,clear=False)
    #             self._sim_dataset.filled['TSS_final'] = self._sim_dataset.filled['TOT_TSS']

    #             print('TSS data have been succesfully filled!')
    #     except:
    #         print('TSS data cannot be analyzed!')


    #     ######################
    #     #### ammonia data ####
    #     ######################
    #     print('########################')
    #     print('##### Ammonia data #####')
    #     print('########################')

    #     try:
    #         if _amm_data_is_available:
    #             # if data is available
    #             # filtering and filling
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filtering and filling ammonia data...')
    #                 print('--------------------')
                
    #             self._sim_dataset.tag_doubles(self._amm_column, clear=True, bound=0.01)
    #             self._sim_dataset.moving_slope_filter(self._sim_dataset.timename, self._amm_column, 3720, arange=self._subset_range, time_unit='min', plot=False)
    #             self._sim_dataset.moving_average_filter(self._amm_column, 12, 0.2, arange=self._subset_range, plot=False)

    #             self._sim_dataset.fill_missing_interpolation(self._amm_column, 12, arange=self._subset_range)
                
    #             # self._sim_dataset.fill_missing_model(self._amm_column,self._inf_mod_dataset[self._model_col_names['NH4']],self._subset_range,only_checked=True,clear=True)
                
    #             self._sim_dataset.filled['NH4_final'] = self._sim_dataset.filled[self._amm_column]
    #             print('Ammonia  data have been succesfully filtered and filled!')

    #         else:
    #             # if data is not available
    #             # filling dataset with inf model
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filling ammonia data...')
    #                 print('--------------------')
    #             self._sim_dataset.meta_valid[['TOT_AMM']] = pd.DataFrame(np.array([['original']*len(self._sim_dataset.meta_valid)]).T,index=self._sim_dataset.index())
    #             self._sim_dataset.data['TOT_AMM'] = pd.DataFrame(np.array([[np.nan]*len(self._sim_dataset.meta_valid)]).T, index=self._sim_dataset.index())
    #             self._sim_dataset.fill_missing_model('TOT_AMM',self._inf_mod_dataset[self._model_col_names['NH4']],self._subset_range,only_checked=False,clear=False)
    #             self._sim_dataset.filled['NH4_final'] = self._sim_dataset.filled['TOT_AMM']

    #             print('Ammonia data have been succesfully filled!')
    #     except Exception as e:
    #         print(e)
    #         print('Ammonia data cannot be analyzed!')






    #     ######################
    #     ### phosphate data ###
    #     ######################
    #     print('########################')
    #     print('#### Phosphate data ####')
    #     print('########################')

    #     try:
    #         if _phos_data_is_available:
    #             # if data is available
    #             # filtering and filling
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filtering and filling phosphate data...')
    #                 print('--------------------')
    #                 print('to be implemented!')
                
    #             print('Phosphate  data have been succesfully filtered and filled!')
    #         else:
    #             # if data is not available
    #             # filling dataset with inf model
    #             if self._advanced_log:
    #                 print('--------------------')
    #                 print('Filling phosphate data...')
    #                 print('--------------------')
    #             self._sim_dataset.meta_valid[['TOT_PHOS']] = pd.DataFrame(np.array([['original']*len(self._sim_dataset.meta_valid)]).T,index=self._sim_dataset.index())
    #             self._sim_dataset.data['TOT_PHOS'] = pd.DataFrame(np.array([[np.nan]*len(self._sim_dataset.meta_valid)]).T, index=self._sim_dataset.index())
    #             self._sim_dataset.fill_missing_model('TOT_PHOS',self._inf_mod_dataset[self._model_col_names['PO4']],self._subset_range,only_checked=False,clear=False)
    #             self._sim_dataset.filled['PO4_final'] = self._sim_dataset.filled['TOT_PHOS']

    #             print('Phosphate data have been succesfully filled!')
    #     except:
    #         print('Phosphate data cannot be analyzed!')
        
        ##################################
        ### calculate proportionaliies ###
        ##################################



        
        


    ####################################
    ### create WEST compatible files ###
    ####################################
    # def create_west_model_input(self, path_to_west_model, influent_file_name):
    #     """
    #     creates input files for the model from the processed data
    #     and store them in the model project directory

    #     Parameters
    #     ----------
    #     path_to_west_model : str
    #         path to the folder containing the model project files
    #     influent_file_name : str
    #         name of the influent file, should be the same as the influent file name
    #         already present in the project folder
        
    #     Returns
    #     -------
    #     None
        
    #     """
    #     self._path_to_west_model = path_to_west_model
    #     self._influent_file_name = influent_file_name

    #     self._sim_index = self._sim_dataset.absolute_to_relative(inplace=False).set_index('time_rel').index()
    #     try:
    #         self._filled = ww.OnlineSensorBased(self._sim_dataset.filled)
    #     except:
    #         print('No data have been processed! model inputs will not be created.')
    #     else:
    #         self._filled.absolute_to_relative()
            
    #         self._first_sim_day = self._sim_dataset.index()[0].timetuple().tm_yday
    #         self._last_sim_day = self._sim_dataset.index()[-1].timetuple().tm_yday
    #         self._sim_index = self._filled.time + self._first_sim_day

    #         _data_for_WEST = pd.DataFrame(index=self._sim_index)
    #         try:
    #             _data_for_WEST['CODs'] = self._filled.data['CODs_final'].values
    #         except:
    #             _data_for_WEST['CODs'] = 0
    #         try:
    #             _data_for_WEST['CODt'] = self._filled.data['COD_final'].values
    #         except:
    #             _data_for_WEST['CODt'] = 0
    #         try:
    #             _data_for_WEST['NH'] = self._filled.data['NH4_final'].values
    #         except:
    #             _data_for_WEST['NH'] = 0
    #         try:
    #             _data_for_WEST['PO'] = self._filled.data['PO4_final'].values
    #         except:
    #             _data_for_WEST['PO'] = 0    
    #         try:    
    #             _data_for_WEST['TSS'] = self._filled.data['TSS_final'].values
    #         except:
    #             _data_for_WEST['TSS'] = 0
    #         try:    
    #             _data_for_WEST['Water'] = self._filled.data['Q_final'].values*24
    #         except:
    #             _data_for_WEST['Water'] = 1
            
            
    #         _units = ['g/m3','g/m3','g/m3','g/m3','g/m3','m3/d']

    #         write_to_WEST(_data_for_WEST,'2013_simulation.txt', self._influent_file_name,
    #                 _units,filepath=self._path_to_west_model)

    #         # change name to processed_data
    #         write_to_db(_data_for_WEST, 'data_for_west', self._conn, 'append', index=True, index_label=None)

    
    # def create_sumo_model_input(self):
    #     pass
        
    ####################################
    ######### create csv files #########
    ####################################

    # def create_csv_file(self, path_to_csv, file_name, tagnames, time_zone='Europe/Brussels'):
    #     """
    #     create csv file from the processed data and store
    #     them in the specified directory

    #     Parameters
    #     ----------
    #     path_to_csv : str
    #         path to store the csv files
    #     file_name : str
    #         name of the csv file
    #     tagnames : list
    #         list of the column names for the dataset
    #     time_zone : str
    #         time zone for formatting the datetime index 
        
    #     Returns
    #     -------
    #     None

    #     """    
        
    #     try:
    #         self._filled = ww.OnlineSensorBased(self._sim_dataset.filled)
    #     except:
    #         print('No data have been processed! csv files will not be created.')
    #     else:
            
    #     # check if it is necessary to use tagnames, what are available columns in filled? 
    #         _data_to_csv = pd.DataFrame(self._filled.index, columns=list(tagnames.keys()))
    #         for key in tagnames.keys():
    #             _data_to_csv[key] = self._filled[key]


    #         _data_to_csv.index = _data_to_csv.index.map(lambda x: pd.Timestamp(x, tz=time_zone).isoformat())


    #         _data_to_csv.to_csv(path_to_csv + file_name)
        






#############################
#### Non class functions ####
#############################

