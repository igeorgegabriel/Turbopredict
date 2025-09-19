"""
Hybrid anomaly detection for Option [2]

Primary scanners:
- 2.5-sigma (per-tag z-score)
- AutoEncoder signals from `AutoEncoder/` directory (if available)

Verification:
- MTD-style threshold verification using baseline tag thresholds when available
- Isolation Forest (per-tag, univariate)

This module is designed to plug into the existing Option [2] path by exposing
`enhanced_anomaly_detection(df, unit)` with the same signature used by
`smart_anomaly_detection`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional, Set, Tuple, List
import logging
import json

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# TensorFlow/Keras is optional; gated by env ENABLE_AE_LIVE
import os
AE_LIVE_ENABLED = os.getenv('ENABLE_AE_LIVE', '').strip().lower() in ('1', 'true', 'yes', 'y')
# Gate to require AE participation before running verification
REQUIRE_AE_FOR_VERIFY = os.getenv('REQUIRE_AE', '').strip().lower() in ('1', 'true', 'yes', 'y')
try:
    if AE_LIVE_ENABLED:
        import tensorflow as tf  # noqa: F401
        from tensorflow.keras.models import load_model  # type: ignore
        TF_AVAILABLE = True
    else:
        TF_AVAILABLE = False
except Exception:
    TF_AVAILABLE = False


def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if 'time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['time']):
        try:
            df = df.copy()
            df['time'] = pd.to_datetime(df['time'])
        except Exception:
            pass
    return df


def _load_baseline_thresholds(unit: Optional[str]) -> Dict[str, Dict[str, float]]:
    """Load per-tag thresholds from `baseline_config_{unit}.json` if present.

    Returns a mapping: tag -> { 'upper_limit': float, 'lower_limit': float, 'outlier_sigma': float }
    """
    thresholds: Dict[str, Dict[str, float]] = {}
    if not unit:
        return thresholds
    try:
        cfg_path = Path(f"baseline_config_{unit}.json")
        if cfg_path.exists():
            with open(cfg_path, 'r') as f:
                cfg = json.load(f)
            tag_cfg = cfg.get('tag_configurations', {}) or {}
            for tag, tcfg in tag_cfg.items():
                th = tcfg.get('thresholds', {}) or {}
                if 'upper_limit' in th and 'lower_limit' in th:
                    thresholds[tag] = {
                        'upper_limit': float(th['upper_limit']),
                        'lower_limit': float(th['lower_limit']),
                        'outlier_sigma': float(th.get('outlier_sigma', 2.5))
                    }
    except Exception as e:
        logger.warning(f"Failed to load baseline thresholds for {unit}: {e}")
    return thresholds


def _sigma_2p5_candidates(df: pd.DataFrame) -> Tuple[Set[pd.Timestamp], Dict[str, Any], Dict[str, Set[pd.Timestamp]]]:
    """Compute 2.5-sigma candidates per tag.

    Returns (candidate_times, summary_by_tag, times_by_tag)
    where times_by_tag maps each tag to the set of candidate timestamps.
    """
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return set(), {}

    # Compute per-tag mean and std
    try:
        grouped = df[['tag', 'value', 'time']].dropna(subset=['value'])
        # Ensure datetime for accurate set operations later
        grouped = _ensure_datetime(grouped)
        # Using groupby to compute mean/std
        stats = grouped.groupby('tag')['value'].agg(['mean', 'std'])
        stats = stats.replace({np.nan: 0.0})

        # Join to compute z-scores efficiently
        joined = grouped.join(stats, on='tag', how='left')
        # Avoid division by zero
        joined['std'] = joined['std'].replace(0.0, np.nan)
        joined['z'] = (joined['value'] - joined['mean']) / joined['std']
        mask = joined['z'].abs() > 2.5
        cand_rows = joined[mask]

        candidate_times: Set[pd.Timestamp] = set()
        by_tag: Dict[str, Any] = {}
        times_by_tag: Dict[str, Set[pd.Timestamp]] = {}
        if not cand_rows.empty:
            # Collect times; ensure tz-aware consistency
            times = pd.to_datetime(cand_rows['time'])
            candidate_times = set(times.dt.tz_convert(None) if times.dt.tz is not None else times)

            # Per-tag counts and rates
            counts = cand_rows.groupby('tag').size()
            totals = grouped.groupby('tag').size()
            for tag, cnt in counts.items():
                total = int(totals.get(tag, 0)) or 1
                by_tag[tag] = {
                    'count': int(cnt),
                    'rate': float(cnt) / float(total),
                    'method': 'zscore_2p5'
                }
                # collect candidate times per tag
                tseries = pd.to_datetime(cand_rows[cand_rows['tag'] == tag]['time'])
                if getattr(tseries.dt, 'tz', None) is not None:
                    tseries = tseries.dt.tz_convert(None)
                times_by_tag[tag] = set(tseries.dropna())

        return candidate_times, by_tag, times_by_tag

    except Exception as e:
        logger.warning(f"2.5-sigma candidate detection failed: {e}")
        return set(), {}


def _load_autoencoder_anomaly_times(project_root: Path) -> Set[pd.Timestamp]:
    """Load precomputed AE anomaly timestamps if available.

    This avoids introducing a hard dependency on TensorFlow at runtime.
    """
    ae_dir = project_root / 'AutoEncoder'
    if not ae_dir.exists():
        return set()
    csv_path = ae_dir / 'anomaly_timestamps_hybrid.csv'
    if not csv_path.exists():
        return set()
    try:
        ts = pd.read_csv(csv_path)
        if 'timestamp' not in ts.columns:
            return set()
        times = pd.to_datetime(ts['timestamp'], errors='coerce')
        times = times.dropna()
        if times.empty:
            return set()
        # Normalize to naive timestamps for set operations
        if getattr(times.dt, 'tz', None) is not None:
            times = times.dt.tz_convert(None)
        return set(times)
    except Exception as e:
        logger.warning(f"Failed to load AE anomaly times: {e}")
        return set()


def _verify_candidates_with_mtd_if(
    df: pd.DataFrame,
    candidate_times: Set[pd.Timestamp],
    baseline_thresholds: Dict[str, Dict[str, float]],
) -> Tuple[Dict[str, Any], int, Dict[str, Dict[str, Set[pd.Timestamp]]]]:
    """Verify candidate (tag, time) anomalies using MTD-like thresholds and Isolation Forest.

    - MTD-like: per-tag threshold using baseline upper/lower if available, else 2.5-sigma
    - IF: per-tag univariate Isolation Forest on the tag's full series; a candidate point is
      confirmed if IF marks it as an outlier (-1)

    Returns (by_tag_verified, total_verified)
    """
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return {}, 0

    df = _ensure_datetime(df)
    # Normalize times to naive to match candidate_times set
    times_series = pd.to_datetime(df['time'], errors='coerce')
    if getattr(times_series.dt, 'tz', None) is not None:
        df_times = times_series.dt.tz_convert(None)
    else:
        df_times = times_series

    df_local = df.copy()
    df_local['_t'] = df_times

    # Precompute per-tag stats for fallback thresholds
    stats = df_local.dropna(subset=['value']).groupby('tag')['value'].agg(['mean', 'std'])
    stats = stats.replace({np.nan: 0.0})

    by_tag_verified: Dict[str, Any] = {}
    total_verified = 0
    detail_times: Dict[str, Dict[str, Set[pd.Timestamp]]] = {}

    # Group by tag for efficiency
    for tag, tag_df in df_local.groupby('tag'):
        tag_df = tag_df.dropna(subset=['value', '_t'])
        if tag_df.empty:
            continue

        # Candidate rows for this tag by time intersection
        cand_mask = tag_df['_t'].isin(candidate_times)
        cand_rows = tag_df[cand_mask]
        if cand_rows.empty:
            continue

        # MTD-like thresholds
        if tag in baseline_thresholds:
            th = baseline_thresholds[tag]
            lo, up = th['lower_limit'], th['upper_limit']
            used_sigma = th.get('outlier_sigma', 2.5)
        else:
            mu = float(stats.loc[tag, 'mean']) if tag in stats.index else float(tag_df['value'].mean())
            sd = float(stats.loc[tag, 'std']) if tag in stats.index else float(tag_df['value'].std())
            used_sigma = 2.5
            # Use 2.5-sigma as per requirement
            up = mu + used_sigma * sd
            lo = mu - used_sigma * sd

        mtd_conf_mask = (cand_rows['value'] < lo) | (cand_rows['value'] > up)
        mtd_confirmed_times: Set[pd.Timestamp] = set(cand_rows.loc[mtd_conf_mask, '_t'])

        # Isolation Forest verification (univariate)
        if_confirmed_times: Set[pd.Timestamp] = set()
        if SKLEARN_AVAILABLE and len(tag_df) >= 30:
            try:
                X = tag_df['value'].values.reshape(-1, 1)
                # Conservative contamination; we are only verifying candidates
                iso = IsolationForest(contamination=0.02, random_state=42)
                labels = iso.fit_predict(X)
                # Map indices to times
                outlier_idx = np.where(labels == -1)[0]
                # Build a set of outlier times
                outlier_times = set(tag_df.iloc[outlier_idx]['_t'])
                # Confirm only candidate times
                if_confirmed_times = outlier_times.intersection(set(cand_rows['_t']))
            except Exception as e:
                logger.debug(f"IF verification failed for tag {tag}: {e}")

        # Combine confirmations (union of MTD and IF confirmations)
        confirmed_times = mtd_confirmed_times.union(if_confirmed_times)
        confirmed_count = len(confirmed_times)
        if confirmed_count <= 0:
            # Still record candidate info for traceability
            by_tag_verified[tag] = {
                'count': 0,
                'rate': 0.0,
                'method': 'hybrid_verified',
                'verification_breakdown': {
                    'mtd_confirmed': len(mtd_confirmed_times),
                    'if_confirmed': len(if_confirmed_times),
                    'candidates': int(cand_rows.shape[0])
                }
            }
            continue

        total_verified += confirmed_count
        detail_times[tag] = {
            'mtd': set(mtd_confirmed_times),
            'if': set(if_confirmed_times),
        }
        by_tag_verified[tag] = {
            'count': int(confirmed_count),
            'rate': float(confirmed_count) / float(len(tag_df) or 1),
            'method': 'hybrid_verified',
            'verification_breakdown': {
                'mtd_confirmed': len(mtd_confirmed_times),
                'if_confirmed': len(if_confirmed_times),
                'candidates': int(cand_rows.shape[0])
            }
        }

    return by_tag_verified, total_verified, detail_times


def _load_ae_assets(project_root: Path):
    """Load AE model, scaler, and features if available.

    Returns (model, scaler, features) or (None, None, None)
    """
    if not TF_AVAILABLE:
        return None, None, None
    try:
        ae_dir = project_root / 'AutoEncoder'
        model_path = ae_dir / 'autoencoder.keras'
        scaler_path = ae_dir / 'scaler.pkl.gz'
        feats_path = ae_dir / 'features_used.csv'
        if not (model_path.exists() and feats_path.exists() and scaler_path.exists()):
            return None, None, None

        # Load model
        model = load_model(str(model_path))

        # Load scaler
        scaler = None
        try:
            import joblib  # type: ignore
            scaler = joblib.load(str(scaler_path))
        except Exception:
            import gzip, pickle
            with gzip.open(scaler_path, 'rb') as f:
                scaler = pickle.load(f)

        # Load features order
        feats_df = pd.read_csv(feats_path)
        features: List[str] = [str(x) for x in feats_df.iloc[:, 0].dropna().tolist()]
        if not features:
            return None, None, None

        return model, scaler, features

    except Exception as e:
        logger.warning(f"Failed to load AE assets: {e}")
        return None, None, None


def _load_feature_mapping(project_root: Path) -> Dict[str, Any]:
    """Load optional feature->tag mapping for AE construction.

    Expected format (JSON): { "K-12-01": "PCFS_K-12-01_..._PV", ... }
    Values may be a string (exact tag) or array of strings (patterns).
    """
    try:
        mpath = project_root / 'AutoEncoder' / 'feature_mapping.json'
        if mpath.exists():
            with open(mpath, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load feature mapping: {e}")
    return {}


def _build_ae_frame_from_df(
    df: pd.DataFrame,
    features: List[str],
    mapping: Dict[str, Any]
) -> Optional[pd.DataFrame]:
    """Build a wide feature frame (time index, columns=features) from df.

    Strategy:
    - Pivot df to time x tag.
    - For each requested feature name:
      - If mapping provides an exact tag, use that column.
      - Else, use the mean across columns whose name contains the feature key.
    - Require coverage >= AE_MIN_FEATURE_COVERAGE (default 0.8) of features with data.
    """
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return None

    try:
        df = _ensure_datetime(df)
        piv = df.pivot_table(index='time', columns='tag', values='value', aggfunc='mean')
        piv = piv.sort_index().ffill()
        available_cols = list(piv.columns)

        agg_mode = os.getenv('AUTOENCODER_FEATURE_AGG', 'mean').lower()
        min_cov = float(os.getenv('AE_MIN_FEATURE_COVERAGE', '0.8'))

        data_cols = {}
        for feat in features:
            series = None
            # Use mapping if provided
            if mapping:
                mval = mapping.get(feat)
                if isinstance(mval, str) and mval in piv.columns:
                    series = piv[mval]
                elif isinstance(mval, list) and mval:
                    cols = []
                    for pat in mval:
                        cols.extend([c for c in available_cols if pat in c])
                    cols = sorted(set(cols))
                    if cols:
                        sub = piv[cols]
                        series = sub.mean(axis=1) if agg_mode == 'mean' else sub.median(axis=1)
            # Fallback heuristic: substring match on feature name
            if series is None:
                cols = [c for c in available_cols if feat in c]
                if cols:
                    sub = piv[cols]
                    series = sub.mean(axis=1) if agg_mode == 'mean' else sub.median(axis=1)

            if series is not None:
                data_cols[feat] = series

        coverage = len(data_cols) / max(1, len(features))
        if coverage < min_cov or len(data_cols) < 2:
            # Not enough features to run AE reliably
            return None

        # Build aligned frame with exactly the found features; reindex to intersection of valid rows
        frame = pd.DataFrame(data_cols)
        # Drop rows with any NA
        frame = frame.dropna()
        if frame.empty:
            return None
        return frame
    except Exception as e:
        logger.debug(f"Failed to build AE frame: {e}")
        return None


def _try_live_ae_anomaly_times(df: pd.DataFrame, project_root: Path) -> Set[pd.Timestamp]:
    """Optionally compute AE anomaly times from df using TF/Keras model.

    Enabled when env ENABLE_AE_LIVE is truthy and TF is available. Returns a set of
    timestamps whose reconstruction error exceeds a high-quantile threshold.
    """
    if not (AE_LIVE_ENABLED and TF_AVAILABLE):
        return set()

    try:
        model, scaler, features = _load_ae_assets(project_root)
        if model is None or scaler is None or not features:
            return set()

        mapping = _load_feature_mapping(project_root)
        X = _build_ae_frame_from_df(df, features, mapping)
        if X is None:
            return set()

        # Ensure columns align with training feature order, drop extras
        missing = [f for f in features if f not in X.columns]
        if missing:
            # If some features missing, bail; we enforce coverage earlier, but AE requires exact order
            return set()
        X = X[features]

        # Scale
        try:
            X_scaled = scaler.transform(X.values)
        except Exception:
            # Fallback: try fit_transform (not ideal, but keeps system running)
            X_scaled = scaler.fit_transform(X.values)

        # Predict reconstruction and compute MSE per row
        X_rec = model.predict(X_scaled, verbose=0)
        errs = np.mean((X_scaled - X_rec) ** 2, axis=1)

        # Threshold via high quantile; env override AE_Q (default 0.995)
        q = float(os.getenv('AE_Q', '0.995'))
        q = min(0.9999, max(0.9, q))
        thr = float(np.quantile(errs, q))
        anom_idx = np.where(errs > thr)[0]
        if len(anom_idx) == 0:
            return set()

        times = X.index.to_series().iloc[anom_idx]
        # Normalize to naive timestamps
        times = pd.to_datetime(times, errors='coerce')
        if getattr(times.dt, 'tz', None) is not None:
            times = times.dt.tz_convert(None)
        return set(times.dropna())

    except Exception as e:
        logger.warning(f"AE live inference failed: {e}")
        return set()


def enhanced_anomaly_detection(df: pd.DataFrame, unit: Optional[str] = None) -> Dict[str, Any]:
    """Hybrid detection entry point used by Option [2] with unit status awareness upstream.

    Steps:
    1) Find primary candidates using 2.5-sigma per tag and AutoEncoder timestamps (if available)
    2) Verify candidates with per-tag MTD-like thresholds (baseline-config if present) and Isolation Forest
    3) Aggregate and return results in the standard structure expected by the pipeline
    """
    try:
        df = _ensure_datetime(df)
        total_records = int(len(df))
        unique_tags = int(len(df['tag'].unique())) if 'tag' in df.columns else 0

        # Stage 1: 2.5-sigma candidates
        sigma_times, sigma_by_tag, sigma_times_by_tag = _sigma_2p5_candidates(df)
        sigma_total = sum(info.get('count', 0) for info in sigma_by_tag.values())

        # Stage 1b: AutoEncoder anomaly times
        project_root = Path(__file__).resolve().parents[1]
        # Try live inference first (optional); fallback to file-based if none
        ae_times = _try_live_ae_anomaly_times(df, project_root)
        ae_csv_times = set()
        if not ae_times:
            ae_csv_times = _load_autoencoder_anomaly_times(project_root)
            ae_times = ae_csv_times
        # Restrict AE times to the timeframe present in df to avoid counting unrelated timestamps
        if 'time' in df.columns and not df.empty and ae_times:
            tmin = pd.to_datetime(df['time']).min()
            tmax = pd.to_datetime(df['time']).max()
            if getattr(tmin, 'tzinfo', None) is not None:
                tmin = tmin.tz_convert(None)
            if getattr(tmax, 'tzinfo', None) is not None:
                tmax = tmax.tz_convert(None)
            ae_times = {t for t in ae_times if tmin <= t <= tmax}
        ae_total = len(ae_times)

        # Determine whether AE analysis was actually attempted/completed
        ae_available = bool(AE_LIVE_ENABLED) or bool(ae_csv_times)
        if REQUIRE_AE_FOR_VERIFY and not ae_available:
            # AE is required but not available: return an informative result without verification
            result = {
                'method': 'hybrid_awaiting_ae',
                'total_anomalies': 0,
                'anomaly_rate': 0.0,
                'by_tag': {},
                'config_loaded': False,
                'tags_analyzed': int(len(df['tag'].unique())) if 'tag' in df.columns else 0,
                'primary_candidates': {
                    'sigma_2p5_total': 0,
                    'ae_total': 0,
                    'unique_candidate_timestamps': 0,
                },
                'message': 'AE is required before verification. Set ENABLE_AE_LIVE=1 with model assets or provide AutoEncoder/anomaly_timestamps_hybrid.csv.',
            }
            return result

        # Combine candidate times
        candidate_times = set(sigma_times)
        candidate_times.update(ae_times)

        # Stage 2: Verification using baseline thresholds and IF
        baseline_thresholds = _load_baseline_thresholds(unit)
        by_tag_verified, total_verified, verify_times_by_tag = _verify_candidates_with_mtd_if(
            df, candidate_times, baseline_thresholds
        )

        # Aggregate results
        anomaly_rate_global = (float(total_verified) / float(total_records)) if total_records > 0 else 0.0
        result = {
            'method': 'hybrid_sigma2p5_ae_mtd_if',
            'total_anomalies': int(total_verified),
            'anomaly_rate': float(anomaly_rate_global),
            'by_tag': by_tag_verified,
            'config_loaded': bool(baseline_thresholds),
            'tags_analyzed': unique_tags,
            'primary_candidates': {
                'sigma_2p5_total': int(sigma_total),
                'ae_total': int(ae_total),
                'unique_candidate_timestamps': int(len(candidate_times)),
            }
        }

        # Attach visualization-friendly details without breaking existing consumers
        try:
            # Convert sets to sorted ISO strings for JSON/plotting friendliness
            def _ser_set(s):
                try:
                    return sorted(pd.to_datetime(list(s)).astype('datetime64[ns]').astype(str))
                except Exception:
                    return sorted(str(x) for x in s)

            details_times = {
                'sigma_2p5_times_by_tag': {k: _ser_set(v) for k, v in sigma_times_by_tag.items()},
                'verification_times_by_tag': {
                    k: {
                        'mtd': _ser_set(v.get('mtd', set())),
                        'if': _ser_set(v.get('if', set())),
                    }
                    for k, v in verify_times_by_tag.items()
                },
                'ae_times': _ser_set(ae_times),
            }
            result['details'] = details_times
        except Exception:
            # Non-fatal: keep core results
            pass

        return result

    except Exception as e:
        logger.error(f"Hybrid enhanced detection failed: {e}")
        return {
            'method': 'hybrid_error',
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'by_tag': {},
            'config_loaded': False,
            'error': str(e)
        }
