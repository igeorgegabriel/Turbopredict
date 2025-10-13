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


def _detect_running_state(df: pd.DataFrame, speed_threshold: float = 1.0) -> pd.Series:
    """Detect when equipment is running vs shutdown.

    Args:
        df: DataFrame with 'tag' and 'value' columns
        speed_threshold: Minimum value to consider equipment running

    Returns:
        Boolean series indicating running state for each row
    """
    # Identify speed/flow sensors that indicate equipment operation
    speed_patterns = ['SI', 'SIA', 'SPEED', 'RPM', 'FI', 'FLOW']

    # Find speed-related tags
    speed_tags = []
    for tag in df['tag'].unique():
        if not isinstance(tag, str):
            continue
        tag_upper = tag.upper()
        if any(pattern in tag_upper for pattern in speed_patterns):
            speed_tags.append(tag)

    # If we have speed sensors, use them to determine running state
    if speed_tags:
        speed_data = df[df['tag'].isin(speed_tags)].copy()
        if not speed_data.empty:
            # Group by time and check if any speed sensor shows running
            running_times = speed_data[speed_data['value'] > speed_threshold]['time'].unique()
            return df['time'].isin(running_times)

    # Fallback: use per-tag thresholds (assume running if value > 10% of max)
    running_mask = pd.Series([True] * len(df), index=df.index)
    for tag in df['tag'].unique():
        tag_mask = df['tag'] == tag
        tag_data = df[tag_mask]['value']

        # Calculate tag-specific threshold (10% of operating range)
        tag_max = tag_data.max()
        tag_min = tag_data.min()
        tag_range = tag_max - tag_min

        if tag_range > 1.0:  # Only filter if there's significant range
            threshold = tag_min + (0.1 * tag_range)
            running_mask = running_mask | ((df['tag'] == tag) & (df['value'] > threshold))

    return running_mask


def _sigma_2p5_candidates(df: pd.DataFrame) -> Tuple[Set[pd.Timestamp], Dict[str, Any], Dict[str, Set[pd.Timestamp]]]:
    """Compute 2.5-sigma candidates per tag.

    PERFORMANCE FIX: Shutdown detection disabled - caused false positives and messy charts.
    Now analyzes all data without filtering shutdown/startup periods.

    Returns (candidate_times, summary_by_tag, times_by_tag)
    where times_by_tag maps each tag to the set of candidate timestamps.
    """
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return set(), {}, {}

    # Compute per-tag mean and std
    try:
        grouped = df[['tag', 'value', 'time']].dropna(subset=['value'])
        # Ensure datetime for accurate set operations later
        grouped = _ensure_datetime(grouped)

        # SHUTDOWN DETECTION DISABLED - was causing false positives
        # Use all data for cleaner, more accurate detection
        grouped_running = grouped.copy()

        # Using groupby to compute mean/std ON ALL DATA
        stats = grouped_running.groupby('tag')['value'].agg(['mean', 'std'])
        stats = stats.replace({np.nan: 0.0})

        # Join to compute z-scores efficiently
        joined = grouped_running.join(stats, on='tag', how='left')
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


def _calculate_time_weighted_score(anomaly_times: Set[pd.Timestamp], half_life_days: float = 7.0) -> float:
    """Calculate time-weighted anomaly score with exponential decay.

    Recent anomalies get higher weight than older ones using exponential decay.

    Args:
        anomaly_times: Set of timestamps when anomalies occurred
        half_life_days: Number of days for weight to decay to 50% (default 7 days)

    Returns:
        Weighted anomaly score (higher = more recent/severe)
    """
    if not anomaly_times:
        return 0.0

    now = pd.Timestamp.now().tz_localize(None)
    decay_constant = np.log(2) / half_life_days

    weighted_score = 0.0
    for timestamp in anomaly_times:
        # Ensure timestamp is timezone-naive for comparison
        if isinstance(timestamp, pd.Timestamp) and timestamp.tz is not None:
            timestamp = timestamp.tz_convert(None)

        # Calculate age in days
        age_days = (now - timestamp).total_seconds() / (24 * 3600)

        # Exponential decay: weight = e^(-lambda * age)
        # Recent: age=0 days → weight=1.0 (100%)
        # 7 days: age=7 days → weight=0.5 (50%)
        # 14 days: age=14 days → weight=0.25 (25%)
        # 30 days: age=30 days → weight=0.089 (9%)
        weight = np.exp(-decay_constant * max(0, age_days))
        weighted_score += weight

    return float(weighted_score)


def _calculate_recency_breakdown(anomaly_times: Set[pd.Timestamp]) -> Dict[str, int]:
    """Break down anomalies by time period for better visibility.

    Args:
        anomaly_times: Set of timestamps when anomalies occurred

    Returns:
        Dictionary with counts for different time periods
    """
    if not anomaly_times:
        return {'last_24h': 0, 'last_7d': 0, 'last_30d': 0, 'older': 0}

    now = pd.Timestamp.now().tz_localize(None)
    breakdown = {'last_24h': 0, 'last_7d': 0, 'last_30d': 0, 'older': 0}

    for timestamp in anomaly_times:
        if isinstance(timestamp, pd.Timestamp) and timestamp.tz is not None:
            timestamp = timestamp.tz_convert(None)

        age = (now - timestamp).total_seconds() / 3600  # Age in hours

        if age <= 24:
            breakdown['last_24h'] += 1
        elif age <= 24 * 7:
            breakdown['last_7d'] += 1
        elif age <= 24 * 30:
            breakdown['last_30d'] += 1
        else:
            breakdown['older'] += 1

    return breakdown


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
    sigma_by_tag: Dict[str, Dict[str, Any]] = None,
    ae_total: int = 0,
) -> Tuple[Dict[str, Any], int, Dict[str, Dict[str, Set[pd.Timestamp]]]]:
    """Verify candidate (tag, time) anomalies using MTD-like thresholds and Isolation Forest.

    - MTD-like: per-tag threshold using baseline upper/lower if available, else 2.5-sigma
    - IF: per-tag univariate Isolation Forest on the tag's full series; a candidate point is
      confirmed if IF marks it as an outlier (-1)

    Returns (by_tag_verified, total_verified)
    """
    if df.empty or 'tag' not in df.columns or 'value' not in df.columns:
        return {}, 0, {}

    # Handle defaults for primary detection data
    if sigma_by_tag is None:
        sigma_by_tag = {}

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
            # Get primary detector counts for this tag from sigma_by_tag
            sigma_count = sigma_by_tag.get(tag, {}).get('count', 0)
            tag_candidate_count = int(cand_rows.shape[0])
            ae_count = int(ae_total * (tag_candidate_count / max(1, len(candidate_times)))) if ae_total > 0 else 0

            by_tag_verified[tag] = {
                'count': 0,
                'rate': 0.0,
                'method': 'hybrid_verified',
                'confidence': 'LOW',
                # Primary detector counts (for anomaly-triggered plotting)
                'sigma_2_5_count': int(sigma_count),
                'autoencoder_count': int(ae_count),
                # Verification detector counts (for anomaly-triggered plotting)
                'mtd_count': len(mtd_confirmed_times),
                'isolation_forest_count': len(if_confirmed_times),
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
        # Get primary detector counts for this tag from sigma_by_tag and ae_times
        sigma_count = sigma_by_tag.get(tag, {}).get('count', 0)
        # AE count is global, so estimate based on tag's share of candidate times
        tag_candidate_count = int(cand_rows.shape[0])
        ae_count = int(ae_total * (tag_candidate_count / max(1, len(candidate_times)))) if ae_total > 0 else 0

        # TIME-WEIGHTED SCORING: Recent anomalies get higher priority
        weighted_score = _calculate_time_weighted_score(confirmed_times, half_life_days=7.0)
        recency_breakdown = _calculate_recency_breakdown(confirmed_times)

        # Calculate priority level based on weighted score and recency
        # High priority: Recent anomalies (last 24h) or high weighted score
        if recency_breakdown['last_24h'] > 0:
            priority = 'CRITICAL'
        elif recency_breakdown['last_7d'] > 5 or weighted_score > 10:
            priority = 'HIGH'
        elif recency_breakdown['last_30d'] > 10 or weighted_score > 5:
            priority = 'MEDIUM'
        else:
            priority = 'LOW'

        # WEIGHTED CONFIDENCE SCORING (0-100 scale)
        # Provides granular confidence metric for anomaly detection quality
        confidence_score = 0.0

        # PRIMARY DETECTORS: 70 points total (cast wide net)
        # 2.5-Sigma: Statistical outlier detection (max 40 points)
        if sigma_count > 0:
            # Scale based on detection count, cap at 40
            sigma_contribution = min(40.0, sigma_count * 4.0)
            confidence_score += sigma_contribution

        # AutoEncoder: Pattern-based anomaly detection (max 30 points)
        if ae_count > 0:
            # Scale based on detection count, cap at 30
            ae_contribution = min(30.0, ae_count * 3.0)
            confidence_score += ae_contribution

        # VERIFICATION LAYER: 30 points total (confirm findings)
        # MTD: Threshold-based verification (max 20 points)
        mtd_count_val = len(mtd_confirmed_times)
        if mtd_count_val > 0:
            # Scale based on confirmed count, cap at 20
            mtd_contribution = min(20.0, mtd_count_val * 2.0)
            confidence_score += mtd_contribution

        # Isolation Forest: Distribution-based verification (max 10 points)
        if_count_val = len(if_confirmed_times)
        if if_count_val > 0:
            # Scale based on confirmed count, cap at 10
            if_contribution = min(10.0, if_count_val * 1.0)
            confidence_score += if_contribution

        # Map numerical score to categorical levels (backward compatibility)
        if confidence_score >= 80:
            confidence_level = 'VERY_HIGH'
        elif confidence_score >= 60:
            confidence_level = 'HIGH'
        elif confidence_score >= 40:
            confidence_level = 'MEDIUM'
        else:
            confidence_level = 'LOW'

        by_tag_verified[tag] = {
            'count': int(confirmed_count),
            'rate': float(confirmed_count) / float(len(tag_df) or 1),
            'method': 'hybrid_verified',
            'confidence': confidence_level,  # Categorical (backward compatible)
            'confidence_score': float(confidence_score),  # NEW: Numerical (0-100)
            'confidence_breakdown': {  # NEW: Detailed scoring breakdown
                'sigma_contribution': min(40.0, sigma_count * 4.0) if sigma_count > 0 else 0.0,
                'ae_contribution': min(30.0, ae_count * 3.0) if ae_count > 0 else 0.0,
                'mtd_contribution': min(20.0, mtd_count_val * 2.0) if mtd_count_val > 0 else 0.0,
                'if_contribution': min(10.0, if_count_val * 1.0) if if_count_val > 0 else 0.0,
                'total': float(confidence_score)
            },
            # Primary detector counts (for anomaly-triggered plotting)
            'sigma_2_5_count': int(sigma_count),
            'autoencoder_count': int(ae_count),
            # Verification detector counts (for anomaly-triggered plotting)
            'mtd_count': len(mtd_confirmed_times),
            'isolation_forest_count': len(if_confirmed_times),
            'verification_breakdown': {
                'mtd_confirmed': len(mtd_confirmed_times),
                'if_confirmed': len(if_confirmed_times),
                'candidates': int(cand_rows.shape[0])
            },
            # TIME-WEIGHTED SCORING: Prioritize recent events
            'weighted_score': float(weighted_score),
            'recency_breakdown': recency_breakdown,
            'priority': priority
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
        # Normalize tag column to string to avoid downstream 'float' or None tag ids
        if 'tag' in df.columns:
            try:
                df = df.copy()
                df['tag'] = df['tag'].astype(str)
            except Exception:
                pass
        total_records = int(len(df))
        unique_tags = int(len(df['tag'].unique())) if 'tag' in df.columns else 0

        # PERFORMANCE FIX: Intelligent sampling for very large datasets (>1M records)
        # This prevents multi-hour hangs on units like C-13001 with 2.2M records
        sampled = False
        sample_rate = 1.0
        if total_records > 1_000_000:
            # Sample to 500K records max (preserves statistical validity while improving performance)
            sample_rate = min(1.0, 500_000 / total_records)
            df_sampled = df.sample(frac=sample_rate, random_state=42)
            logger.info(f"Large dataset detected ({total_records:,} records) - sampling {sample_rate:.1%} ({len(df_sampled):,} records) for performance")
            df_original = df  # Keep reference to original for metadata
            df = df_sampled
            sampled = True

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
            df, candidate_times, baseline_thresholds, sigma_by_tag, ae_total
        )

        # Aggregate results
        anomaly_rate_global = (float(total_verified) / float(total_records)) if total_records > 0 else 0.0
        result = {
            'method': 'hybrid_sigma2p5_ae_mtd_if' + ('_sampled' if sampled else ''),
            'total_anomalies': int(total_verified),
            'anomaly_rate': float(anomaly_rate_global),
            'by_tag': by_tag_verified,
            'config_loaded': bool(baseline_thresholds),
            'tags_analyzed': unique_tags,
            'primary_candidates': {
                'sigma_2p5_total': int(sigma_total),
                'ae_total': int(ae_total),
                'unique_candidate_timestamps': int(len(candidate_times)),
            },
            'performance': {
                'total_records': total_records,
                'sampled': sampled,
                'sample_rate': sample_rate,
                'records_analyzed': len(df)
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
