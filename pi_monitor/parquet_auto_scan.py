"""
Auto-scan functionality using existing Parquet files for TURBOPREDICT X PROTEAN
Works with real data in the data directory
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
import json

from .parquet_database import ParquetDatabase
from .config import Config
from .excel_refresh import refresh_excel_safe
from .breakout import detect_breakouts
from .batch import build_unit_from_tags
from .clean import dedup_parquet

logger = logging.getLogger(__name__)


class ParquetAutoScanner:
    """Auto-scanner using existing Parquet files"""
    
    def __init__(self, config: Config = None, data_dir: Path = None):
        """Initialize auto-scanner for Parquet files.
        
        Args:
            config: Configuration object
            data_dir: Path to data directory
        """
        self.config = config or Config()
        
        if data_dir is None:
            # Default to data directory relative to current location
            current_dir = Path(__file__).parent.parent
            data_dir = current_dir / "data"
        
        self.db = ParquetDatabase(data_dir)
        
    def scan_all_units(self, max_age_hours: float = 1.0, force_refresh: bool = False) -> Dict[str, Any]:
        """Scan all available units and check data freshness.
        
        Args:
            max_age_hours: Maximum data age before considering stale
            force_refresh: Force refresh even if data is fresh
            
        Returns:
            Dictionary with scan results
        """
        results = {
            'scan_timestamp': datetime.now().isoformat(),
            'max_age_hours': max_age_hours,
            'force_refresh': force_refresh,
            'units_scanned': [],
            'fresh_units': [],
            'stale_units': [],
            'empty_units': [],
            'total_records': 0,
            'total_size_mb': 0
        }
        
        # Proactively archive any stray Parquet files not prefixed by a unit id
        try:
            archived = self.db.archive_non_unit_parquet()
            if archived:
                logger.info(f"Archived {len(archived)} non-unit Parquet file(s) to 'archive/'")
        except Exception:
            pass

        # Get all available units
        units = self.db.get_all_units()
        logger.info(f"Found {len(units)} units with data")
        
        for unit in units:
            try:
                # Get freshness info
                info = self.db.get_data_freshness_info(unit)
                
                unit_result = {
                    'unit': unit,
                    'total_records': info['total_records'],
                    'unique_tags': len(info['unique_tags']),
                    'latest_timestamp': info['latest_timestamp'],
                    'data_age_hours': info['data_age_hours'],
                    'is_stale': info['is_stale'],
                    'date_range_days': info['date_range_days']
                }
                
                results['units_scanned'].append(unit_result)
                results['total_records'] += info['total_records']
                
                # Categorize units
                if info['total_records'] == 0:
                    results['empty_units'].append(unit)
                elif force_refresh or (info['data_age_hours'] and info['data_age_hours'] > max_age_hours):
                    results['stale_units'].append(unit)
                else:
                    results['fresh_units'].append(unit)
                    
            except Exception as e:
                logger.error(f"Error scanning unit {unit}: {e}")
        
        # Calculate summary statistics
        results['summary'] = {
            'total_units': len(units),
            'fresh_units': len(results['fresh_units']),
            'stale_units': len(results['stale_units']),
            'empty_units': len(results['empty_units']),
            'freshness_rate': len(results['fresh_units']) / len(units) if units else 0,
            'total_records': results['total_records'],
            'units_needing_refresh': len(results['stale_units']) + len(results['empty_units'])
        }
        
        logger.info(f"Scan complete: {results['summary']['fresh_units']} fresh, "
                   f"{results['summary']['stale_units']} stale, "
                   f"{results['summary']['empty_units']} empty units")
        
        return results

    # --- Auto-build support for missing units ---------------------------------
    def _discover_configured_units(self) -> list[dict[str, Any]]:
        """Discover units from tag files in the config directory.

        Returns list of dicts: { 'unit': str, 'plant': str, 'tags_file': Path }
        """
        configured: list[dict[str, Any]] = []
        try:
            config_dir = Path(__file__).parent.parent / "config"
            if not config_dir.exists():
                return configured

            for tags_file in sorted(config_dir.glob("tags_*.txt")):
                # Read first non-empty, non-comment line to infer unit/plant
                first_line = None
                try:
                    for raw in tags_file.read_text(encoding="utf-8").splitlines():
                        s = raw.strip()
                        if s and not s.startswith("#"):
                            first_line = s
                            break
                except Exception:
                    first_line = None

                unit = None
                plant = None

                # Infer from first tag line if available
                if first_line:
                    # Try to find patterns like 'K-31-01', 'C-02001', 'C-104', 'C-13001', 'XT-07002'
                    import re
                    m = re.search(r"\b([A-Z]{1,4}-\d{2,5}(?:-\d{2})?)\b", first_line)
                    if m:
                        unit = m.group(1)
                    # Plant hint: token before first '.' or '_' often the plant code (e.g. PCFS, PCM)
                    m2 = re.match(r"^([A-Za-z]+)[\._]", first_line)
                    if m2:
                        plant_code = m2.group(1).upper()
                        # Map known aliases
                        plant = {
                            "PCM": "PCMSB",
                        }.get(plant_code, plant_code)

                # Fallbacks from filename
                name = tags_file.stem.lower()
                if unit is None:
                    if "k12_01" in name:
                        unit = "K-12-01"
                    elif "k16_01" in name:
                        unit = "K-16-01"
                    elif "k19_01" in name:
                        unit = "K-19-01"
                    elif "k31_01" in name:
                        unit = "K-31-01"
                    elif "c02001" in name or "c-02001" in name:
                        unit = "C-02001"
                    elif "c104" in name or "c-104" in name:
                        unit = "C-104"
                    elif "c13001" in name or "c-13001" in name:
                        unit = "C-13001"
                    elif "c1301" in name or "c-1301" in name:
                        unit = "C-1301"
                    elif "c1302" in name or "c-1302" in name:
                        unit = "C-1302"
                    elif "c201" in name or "c-201" in name:
                        unit = "C-201"
                    elif "c202" in name or "c-202" in name:
                        unit = "C-202"
                    elif ("abf" in name) and ("07" in name) and ("mt01" in name) and ("k001" in name):
                        unit = "07-MT01-K001"
                    elif ("abf" in name) and ("07" in name) and ("mt001" in name) and ("k001" in name):
                        unit = "07-MT01-K001"
                    elif "xt07002" in name or "xt-07002" in name or "xt_07002" in name:
                        unit = "XT-07002"

                if plant is None:
                    # Heuristic: PCMSB for files mentioning pcmsb/pcm, else PCFS
                    if ("abf" in name):
                        plant = "ABF"
                    else:
                        plant = "PCMSB" if ("pcmsb" in name or "pcm" in name) else "PCFS"

                if unit:
                    configured.append({
                        "unit": unit,
                        "plant": plant,
                        "tags_file": tags_file,
                    })
        except Exception:
            # Fail-soft: no configured units discovered
            pass
        return configured

    def _auto_build_missing_units(self, xlsx_path: Path) -> list[dict[str, Any]]:
        """Build Parquet files for any configured units missing in processed/.

        Returns list of build result dicts.
        """
        results: list[dict[str, Any]] = []
        configured = self._discover_configured_units()
        if not configured:
            return results

        # Normalize aliases to canonical names to avoid duplicate (alias vs canonical) builds
        alias_map = {
            '07-MT01/K001': '07-MT01-K001',
            '07-MT001/K001': '07-MT01-K001',
            'FI-07001': '07-MT01-K001',
        }
        def _canon(u: str) -> str:
            return alias_map.get(u, u)

        existing_units = set(_canon(u) for u in self.db.get_all_units())
        for item in configured:
            item["unit"] = _canon(item.get("unit", ""))
        to_build = [c for c in configured if c["unit"] not in existing_units]
        if not to_build:
            return results

        print(f"Discovered {len(to_build)} configured unit(s) with no Parquet. Seeding...")
        for item in to_build:
            unit = item["unit"]
            plant = item["plant"]
            tags_file: Path = Path(item["tags_file"])  # type: ignore[assignment]
            try:
                import re as _re
                safe_unit = _re.sub(r"[^A-Za-z0-9._-]", "_", str(unit))
                out_parquet = self.db.processed_dir / f"{safe_unit}_1y_0p1h.parquet"
                out_parquet.parent.mkdir(parents=True, exist_ok=True)
                # Prefer plant-specific workbook if available (e.g., PCMSB)
                preferred_wb = xlsx_path
                try:
                    if str(plant).upper().startswith("PCMSB"):
                        pcmsb_wb = Path("excel/PCMSB_Automation.xlsx")
                        if pcmsb_wb.exists():
                            preferred_wb = pcmsb_wb
                    if str(plant).upper().startswith("ABF"):
                        abf_wb = Path("excel/ABF_Automation.xlsx")
                        if abf_wb.exists():
                            preferred_wb = abf_wb
                except Exception:
                    pass
                print(f"  - Building {plant} {unit} from {tags_file.name} using {Path(preferred_wb).name} -> {out_parquet.name}")
                build_unit_from_tags(
                    xlsx=preferred_wb,
                    tags=[t.strip() for t in tags_file.read_text(encoding="utf-8").splitlines() if t.strip() and not t.strip().startswith('#')],
                    out_parquet=out_parquet,
                    plant=plant,
                    unit=unit,
                    # use defaults: server/start/end/step/sheet/settle_seconds/visible
                )
                # Dedup
                dedup_path = dedup_parquet(out_parquet)
                size_mb = dedup_path.stat().st_size / (1024 * 1024)
                print(f"    Seeded {unit}: {len(pd.read_parquet(dedup_path)):,} rows, {size_mb:.1f}MB")
                results.append({
                    "unit": unit,
                    "plant": plant,
                    "parquet": str(dedup_path),
                    "size_mb": size_mb,
                    "success": True,
                })
            except Exception as e:
                print(f"    Failed to seed {unit}: {e}")
                results.append({
                    "unit": unit,
                    "plant": plant,
                    "parquet": None,
                    "size_mb": None,
                    "success": False,
                    "error": str(e),
                })
        # Refresh DB view of processed files
        self.db = ParquetDatabase(self.db.data_dir)
        return results

    def analyze_unit_data(self, unit: str, run_anomaly_detection: bool = True) -> Dict[str, Any]:
        """Analyze data for a specific unit.
        
        Args:
            unit: Unit identifier
            run_anomaly_detection: Whether to run anomaly detection
            
        Returns:
            Analysis results
        """
        logger.info(f"Analyzing unit: {unit}")
        
        # Get unit data
        df = self.db.get_unit_data(unit)
        
        if df.empty:
            return {
                'unit': unit,
                'status': 'no_data',
                'records': 0,
                'tags': [],
                'analysis_timestamp': datetime.now().isoformat()
            }
        
        # Basic statistics
        analysis = {
            'unit': unit,
            'status': 'success',
            'records': len(df),
            'date_range': {
                'start': df['time'].min().isoformat() if 'time' in df.columns else None,
                'end': df['time'].max().isoformat() if 'time' in df.columns else None,
            },
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Tag analysis
        if 'tag' in df.columns:
            tag_summary = self.db.get_tag_summary(unit)
            analysis['tags'] = tag_summary.to_dict('records') if not tag_summary.empty else []
            analysis['unique_tags'] = len(df['tag'].unique())
        else:
            analysis['tags'] = []
            analysis['unique_tags'] = 0
        
        # Value statistics
        if 'value' in df.columns:
            analysis['value_stats'] = {
                'count': int(df['value'].count()),
                'mean': float(df['value'].mean()) if df['value'].count() > 0 else None,
                'std': float(df['value'].std()) if df['value'].count() > 0 else None,
                'min': float(df['value'].min()) if df['value'].count() > 0 else None,
                'max': float(df['value'].max()) if df['value'].count() > 0 else None,
                'null_count': int(df['value'].isnull().sum())
            }
        
        # Enhanced anomaly detection with baseline tuning
        if run_anomaly_detection and 'value' in df.columns and 'tag' in df.columns:
            analysis['anomalies'] = self._detect_anomalies_enhanced(df, unit)
        
        return analysis
    
    def _detect_anomalies_enhanced(self, df: pd.DataFrame, unit: str) -> Dict[str, Any]:
        """Enhanced anomaly detection with baseline tuning and unit status awareness"""
        try:
            # Try to use smart detection with unit status checking first
            from .smart_anomaly_detection import smart_anomaly_detection
            
            results = smart_anomaly_detection(df, unit)
            
            # Convert to expected format with unit status awareness
            if results:
                unit_status = results.get('unit_status', {})
                analysis_performed = results.get('anomaly_analysis_performed', True)
                
                base_result = {
                    'total_anomalies': results.get('total_anomalies', 0),
                    'anomaly_rate': results.get('anomaly_rate', 0.0),
                    'by_tag': results.get('by_tag', {}),
                    'method': results.get('method', 'smart_enhanced'),
                    'baseline_calibrated': results.get('config_loaded', False),
                    'tags_analyzed': results.get('tags_analyzed', 0),
                    'unit_status': unit_status.get('status', 'UNKNOWN'),
                    'unit_message': unit_status.get('message', ''),
                    'analysis_performed': analysis_performed
                }
                
                # If tuned path is conservative fallback (no baseline), use robust MTD for better sensitivity
                if base_result['method'] == 'conservative_fallback' and not base_result['baseline_calibrated']:
                    return self._detect_simple_anomalies(df, unit)

                # If tuned result looks suspiciously high (likely mis-calibrated), fallback to robust MTD/IF
                # DISABLED: Allow enhanced detection with higher anomaly rates for better sensitivity
                # try:
                #     if base_result.get('anomaly_rate', 0) > 0.01:
                #         logger.warning("Tuned anomaly rate > 1%; using robust MTD/IF fallback")
                #         return self._detect_simple_anomalies(df, unit)
                # except Exception:
                #     pass

                # Generate appropriate detection summary
                if not analysis_performed:
                    base_result['detection_summary'] = f"UNIT OFFLINE: {unit_status.get('message', 'Unit not running')}"
                elif results.get('total_anomalies', 0) > 0:
                    status_note = f" ({unit_status.get('status', 'RUNNING')})" if unit_status.get('status') != 'RUNNING' else ""
                    base_result['detection_summary'] = f"Smart detection: {results['total_anomalies']} anomalies found ({results['anomaly_rate']*100:.2f}%){status_note}"
                else:
                    base_result['detection_summary'] = f"No anomalies detected - Unit {unit_status.get('status', 'RUNNING')}"
                return base_result
            else:
                # Fallback result
                return {
                    'total_anomalies': 0,
                    'anomaly_rate': 0.0,
                    'by_tag': {},
                    'method': 'smart_enhanced_fallback',
                    'baseline_calibrated': False,
                    'detection_summary': "Smart detection fallback - no anomalies detected"
                }
                
        except ImportError:
            # Fall back to original detection if tuned module not available
            logger.warning("Tuned anomaly detection module not available, using original method")
            return self._detect_simple_anomalies(df, unit)
        except Exception as e:
            # Fall back to original detection on any error
            logger.warning(f"Enhanced anomaly detection failed: {e}, using original method")
            return self._detect_simple_anomalies(df)
    
    def _detect_simple_anomalies(self, df: pd.DataFrame, unit_hint: Optional[str] = None) -> Dict[str, Any]:
        """Mahalanobis-Taguchi Distance anomaly detection for turbomachinery, focused on last 7 days.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            Anomaly detection results using MTD
        """
        anomalies = {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'by_tag': {},
            'analysis_period': 'last_7_days',
            'date_range': None,
            'method': 'mahalanobis_taguchi_distance',
            'speed_dominant': True
        }
        
        try:
            # Filter to recent data for anomaly detection with fallback strategy
            if 'time' in df.columns and not df.empty:
                now = datetime.now()
                
                # Convert time column to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(df['time']):
                    df['time'] = pd.to_datetime(df['time'])
                
                # Try 7 days first
                seven_days_ago = now - timedelta(days=7)
                recent_df = df[df['time'] >= seven_days_ago].copy()
                
                # If 7-day data insufficient, fallback to 1 year
                if recent_df.empty or len(recent_df) < 100:
                    anomalies['analysis_period'] = 'fallback_1_year'
                    one_year_ago = now - timedelta(days=365)
                    recent_df = df[df['time'] >= one_year_ago].copy()
                    
                    if recent_df.empty:
                        anomalies['error'] = 'No data available in last year'
                        return anomalies
                    
                    print(f"MTD: Using 1-year fallback data ({len(recent_df):,} records)")
                else:
                    anomalies['analysis_period'] = 'last_7_days'
                
                anomalies['date_range'] = {
                    'start': recent_df['time'].min().isoformat(),
                    'end': recent_df['time'].max().isoformat(),
                    'records_analyzed': len(recent_df)
                }
                
                # Use recent data for analysis
                analysis_df = recent_df
            else:
                # Fallback to full dataset if no time column
                analysis_df = df
                anomalies['analysis_period'] = 'full_dataset'
            
            # Use MTD for multivariate anomaly detection
            mtd_results = self._mahalanobis_taguchi_detection(analysis_df)
            
            # Check if MTD found anomalies - if not, use Isolation Forest as fallback
            # Also trigger if anomaly rate is suspiciously high (>50%), suggesting MTD sensitivity issues
            mtd_anomalies = mtd_results.get('mtd_anomalies', 0)
            mtd_rate = mtd_results.get('anomaly_rate', 0)
            
            if (mtd_anomalies == 0 or mtd_rate > 0.5) and 'error' not in mtd_results:
                if mtd_anomalies == 0:
                    print("MTD detected no anomalies. Proceeding with Isolation Forest batch analysis...")
                else:
                    print(f"MTD anomaly rate too high ({mtd_rate*100:.1f}%). Using Isolation Forest for refined analysis...")
                if_results = self._isolation_forest_batch_analysis(analysis_df, unit_hint)
                
                # Merge both results
                anomalies.update(mtd_results)
                anomalies['isolation_forest'] = if_results
                anomalies['method'] = 'mtd_with_isolation_forest_fallback'
                anomalies['total_anomalies'] = if_results.get('total_anomalies', 0)
                anomalies['anomaly_rate'] = if_results.get('anomaly_rate', 0.0)
                # Combine per-tag results so plotter has signals
                combined_by_tag = {}
                try:
                    for src in (mtd_results.get('by_tag', {}), if_results.get('by_tag', {})):
                        if src:
                            for t, info in src.items():
                                if t not in combined_by_tag:
                                    combined_by_tag[t] = dict(info)
                                else:
                                    combined_by_tag[t]['count'] = combined_by_tag[t].get('count', 0) + info.get('count', 0)
                                    combined_by_tag[t]['rate'] = max(combined_by_tag[t].get('rate', 0.0), info.get('rate', 0.0))
                    if combined_by_tag:
                        anomalies['by_tag'] = combined_by_tag
                except Exception:
                    pass
            else:
                # Use MTD results
                anomalies.update(mtd_results)
            
        except Exception as e:
            logger.error(f"Error in MTD anomaly detection: {e}")
            anomalies['error'] = str(e)
        
        return anomalies
    
    def _mahalanobis_taguchi_detection(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Implement Mahalanobis-Taguchi Distance for turbomachinery anomaly detection.
        
        Speed as dominant X-axis, all other parameters as Y-axis variables.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            MTD-based anomaly results
        """
        import numpy as np
        # Use numpy instead of scipy for broader compatibility
        
        results = {
            'mtd_anomalies': 0,
            'mtd_threshold': 0,
            'speed_parameter': None,
            'multivariate_analysis': {},
            'by_tag': {}
        }
        # Ensure local unit variable exists to avoid scope/name issues
        unit_local = None

        try:
            # Identify speed parameter - prioritize known speed indicators
            speed_tags = []
            other_tags = []
            
            # Known speed indicators by unit (ISA-5.1 SI = Speed Indicator)
            known_speed_tags = {
                'K-12-01': 'PCFS_K-12-01_12SI-401B_PV',
                'K-16-01': 'PCFS_K-16-01_16SI-501B_PV',
                'K-19-01': 'PCFS_K-19-01_19SI-601B_PV',
                'K-31-01': 'PCFS_K-31-01_31KI-302_PV'
            }
            
            # Extract unit from data
            for tag in df['tag'].unique():
                for unit_code in known_speed_tags.keys():
                    if unit_code in tag:
                        unit_local = unit_code
                        break
                if unit_local:
                    break
            
            # Use known speed tag if available
            if unit_local and known_speed_tags[unit_local]:
                if known_speed_tags[unit_local] in df['tag'].unique():
                    speed_tags = [known_speed_tags[unit_local]]
                    other_tags = [tag for tag in df['tag'].unique() if tag != known_speed_tags[unit_local]]
            
            # Fallback: search by keywords
            if not speed_tags:
                for tag in df['tag'].unique():
                    if any(keyword in tag.upper() for keyword in ['SI-', 'SPEED', 'RPM', 'FREQ', 'ROTATION']):
                        speed_tags.append(tag)
                    else:
                        other_tags.append(tag)
            
            # Last resort: use first available tag
            if not speed_tags:
                all_tags = df['tag'].unique()
                if len(all_tags) > 0:
                    speed_tags = [all_tags[0]]
                    other_tags = all_tags[1:].tolist()
            
            if not speed_tags:
                results['error'] = 'No suitable speed parameter found'
                return results
            
            # Use primary speed tag
            primary_speed_tag = speed_tags[0]
            results['speed_parameter'] = primary_speed_tag
            
            # Load optional MTD config for this unit
            # Defaults
            mtd_cfg = {
                'resample': 'h',
                'baseline_fraction': 0.7,
                'threshold_quantile': 0.995,
                'support_fraction': 0.75,
                'max_features': 20
            }
            try:
                # Derive unit code robustly from available tags
                unit_candidates = []
                for uc in ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']:
                    if any(uc in str(t) for t in df['tag'].unique()):
                        unit_candidates.append(uc)
                unit_code = unit_candidates[0] if unit_candidates else None
                # Try baseline config file first
                if unit_code:
                    base_cfg_path = Path(f"baseline_config_{unit_code}.json")
                    if base_cfg_path.exists():
                        with open(base_cfg_path, 'r', encoding='utf-8') as f:
                            base_cfg = json.load(f)
                        if isinstance(base_cfg, dict) and 'mtd_config' in base_cfg:
                            mtd_cfg.update({k: base_cfg['mtd_config'][k] for k in mtd_cfg.keys() if k in base_cfg['mtd_config']})
                    # Then try standalone mtd config
                    standalone_path = Path(f"mtd_config_{unit_code}.json")
                    if standalone_path.exists():
                        with open(standalone_path, 'r', encoding='utf-8') as f:
                            stand_cfg = json.load(f)
                        if isinstance(stand_cfg, dict):
                            mtd_cfg.update({k: stand_cfg[k] for k in mtd_cfg.keys() if k in stand_cfg})
            except Exception:
                pass
            
            # Create multivariate dataset with speed as dominant axis
            # Use time-based resampling to handle sparse data
            df_time_indexed = df.set_index('time')
            
            # Resample to create aligned dataset (configurable)
            resample_rule = mtd_cfg.get('resample', 'h') or 'h'
            pivot_df = df_time_indexed.groupby(['tag']).resample(resample_rule)['value'].mean().unstack(level=0).reset_index()
            pivot_df.columns.name = None  # Remove multi-level column name
            
            # Ensure we have the speed column and other parameters
            available_tags = [col for col in pivot_df.columns if col != 'time']
            
            if primary_speed_tag not in available_tags:
                results['error'] = f'Speed parameter {primary_speed_tag} not found in resampled data'
                return results
            
            # Select top correlated features to reduce dimensionality
            # Start with speed + all other parameters (we'll prune later)
            feature_cols = [primary_speed_tag]
            other_features = [tag for tag in available_tags if tag != primary_speed_tag]
            
            # Add other features (candidates)
            cfg_max_feat = int(mtd_cfg.get('max_features', 20) or 20)
            feature_cols.extend(other_features)
            
            if len(feature_cols) < 2:
                results['error'] = 'Insufficient multivariate features for MTD analysis'
                return results
            
            # Create feature matrix with forward fill for missing values
            X = pivot_df[feature_cols].ffill().dropna()
            
            if len(X) < 50:
                results['error'] = f'Insufficient data points for MTD analysis: {len(X)} points (need â‰¥50)'
                return results
            
            # Prefer speed-windowed baseline within recent window if configured
            X_baseline = None
            X_test = None
            try:
                baseline_days = int(mtd_cfg.get('baseline_days', 30) or 30)
            except Exception:
                baseline_days = 30
            try:
                speed_window = mtd_cfg.get('speed_window_rpm', None)
            except Exception:
                speed_window = None
            auto_bw = bool(mtd_cfg.get('auto_speed_window', False))
            window_pct = float(mtd_cfg.get('window_pct', 0.03) or 0.03)
            min_window_rpm = float(mtd_cfg.get('min_window_rpm', 100) or 100)

            # Build recent slice for baseline/test
            pivot_df['time'] = pd.to_datetime(pivot_df['time'], errors='coerce')
            if baseline_days and len(pivot_df) > 0:
                cutoff = pivot_df['time'].max() - pd.Timedelta(days=baseline_days)
                recent_mask = pivot_df['time'] >= cutoff
            else:
                recent_mask = pd.Series([True] * len(pivot_df))

            # Auto speed-window discovery
            if (not speed_window) and auto_bw and primary_speed_tag in pivot_df.columns:
                try:
                    speed_series = pivot_df.loc[recent_mask, primary_speed_tag].dropna()
                    if len(speed_series) >= 50:
                        binned = (speed_series/10.0).round().astype(int)
                        mode_bin = binned.value_counts().idxmax()
                        center = float(mode_bin * 10)
                        half_width = max(min_window_rpm, center * window_pct)
                        speed_window = [center - half_width, center + half_width]
                except Exception:
                    speed_window = None

            if speed_window and primary_speed_tag in pivot_df.columns:
                lo, hi = float(speed_window[0]), float(speed_window[1])
                base_mask = recent_mask & pivot_df[primary_speed_tag].between(lo, hi, inclusive='both')
                Xb_raw = pivot_df.loc[base_mask, feature_cols].ffill().dropna()
                Xt_raw = pivot_df.loc[recent_mask, feature_cols].ffill().dropna()
                if len(Xb_raw) >= 50 and len(Xt_raw) >= 1:
                    X_baseline = Xb_raw.copy()
                    X_test = Xt_raw.copy()

            # Fallback to time-split if no valid speed-window baseline
            if X_baseline is None or X_test is None:
                baseline_fraction = float(mtd_cfg.get('baseline_fraction', 0.7) or 0.7)
                baseline_fraction = min(0.9, max(0.5, baseline_fraction))
                baseline_size = int(len(X) * baseline_fraction)
                X_baseline = X.iloc[:baseline_size].copy()
                X_test = X.iloc[baseline_size:].copy()

            # Standardize features using baseline mean/std (z-score)
            # Drop near-constant features to avoid numerical issues
            eps = 1e-8
            mu = X_baseline.mean()
            sigma = X_baseline.std(ddof=0).replace(0, np.nan)
            keep_cols_all = sigma[sigma > eps].index.tolist()
            if len(keep_cols_all) < 2:
                results['error'] = 'Insufficient informative features after standardization'
                return results

            # Correlation-pruned, variance-ranked selection
            selected = []
            if primary_speed_tag in keep_cols_all:
                selected.append(primary_speed_tag)
            variances = sigma[keep_cols_all].sort_values(ascending=False)
            for col in variances.index:
                if col == primary_speed_tag:
                    continue
                if len(selected) >= cfg_max_feat:
                    break
                try:
                    ok = True
                    for s in selected:
                        corr = float(X_baseline[[col, s]].corr().iloc[0,1])
                        if abs(corr) >= 0.95:
                            ok = False
                            break
                    if ok:
                        selected.append(col)
                except Exception:
                    selected.append(col)
            keep_cols = selected if len(selected) >= 2 else keep_cols_all[:cfg_max_feat]

            Xb = ((X_baseline[keep_cols] - mu[keep_cols]) / sigma[keep_cols]).dropna()
            Xt = ((X_test[keep_cols] - mu[keep_cols]) / sigma[keep_cols]).dropna()
            if len(Xb) < 50 or len(Xt) < 1:
                results['error'] = f'Insufficient standardized data for MTD analysis: baseline={len(Xb)}, test={len(Xt)}'
                return results

            # Robust covariance estimation (MinCovDet), fallback to LedoitWolf, then np.cov
            cov_inv = None
            robust_mean = Xb.mean().values
            try:
                from sklearn.covariance import MinCovDet
                import warnings as _warnings
                support_fraction = float(mtd_cfg.get('support_fraction', 0.75) or 0.75)
                support_fraction = min(0.95, max(0.5, support_fraction))
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore", category=RuntimeWarning)
                    mcd = MinCovDet(support_fraction=support_fraction, random_state=42).fit(Xb.values)
                robust_mean = mcd.location_
                cov = mcd.covariance_
                cov_inv = np.linalg.pinv(cov)
            except Exception:
                try:
                    from sklearn.covariance import LedoitWolf
                    lw = LedoitWolf().fit(Xb.values)
                    cov = lw.covariance_
                    cov_inv = np.linalg.pinv(cov)
                except Exception:
                    try:
                        cov = np.cov(Xb.T)
                        cov_inv = np.linalg.pinv(cov)
                    except Exception:
                        results['error'] = 'Covariance matrix estimation/inversion failed'
                        return results

            # Mahalanobis distance helper
            def mahalanobis_distance(point, mean, inv):
                diff = point - mean
                return float(np.sqrt(np.dot(np.dot(diff, inv), diff)))

            # Compute baseline distances to set an adaptive threshold (e.g., 99.5th percentile)
            baseline_d = [mahalanobis_distance(row.values, robust_mean, cov_inv) for _, row in Xb.iterrows()]
            if len(baseline_d) < 10:
                results['error'] = 'Insufficient baseline distances for thresholding'
                return results
            mtd_q = float(mtd_cfg.get('threshold_quantile', 0.995) or 0.995)
            mtd_q = min(0.9999, max(0.9, mtd_q))
            mtd_threshold = float(np.quantile(baseline_d, mtd_q))  # robust high-quantile threshold
            if not np.isfinite(mtd_threshold) or mtd_threshold <= 0:
                mtd_threshold = 3.0  # fallback

            # Calculate distances for test data
            mahal_distances = []
            anomaly_indices = []
            for idx, row in Xt.iterrows():
                try:
                    d = mahalanobis_distance(row.values, robust_mean, cov_inv)
                    mahal_distances.append(d)
                    if d > mtd_threshold:
                        anomaly_indices.append(idx)
                except Exception:
                    continue

            results['mtd_threshold'] = mtd_threshold
            results['mtd_anomalies'] = len(anomaly_indices)
            results['total_test_points'] = len(Xt)
            results['anomaly_rate'] = len(anomaly_indices) / len(Xt) if len(Xt) > 0 else 0
            
            # Detailed analysis by parameter
            results['multivariate_analysis'] = {
                'speed_parameter': primary_speed_tag,
                'feature_count': len(keep_cols),
                'features_analyzed': keep_cols,
                'baseline_size': len(Xb),
                'test_size': len(Xt),
                'mean_mahal_distance': float(np.mean(mahal_distances)) if mahal_distances else 0.0,
                'max_mahal_distance': float(np.max(mahal_distances)) if mahal_distances else 0.0,
                'threshold_quantile': mtd_q,
                'baseline_distance_mean': float(np.mean(baseline_d)) if baseline_d else 0.0,
                'baseline_distance_p995': mtd_threshold,
                'baseline_stats': {
                    param: {
                        'mean': float(mu[param]) if param in mu else None,
                        'std': float(sigma[param]) if param in sigma else None
                    } for param in keep_cols
                }
            }
            
            # Per-tag contribution analysis for MTD (no IQR). Count tags with |z|>z_threshold and
            # optionally those outside density bands derived from the baseline at stable speed.
            try:
                contrib_counts: Dict[str, int] = {}
                z_threshold = float(mtd_cfg.get('z_threshold', 3.0) or 3.0)
                if anomaly_indices:
                    # Xt is standardized on keep_cols
                    Xt_anom = Xt.loc[anomaly_indices, keep_cols]
                    # Count per-feature exceedances in standardized space
                    exceed = (Xt_anom.abs() > z_threshold)
                    counts = exceed.sum(axis=0)
                    for col, cnt in counts.items():
                        if int(cnt) > 0:
                            contrib_counts[col] = contrib_counts.get(col, 0) + int(cnt)

                # Add out-of-band counts using raw baseline bands if available
                try:
                    band_p = mtd_cfg.get('band_percentiles', [5, 95])
                    p_low = float(band_p[0])
                    p_high = float(band_p[1])
                except Exception:
                    p_low, p_high = 5.0, 95.0

                if 'Xb_raw' in locals() and 'Xt_raw' in locals() and len(Xt) > 0:
                    bands = {}
                    for col in keep_cols:
                        try:
                            lo_b = float(np.nanpercentile(Xb_raw[col].values, p_low))
                            hi_b = float(np.nanpercentile(Xb_raw[col].values, p_high))
                            bands[col] = (lo_b, hi_b)
                        except Exception:
                            continue
                    # Align Xt_raw to Xt indices if possible
                    try:
                        Xt_raw_slice = Xt_raw.loc[Xt.index]
                    except Exception:
                        Xt_raw_slice = Xt_raw.iloc[:len(Xt)]
                    for col, (lo_b, hi_b) in bands.items():
                        try:
                            series = Xt_raw_slice[col]
                            ob_mask = (series < lo_b) | (series > hi_b)
                            ob_count = int(ob_mask.sum())
                            if ob_count > 0:
                                contrib_counts[col] = contrib_counts.get(col, 0) + ob_count
                        except Exception:
                            continue
                # Novelty IF on baseline, evaluated only at MTD event times (combine A + C)
                try:
                    if 'Xb_raw' in locals() and 'Xt_raw_slice' in locals() and anomaly_indices:
                        # Align event rows in raw space
                        try:
                            evt_rows = Xt_raw_slice.loc[anomaly_indices]
                        except Exception:
                            # fall back to positional alignment
                            evt_pos = [i for i in range(min(len(Xt_raw_slice), len(Xt))) if Xt.index[i] in anomaly_indices]
                            evt_rows = Xt_raw_slice.iloc[evt_pos]
                        if len(evt_rows) > 0:
                            alpha = float(mtd_cfg.get('if_novelty_alpha', 0.01) or 0.01)
                            from sklearn.ensemble import IsolationForest as _IF
                            for col in keep_cols:
                                try:
                                    train = Xb_raw[[col]].dropna().values
                                    test = evt_rows[[col]].dropna().values
                                    if len(train) < 20 or len(test) < 5:
                                        continue
                                    iso = _IF(contamination='auto', n_estimators=100, random_state=42)
                                    iso.fit(train)
                                    thr = float(np.quantile(iso.decision_function(train), alpha))
                                    test_scores = iso.decision_function(test)
                                    nov_count = int((test_scores < thr).sum())
                                    if nov_count > 0:
                                        contrib_counts[col] = contrib_counts.get(col, 0) + nov_count
                                except Exception:
                                    continue
                except Exception:
                    pass

                # Breakout detection (rolling-window) within stable-speed window
                try:
                    if 'Xb_raw' in locals() and 'Xt_raw' in locals() and len(pivot_df) > 0:
                        br = detect_breakouts(
                            pivot_df,
                            speed_col=primary_speed_tag,
                            tag_cols=keep_cols,
                            window=int(mtd_cfg.get('break_window', 20) or 20),
                            q_low=float(mtd_cfg.get('break_q_low', p_low/100 if 'p_low' in locals() else 0.10)),
                            q_high=float(mtd_cfg.get('break_q_high', p_high/100 if 'p_high' in locals() else 0.90)),
                            persist=int(mtd_cfg.get('break_persist', 2) or 2),
                            persist_window=int(mtd_cfg.get('break_persist_window', 3) or 3),
                            cooldown=int(mtd_cfg.get('break_cooldown', 5) or 5),
                            speed_window=speed_window if 'speed_window' in locals() else None,
                            recent_mask=recent_mask if 'recent_mask' in locals() else None,
                        )
                        for col, info in br.items():
                            contrib_counts[col] = contrib_counts.get(col, 0) + int(info.get('count', 0))
                except Exception:
                    pass

                # Populate by_tag with counts and rates
                by_tag: Dict[str, Any] = {}
                for col, cnt in contrib_counts.items():
                    by_tag[col] = {
                        'count': int(cnt),
                        'rate': float(cnt / max(1, len(Xt))),
                        'method': 'mtd_contribution',
                        'is_speed_parameter': col == primary_speed_tag
                    }
                results['by_tag'] = by_tag
            except Exception:
                results['by_tag'] = {}

            # Set total anomalies to MTD count (multivariate events)
            results['total_anomalies'] = results['mtd_anomalies']
                
        except Exception as e:
            logger.error(f"Error in MTD calculation: {e}")
            results['error'] = f'MTD calculation failed: {str(e)}'
        
        return results
    
    def _isolation_forest_batch_analysis(self, df: pd.DataFrame, unit_hint: Optional[str] = None) -> Dict[str, Any]:
        """Isolation Forest anomaly detection with batch processing for large datasets.
        
        Args:
            df: DataFrame with time series data
            
        Returns:
            Isolation Forest anomaly results
        """
        from sklearn.ensemble import IsolationForest
        import numpy as np
        
        results = {
            'total_anomalies': 0,
            'anomaly_rate': 0.0,
            'batches_processed': 0,
            'batch_results': [],
            'by_tag': {},
            'method': 'isolation_forest_batch'
        }
        
        try:
            # Create time-indexed multivariate dataset similar to MTD
            if 'time' not in df.columns or 'tag' not in df.columns:
                results['error'] = 'Missing required columns (time, tag)'
                return results
            
            # Resample to configured interval for consistent batch processing
            df_time_indexed = df.set_index('time')
            resample_rule_if = 'h'
            try:
                # Try to align IF resample to MTD config for this unit
                unit_code = unit_hint or ''
                if not unit_code:
                    # derive from tags in df
                    tags = df['tag'].unique().tolist()
                    for uc in ['K-12-01','K-16-01','K-19-01','K-31-01']:
                        if any(uc in str(t) for t in tags):
                            unit_code = uc
                            break
                if unit_code:
                    mtd_cfg_path = Path(f"mtd_config_{unit_code}.json")
                    if mtd_cfg_path.exists():
                        with open(mtd_cfg_path, 'r', encoding='utf-8') as f:
                            _m = json.load(f)
                        if isinstance(_m, dict) and _m.get('resample'):
                            resample_rule_if = _m.get('resample')
            except Exception:
                pass
            pivot_df = df_time_indexed.groupby(['tag']).resample(resample_rule_if)['value'].mean().unstack(level=0).reset_index()
            pivot_df.columns.name = None
            
            # Get available features
            available_tags = [col for col in pivot_df.columns if col != 'time']
            
            if len(available_tags) < 2:
                results['error'] = 'Insufficient features for Isolation Forest analysis'
                return results
            
            # Prepare feature matrix
            X = pivot_df[available_tags].ffill().dropna()
            
            if len(X) < 50:
                results['error'] = f'Insufficient data points: {len(X)} (need â‰¥50)'
                return results
            
            # Batch processing parameters
            batch_size = min(10000, max(1000, len(X) // 5))  # Adaptive batch size
            n_batches = (len(X) + batch_size - 1) // batch_size

            # Load optional IF config for this unit
            if_cfg = {
                'contamination': 0.05,
                'n_estimators': 100,
                'min_batch': 10
            }
            try:
                # Determine unit code from hint or by scanning tag names
                unit_code = unit_hint or ''
                if not unit_code:
                    for uc in ['K-12-01', 'K-16-01', 'K-19-01', 'K-31-01']:
                        if any(uc in str(t) for t in available_tags):
                            unit_code = uc
                            break
                base_cfg_path = Path(f"baseline_config_{unit_code}.json")
                if base_cfg_path.exists():
                    with open(base_cfg_path, 'r', encoding='utf-8') as f:
                        base_cfg = json.load(f)
                    if isinstance(base_cfg, dict) and 'if_config' in base_cfg:
                        for k in list(if_cfg.keys()):
                            if k in base_cfg['if_config']:
                                if_cfg[k] = base_cfg['if_config'][k]
                stand_path = Path(f"if_config_{unit_code}.json")
                if stand_path.exists():
                    with open(stand_path, 'r', encoding='utf-8') as f:
                        stand = json.load(f)
                    if isinstance(stand, dict):
                        for k in list(if_cfg.keys()):
                            if k in stand:
                                if_cfg[k] = stand[k]
            except Exception:
                pass
            
            # Build batch boundaries and merge small tail with previous batch
            min_batch = int(if_cfg.get('min_batch', 10) or 10)
            boundaries = []
            start = 0
            N = len(X)
            while start < N:
                end = min(start + batch_size, N)
                boundaries.append((start, end))
                start = end
            if len(boundaries) >= 2:
                tail = boundaries[-1][1] - boundaries[-1][0]
                if tail < min_batch:
                    prev_start, _ = boundaries[-2]
                    boundaries[-2] = (prev_start, N)
                    boundaries.pop()
            n_batches = len(boundaries)

            print(f"Isolation Forest: Processing {N:,} records in {n_batches} batches (size: {batch_size:,}, min_batch: {min_batch})")

            all_anomaly_scores = []
            all_anomaly_labels = []
            batch_anomaly_counts = []
            
            # Process data in batches
            for batch_idx, (start_idx, end_idx) in enumerate(boundaries):
                X_batch = X.iloc[start_idx:end_idx]
                
                # Train Isolation Forest on batch
                iso_forest = IsolationForest(
                    n_estimators=int(if_cfg.get('n_estimators', 100) or 100),
                    contamination=float(if_cfg.get('contamination', 0.05) or 0.05),
                    random_state=42,
                    n_jobs=-1
                )
                
                try:
                    # Fit and predict
                    anomaly_labels = iso_forest.fit_predict(X_batch)
                    anomaly_scores = iso_forest.decision_function(X_batch)
                    
                    # Count anomalies in this batch (Isolation Forest uses -1 for anomalies)
                    batch_anomalies = np.sum(anomaly_labels == -1)
                    batch_anomaly_counts.append(batch_anomalies)
                    
                    # Store results
                    all_anomaly_scores.extend(anomaly_scores)
                    all_anomaly_labels.extend(anomaly_labels)
                    
                    batch_info = {
                        'batch_id': batch_idx + 1,
                        'size': len(X_batch),
                        'anomalies': int(batch_anomalies),
                        'anomaly_rate': batch_anomalies / len(X_batch),
                        'avg_score': float(np.mean(anomaly_scores)),
                        'min_score': float(np.min(anomaly_scores)),
                        'time_range': {
                            'start': X.index[start_idx].isoformat() if hasattr(X.index[start_idx], 'isoformat') else str(X.index[start_idx]),
                            'end': X.index[end_idx-1].isoformat() if hasattr(X.index[end_idx-1], 'isoformat') else str(X.index[end_idx-1])
                        }
                    }
                    results['batch_results'].append(batch_info)
                    
                    print(f"  Batch {batch_idx + 1}/{n_batches}: {batch_anomalies} anomalies ({batch_anomalies/len(X_batch)*100:.1f}%)")
                    
                except Exception as e:
                    print(f"  Batch {batch_idx + 1} failed: {e}")
                    continue
            
            results['batches_processed'] = len(results['batch_results'])
            
            # Calculate overall statistics
            if all_anomaly_labels:
                total_anomalies = np.sum(np.array(all_anomaly_labels) == -1)
                results['total_anomalies'] = int(total_anomalies)
                results['anomaly_rate'] = total_anomalies / len(all_anomaly_labels)
                
                results['score_statistics'] = {
                    'mean_score': float(np.mean(all_anomaly_scores)),
                    'std_score': float(np.std(all_anomaly_scores)),
                    'min_score': float(np.min(all_anomaly_scores)),
                    'max_score': float(np.max(all_anomaly_scores))
                }
                
                print(f"Isolation Forest: Found {total_anomalies:,} anomalies ({total_anomalies/len(all_anomaly_labels)*100:.2f}%)")
            
            # Individual tag analysis for comparison (all tags, no IQR, univariate IF)
            for tag in available_tags:
                tag_data = X[tag].dropna()
                if len(tag_data) >= 50:
                    try:
                        iso_forest_single = IsolationForest(
                            n_estimators=int(if_cfg.get('n_estimators', 100) or 100)//2,
                            contamination=float(if_cfg.get('contamination', 0.05) or 0.05),
                            random_state=42
                        )
                        single_predictions = iso_forest_single.fit_predict(tag_data.values.reshape(-1, 1))
                        tag_anomalies = np.sum(single_predictions == -1)
                        
                        if int(tag_anomalies) > 0:
                            results['by_tag'][tag] = {
                                'count': int(tag_anomalies),
                                'rate': tag_anomalies / len(tag_data),
                                'method': 'isolation_forest_univariate'
                            }
                    except:
                        continue
            
        except Exception as e:
            logger.error(f"Error in Isolation Forest analysis: {e}")
            results['error'] = f'Isolation Forest analysis failed: {str(e)}'
        
        return results
    
    def generate_data_quality_report(self, unit: str) -> Dict[str, Any]:
        """Generate a data quality report for a unit.
        
        Args:
            unit: Unit identifier
            
        Returns:
            Data quality report
        """
        df = self.db.get_unit_data(unit)
        
        if df.empty:
            return {
                'unit': unit,
                'status': 'no_data',
                'report_timestamp': datetime.now().isoformat()
            }
        
        quality_report = {
            'unit': unit,
            'report_timestamp': datetime.now().isoformat(),
            'overall_score': 0.0,
            'metrics': {}
        }
        
        # Completeness (missing values)
        if 'value' in df.columns:
            total_values = len(df)
            missing_values = df['value'].isnull().sum()
            completeness = (total_values - missing_values) / total_values if total_values > 0 else 0
            quality_report['metrics']['completeness'] = {
                'score': completeness,
                'missing_values': int(missing_values),
                'total_values': int(total_values)
            }
        
        # Consistency (check for duplicates)
        if 'time' in df.columns and 'tag' in df.columns:
            total_records = len(df)
            duplicate_records = df.duplicated(subset=['time', 'tag']).sum()
            consistency = (total_records - duplicate_records) / total_records if total_records > 0 else 0
            quality_report['metrics']['consistency'] = {
                'score': consistency,
                'duplicate_records': int(duplicate_records),
                'total_records': int(total_records)
            }
        
        # Freshness (data recency)
        if 'time' in df.columns:
            try:
                series = pd.to_datetime(df['time'], errors='coerce')
                local_tz = datetime.now().astimezone().tzinfo
                if getattr(series.dt, 'tz', None) is None:
                    series = series.dt.tz_localize(local_tz)
                else:
                    series = series.dt.tz_convert(local_tz)
                latest_local = series.max()
                latest_utc = latest_local.tz_convert('UTC')
                now_utc = pd.Timestamp.now(tz='UTC')
                hours_old = (now_utc - latest_utc).total_seconds() / 3600
                latest_out = latest_local.tz_localize(None)
            except Exception:
                latest_out = pd.to_datetime(df['time'].max(), errors='coerce')
                hours_old = None
            freshness = max(0, 1 - (hours_old / 24))  # Linear decay over 24 hours
            
            quality_report['metrics']['freshness'] = {
                'score': freshness,
                'hours_old': hours_old,
                'latest_timestamp': latest_out.isoformat() if latest_out is not None else None
            }
        
        # Calculate overall score (average of available metrics)
        scores = [metric['score'] for metric in quality_report['metrics'].values()]
        quality_report['overall_score'] = sum(scores) / len(scores) if scores else 0
        
        # Quality grade
        if quality_report['overall_score'] >= 0.9:
            quality_report['grade'] = 'A'
        elif quality_report['overall_score'] >= 0.8:
            quality_report['grade'] = 'B'
        elif quality_report['overall_score'] >= 0.7:
            quality_report['grade'] = 'C'
        elif quality_report['overall_score'] >= 0.6:
            quality_report['grade'] = 'D'
        else:
            quality_report['grade'] = 'F'
        
        return quality_report
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all data.
        
        Returns:
            Complete status report
        """
        logger.info("Generating comprehensive status report")
        
        # Get database status
        db_status = self.db.get_database_status()
        
        # Scan all units
        scan_results = self.scan_all_units()
        
        # Generate quality reports for active units
        quality_reports = {}
        for unit in db_status['units'][:5]:  # Limit to top 5 units
            if unit['records'] > 0:
                quality_reports[unit['unit']] = self.generate_data_quality_report(unit['unit'])
        
        comprehensive_status = {
            'report_timestamp': datetime.now().isoformat(),
            'database_status': db_status,
            'scan_results': scan_results,
            'data_quality_reports': quality_reports,
            'recommendations': self._generate_recommendations(db_status, scan_results, quality_reports)
        }
        
        return comprehensive_status
    
    def _generate_recommendations(self, db_status: Dict, scan_results: Dict, quality_reports: Dict) -> List[str]:
        """Generate recommendations based on analysis.
        
        Args:
            db_status: Database status
            scan_results: Scan results
            quality_reports: Quality reports
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Storage recommendations
        if db_status['total_size_gb'] > 10:
            recommendations.append(f"Database size is {db_status['total_size_gb']:.1f}GB. Consider archiving old data.")
        
        # Freshness recommendations  
        if scan_results['summary']['stale_units'] > scan_results['summary']['fresh_units']:
            recommendations.append("More units have stale data than fresh. Consider increasing refresh frequency.")
        
        # Quality recommendations
        low_quality_units = [unit for unit, report in quality_reports.items() 
                           if report.get('overall_score', 0) < 0.7]
        if low_quality_units:
            recommendations.append(f"Units with low data quality: {', '.join(low_quality_units)}")
        
        # Empty units
        if scan_results['summary']['empty_units'] > 0:
            recommendations.append(f"{scan_results['summary']['empty_units']} units have no data. Check data collection.")
        
        if not recommendations:
            recommendations.append("Data quality and freshness look good! System is operating normally.")
        
        return recommendations
    
    def _get_excel_file_for_unit(self, unit: str, default_xlsx_path: Path = None) -> Path:
        """Determine the correct Excel file for a specific unit based on its plant."""
        try:
            # Get unit data to determine plant
            unit_data = self.db.get_unit_data(unit)
            if not unit_data.empty and 'plant' in unit_data.columns:
                plant = unit_data['plant'].iloc[0].upper()

                # Plant-specific Excel files (use absolute paths)
                project_root = Path(__file__).resolve().parents[1]

                if plant.startswith("ABF"):
                    abf_path = project_root / "excel" / "ABF_Automation.xlsx"
                    if abf_path.exists():
                        return abf_path
                elif plant.startswith("PCMSB"):
                    pcmsb_path = project_root / "excel" / "PCMSB_Automation.xlsx"
                    if pcmsb_path.exists():
                        return pcmsb_path
                    # Fallback: reuse PCFS workbook for PCMSB if dedicated file missing
                    pcmsb_path = project_root / "excel" / "PCFS_Automation_2.xlsx"
                    if pcmsb_path.exists():
                        return pcmsb_path
                    pcmsb_path = project_root / "excel" / "PCFS_Automation.xlsx"
                    if pcmsb_path.exists():
                        return pcmsb_path
                elif plant.startswith("PCFS"):
                    pcfs_path = project_root / "excel" / "PCFS_Automation_2.xlsx"
                    if pcfs_path.exists():
                        return pcfs_path
                    # Fallback to original PCFS file
                    pcfs_path = project_root / "excel" / "PCFS_Automation.xlsx"
                    if pcfs_path.exists():
                        return pcfs_path
        except Exception:
            pass

        # Fallback to default or first available
        if default_xlsx_path and default_xlsx_path.exists():
            return default_xlsx_path

        # Last resort - find any available Excel file (use absolute paths)
        project_root = Path(__file__).resolve().parents[1]
        excel_paths = [
            project_root / "excel" / "PCFS_Automation_2.xlsx",
            project_root / "excel" / "PCFS_Automation.xlsx",
            project_root / "excel" / "PCMSB_Automation.xlsx",
            project_root / "excel" / "ABF_Automation.xlsx",
            project_root / "data" / "raw" / "Automation.xlsx",
            project_root / "Automation.xlsx"
        ]
        for path in excel_paths:
            if path.exists():
                return path

        raise RuntimeError("No Excel automation file found")



    def _infer_plant_from_unit(self, unit: str) -> str:
        if unit.startswith('07') or unit.upper().startswith('ABF'):
            return 'ABF'
        if unit.startswith('K-'):
            return 'PCFS'
        if unit.startswith('C-') or unit.startswith('XT-'):
            return 'PCMSB'
        return 'UNKNOWN'

    def _load_unit_from_tags(self, unit: str, default_xlsx_path: Path, lookback: str = '-2d') -> pd.DataFrame:
        from .config import Config
        from .batch import build_unit_from_tags
        from .clean import dedup_parquet
        import pandas as pd

        cfg = Config()
        config_dir = cfg.paths.project_root / 'config'
        target = unit.replace('-', '').lower()
        tags_file = None
        for candidate in sorted(config_dir.glob('tags_*.txt')):
            name = candidate.stem.replace('tags_', '').replace('_', '').lower()
            if target in name:
                tags_file = candidate
                break
        if tags_file is None:
            raise RuntimeError(f"Tag file not found for {unit}")

        tags = [
            t.strip() for t in tags_file.read_text(encoding='utf-8').splitlines()
            if t.strip() and not t.strip().startswith('#')
        ]
        if not tags:
            raise RuntimeError(f"No tags defined for {unit} in {tags_file}")

        excel_path = self._get_excel_file_for_unit(unit, default_xlsx_path)
        if excel_path is None or not excel_path.exists():
            raise RuntimeError(f"No Excel workbook available for {unit}")

        plant = self._infer_plant_from_unit(unit)
        print(f"   Fallback: fetching {len(tags)} tags from PI for {unit} (plant {plant}) using {excel_path.name}...")
        temp_parquet = self.db.processed_dir / f"{unit}_fallback.parquet"
        try:
            build_unit_from_tags(
                excel_path,
                tags,
                temp_parquet,
                plant=plant,
                unit=unit,
                start=lookback,
                end='*',
                step='-0.1h',
                visible=False,
                settle_seconds=0.5,
            )
            df = pd.read_parquet(temp_parquet)
        finally:
            temp_parquet.unlink(missing_ok=True)

        if df.empty:
            raise RuntimeError(f"Tag fallback produced no data for {unit}")

        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time').drop_duplicates(subset=['time', 'tag'], keep='last').reset_index(drop=True)

        master = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
        if master.exists():
            try:
                existing = pd.read_parquet(master)
                existing['time'] = pd.to_datetime(existing['time'])
                df = pd.concat([existing, df], ignore_index=True)
                df = df.sort_values('time').drop_duplicates(subset=['time', 'tag'], keep='last').reset_index(drop=True)
            except Exception:
                pass

        df.to_parquet(master, index=False)
        dedup_parquet(master)
        return df

    def refresh_stale_units_with_progress(self, xlsx_path: Path = None, max_age_hours: float = 8.0) -> Dict[str, Any]:
        """Refresh stale units with real-time progress tracking.

        Args:
            xlsx_path: Default Excel file path (will be overridden per unit based on plant)
            max_age_hours: Maximum age before refreshing

        Returns:
            Refresh results with unit-by-unit progress
        """
        refresh_start = datetime.now()

        # Determine default Excel file if not provided (needed for auto-build)
        if xlsx_path is None:
            excel_paths = [
                Path("excel/ABF_Automation.xlsx"),
                Path("excel/PCMSB_Automation.xlsx"),
                Path("excel/PCFS_Automation.xlsx"),
                Path("data/raw/Automation.xlsx"),
                Path("Automation.xlsx")
            ]
            for path in excel_paths:
                if path.exists():
                    xlsx_path = path
                    break

        # Auto-build any configured units that don't have Parquet yet
        if xlsx_path is not None:
            try:
                seeded = self._auto_build_missing_units(xlsx_path)
                if seeded:
                    print(f"Seeded {sum(1 for s in seeded if s['success'])} new unit(s) before refresh.")
            except Exception as e:
                print(f"Auto-build step skipped due to error: {e}")

        # Get stale units after possible seeding
        scan_results = self.scan_all_units(max_age_hours=max_age_hours)
        stale_units = scan_results['stale_units']
        
        if not stale_units:
            return {
                "success": True,
                "message": "All units are fresh - no refresh needed",
                "fresh_units": scan_results['fresh_units'],
                "total_time": 0,
                "units_processed": []
            }
        
        # Ensure Excel file is available
        if xlsx_path is None:
            return {
                "success": False,
                "error": "No Excel file found for refresh"
            }
        
        print(f"\\nREFRESHING {len(stale_units)} STALE UNITS")
        print(f"Excel file: {xlsx_path}")
        print(f"Started: {refresh_start.strftime('%H:%M:%S')}")
        print("=" * 60)
        
        results = {
            "success": True,
            "start_time": refresh_start.isoformat(),
            "excel_file": str(xlsx_path),
            "units_to_refresh": stale_units.copy(),
            "units_processed": [],
            "unit_results": {},
            "total_time": 0,
            "fresh_after_refresh": []
        }
        
        # Track refreshed Excel files to avoid multiple refreshes
        refreshed_excel_files = set()

        # Process each unit with progress display
        for i, unit in enumerate(stale_units):
            unit_number = i + 1
            unit_start = time.time()

            print(f"\\n[{unit_number}/{len(stale_units)}] Processing: {unit}")
            print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 40)

            try:
                # Show before status
                before_info = self.db.get_data_freshness_info(unit)
                print(f"   Before: {before_info['total_records']:,} records, {before_info['data_age_hours']:.1f}h old")

                # Determine correct Excel file for this specific unit
                unit_xlsx_path = self._get_excel_file_for_unit(unit, xlsx_path)
                print(f"   Excel file for {unit}: {unit_xlsx_path.name}")

                # Perform Excel refresh (only once per Excel file)
                if str(unit_xlsx_path) not in refreshed_excel_files:
                    print(f"   Refreshing {unit_xlsx_path.name} with PI DataLink...")
                    excel_start = time.time()
                    refresh_excel_safe(unit_xlsx_path)
                    excel_time = time.time() - excel_start
                    print(f"   Excel refresh completed in {excel_time:.1f}s")
                    refreshed_excel_files.add(str(unit_xlsx_path))
                else:
                    print(f"   Using data from previous {unit_xlsx_path.name} refresh")

                # Process unit data
                print(f"   Processing data for {unit}...")
                from .ingest import load_latest_frame, write_parquet

                # Load fresh data using the correct Excel file
                df = load_latest_frame(unit_xlsx_path, unit=unit)
                if df.empty:
                    print(f"   WARNING: Excel returned no data for {unit}; switching to direct tag retrieval...")
                    df = self._load_unit_from_tags(unit, unit_xlsx_path)

                # Merge fresh data with existing historical data
                print(f"   Merging fresh data with historical records...")
                
                # Load existing master data (excluding other refreshed files)
                existing_df = pd.DataFrame()
                unit_files = list(self.db.processed_dir.glob(f"*{unit}*.parquet"))
                master_files = [f for f in unit_files if 'refreshed' not in f.name]
                
                if master_files:
                    # Use the most recent master file (dedup preferred)
                    dedup_files = [f for f in master_files if 'dedup' in f.name]
                    if dedup_files:
                        master_file = max(dedup_files, key=lambda x: x.stat().st_mtime)
                    else:
                        master_file = max(master_files, key=lambda x: x.stat().st_mtime)
                    
                    print(f"   Loading historical data from: {master_file.name}")
                    existing_df = pd.read_parquet(master_file)
                    print(f"   Historical records: {len(existing_df):,}")
                
                # Combine data: existing historical + fresh data
                if not existing_df.empty:
                    # Ensure time columns are datetime
                    if 'time' in existing_df.columns:
                        existing_df['time'] = pd.to_datetime(existing_df['time'])
                    if 'time' in df.columns:
                        df['time'] = pd.to_datetime(df['time'])
                    
                    # Combine and remove duplicates by time
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    if 'time' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(subset=['time']).sort_values('time').reset_index(drop=True)
                    
                    print(f"   Combined records: {len(combined_df):,}")
                else:
                    combined_df = df
                    print(f"   No historical data found, using fresh data only")
                
                # Find and update the master parquet file directly
                # Look for the main master file (prioritize _1y_0p1h.parquet pattern)
                master_patterns = [f"{unit}_1y_0p1h.parquet", f"{unit}.parquet", f"*{unit}*.parquet"]
                master_file = None
                
                for pattern in master_patterns:
                    matches = list(self.db.processed_dir.glob(pattern))
                    # Exclude updated/refreshed files to find the real master
                    matches = [f for f in matches if 'updated' not in f.name and 'refreshed' not in f.name]
                    if matches:
                        master_file = matches[0]  # Take the first match
                        break
                
                if master_file:
                    # Update the existing master file
                    parquet_path = master_file
                    print(f"   Updating master file: {parquet_path.name}")
                else:
                    # Create new master file if none exists
                    parquet_path = self.db.processed_dir / f"{unit}_1y_0p1h.parquet"
                    print(f"   Creating new master file: {parquet_path.name}")

                output_path = write_parquet(combined_df, parquet_path)
                print(f"   Updated master: {parquet_path.name}")

                # Regenerate deduplicated view so freshness checks see new data
                dedup_path = dedup_parquet(parquet_path)
                print(f"   Updated dedup: {dedup_path.name}")

                # Update df for metrics reporting
                df = combined_df

                # Check after status (master file was updated in place)
                after_info = self.db.get_data_freshness_info(unit)
                unit_time = time.time() - unit_start
                
                unit_result = {
                    "success": True,
                    "unit": unit,
                    "processing_time": unit_time,
                    "records_before": before_info['total_records'],
                    "records_after": len(df),
                    "age_before_hours": before_info['data_age_hours'],
                    "age_after_hours": after_info['data_age_hours'] if not after_info['is_stale'] else 0,
                    "output_file": str(output_path),
                    "file_size_mb": output_path.stat().st_size / (1024 * 1024)
                }
                
                results["unit_results"][unit] = unit_result
                results["units_processed"].append(unit)
                
                if not after_info['is_stale']:
                    results["fresh_after_refresh"].append(unit)
                
                print(f"   {unit} completed in {unit_time:.1f}s")
                print(f"   Records: {before_info['total_records']:,} -> {len(df):,}")
                print(f"   Output: {unit_result['file_size_mb']:.1f}MB")
                print(f"   Age: {before_info['data_age_hours']:.1f}h -> Fresh")
                
                # Show overall progress
                completed = len(results["units_processed"])
                remaining = len(stale_units) - completed
                progress_pct = (completed / len(stale_units)) * 100
                
                print(f"\\nProgress: {completed}/{len(stale_units)} completed ({progress_pct:.1f}%), {remaining} remaining")
                
            except Exception as e:
                unit_time = time.time() - unit_start
                error_msg = str(e)
                
                unit_result = {
                    "success": False,
                    "unit": unit,
                    "processing_time": unit_time,
                    "error": error_msg
                }
                
                results["unit_results"][unit] = unit_result
                results["units_processed"].append(unit)
                
                print(f"   {unit} FAILED after {unit_time:.1f}s")
                print(f"   Error: {error_msg}")
        
        # Final summary
        refresh_end = datetime.now()
        total_time = (refresh_end - refresh_start).total_seconds()
        successful = len([r for r in results["unit_results"].values() if r["success"]])
        failed = len([r for r in results["unit_results"].values() if not r["success"]])
        
        results.update({
            "end_time": refresh_end.isoformat(),
            "total_time": total_time,
            "successful_units": successful,
            "failed_units": failed,
            "success_rate": (successful / len(stale_units)) * 100 if stale_units else 0
        })
        
        print(f"\\n" + "=" * 60)
        print(f"REFRESH COMPLETED")
        print(f"Total time: {total_time/60:.1f} minutes ({total_time:.1f} seconds)")
        print(f"Successful: {successful}/{len(stale_units)} units")
        print(f"Failed: {failed}/{len(stale_units)} units")
        print(f"Success rate: {results['success_rate']:.1f}%")
        
        if results["fresh_after_refresh"]:
            print(f"Fresh units: {', '.join(results['fresh_after_refresh'])}")
        
        return results






