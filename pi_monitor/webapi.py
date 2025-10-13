from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Dict, Any, Tuple, List
import os
import urllib.parse
import requests
import pandas as pd
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _to_webapi_time(t: str) -> str:
    # Convert PI relative format like "-4h" to Web API friendly "*-4h".
    if t is None:
        return "*"
    ts = str(t).strip()
    if ts.startswith("-"):
        return f"*{ts}"
    if ts == "*":
        return ts
    return ts


@dataclass
class PIWebAPIClient:
    base_url: str
    auth_mode: str = "windows"  # 'windows' | 'basic' | 'none'
    username: Optional[str] = None
    password: Optional[str] = None
    verify_ssl: bool = True
    timeout: float = 30.0

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        # Thread-local session to keep requests safe across threads
        self._local = threading.local()

    def _get_session(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            if self.auth_mode == "windows":
                # Use Windows authentication (NTLM/Kerberos)
                try:
                    from requests_ntlm import HttpNtlmAuth
                    sess.auth = HttpNtlmAuth(self.username or None, self.password or None)
                except ImportError:
                    # Fallback to requests-negotiate-sspi for Windows
                    try:
                        from requests_negotiate_sspi import HttpNegotiateAuth
                        sess.auth = HttpNegotiateAuth()
                    except ImportError:
                        # Last resort: try basic auth or anonymous
                        pass
            elif self.auth_mode == "basic" and self.username:
                from requests.auth import HTTPBasicAuth
                sess.auth = HTTPBasicAuth(self.username or "", self.password or "")
            # else: anonymous (no auth)
            sess.headers.update({"Accept": "application/json"})
            self._local.session = sess
        return sess

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        sess = self._get_session()
        r = sess.get(url, params=params or {}, timeout=self.timeout, verify=self.verify_ssl)
        r.raise_for_status()
        return r.json()

    def health_check(self) -> Tuple[bool, str]:
        """Quickly probe the Web API endpoint.

        Returns (ok, info). On success, info is HTTP status text or 'OK'. On failure,
        info contains a short error string (status or exception).
        """
        try:
            sess = self._get_session()
            # Prefer /system when available; fall back to base path
            url = f"{self.base_url}/system"
            r = sess.get(url, timeout=min(10.0, self.timeout or 10.0), verify=self.verify_ssl)
            if 200 <= r.status_code < 300:
                return True, "OK"
            return False, f"HTTP {r.status_code}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def resolve_point_webid(self, server: str, tag: str) -> Optional[str]:
        # Path format: \\Server\TagName must be URL-encoded
        path = f"\\\\{server.strip('\\')}\\{tag}"
        # Newer PI Web API uses /points?path=, older may need /points?path= too; keep default
        data = self._get("/points", params={"path": path})
        # Expected: { "WebId": "...", ... }
        if isinstance(data, dict) and data.get("WebId"):
            return data["WebId"]
        # Some servers return Items array
        items = data.get("Items") if isinstance(data, dict) else None
        if isinstance(items, list) and items:
            w = items[0].get("WebId")
            if w:
                return w
        return None

    def fetch_interpolated(self, webid: str, start: str, end: str, interval: str) -> pd.DataFrame:
        params = {
            "startTime": _to_webapi_time(start),
            "endTime": _to_webapi_time(end),
            "interval": interval.replace("-", "").replace("h", "h").replace("m", "m"),
        }
        data = self._get(f"/streams/{urllib.parse.quote(webid)}/interpolated", params=params)
        items = data.get("Items", []) if isinstance(data, dict) else []
        times = []
        values = []
        for it in items:
            ts = it.get("Timestamp")
            val = it.get("Value")
            # PI Web API may wrap errors as {"Value":{"Name":"Bad Input"}, ...}
            if isinstance(val, dict):
                continue
            times.append(ts)
            values.append(val)
        if not times:
            return pd.DataFrame(columns=["time", "value"])  # empty
        df = pd.DataFrame({"time": pd.to_datetime(times, errors="coerce"), "value": pd.to_numeric(values, errors="coerce")})
        df = df.dropna(subset=["time", "value"]).reset_index(drop=True)
        return df


class _RateLimiter:
    def __init__(self, qps: float) -> None:
        self.qps = max(0.1, float(qps))
        self.min_interval = 1.0 / self.qps
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self._next <= now:
                self._next = now + self.min_interval
                return
            sleep_s = self._next - now
            self._next += self.min_interval
        if sleep_s > 0:
            time.sleep(sleep_s)


def _fetch_one_tag(
    client: PIWebAPIClient,
    tag: str,
    server: str,
    start: str,
    end: str,
    interval: str,
    limiter: Optional[_RateLimiter],
    retries: int,
    jitter: Tuple[float, float],
) -> Optional[pd.DataFrame]:
    tag = tag.strip()
    if not tag or tag.startswith('#'):
        return None
    backoff = 0.5
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            if limiter:
                limiter.acquire()
            webid = client.resolve_point_webid(server, tag)
            if not webid:
                return None
            if limiter:
                limiter.acquire()
            df = client.fetch_interpolated(webid, start, end, interval)
            if df.empty:
                return None
            df["tag"] = tag.replace(".", "_")
            return df
        except requests.HTTPError as he:
            last_exc = he
            # Respect 429/503 with backoff
            code = getattr(he.response, 'status_code', None)
            if code in (429, 503):
                time.sleep(backoff + random.uniform(*jitter))
                backoff = min(backoff * 2.0, 8.0)
                continue
            # Other HTTP errors: don’t hammer
            time.sleep(0.2 + random.uniform(*jitter))
        except Exception as e:
            last_exc = e
            time.sleep(0.2 + random.uniform(*jitter))
    # Exhausted retries
    return None


def fetch_tags_via_webapi(
    tags: Iterable[str],
    server: str,
    start: str,
    end: str,
    step: str,
    *,
    base_url: Optional[str] = None,
    auth_mode: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
    timeout: Optional[float] = None,
    max_workers: Optional[int] = None,
    qps: Optional[float] = None,
    retries: Optional[int] = None,
) -> pd.DataFrame:
    # Load defaults from environment
    base_url = base_url or os.getenv("PI_WEBAPI_URL")
    if not base_url:
        raise RuntimeError("PI_WEBAPI_URL is not set; cannot use PI Web API fetch.")
    auth_mode = (auth_mode or os.getenv("PI_WEBAPI_AUTH") or "windows").strip().lower()
    username = username or os.getenv("PI_WEBAPI_USER")
    password = password or os.getenv("PI_WEBAPI_PASS")
    verify_ssl = _bool_env("PI_WEBAPI_VERIFY_SSL", True) if verify_ssl is None else verify_ssl
    timeout = float(os.getenv("PI_WEBAPI_TIMEOUT", "30").strip()) if timeout is None else timeout

    # Polite, conservative defaults
    max_workers = int(os.getenv("PI_WEBAPI_MAX_WORKERS", str(max_workers or 4)))
    qps = float(os.getenv("PI_WEBAPI_QPS", str(qps or 3.0)))  # requests per second globally
    retries = int(os.getenv("PI_WEBAPI_RETRIES", str(retries or 2)))

    client = PIWebAPIClient(
        base_url=base_url,
        auth_mode=auth_mode,
        username=username,
        password=password,
        verify_ssl=verify_ssl,
        timeout=timeout,
    )

    # Fast preflight: if unreachable, emit a clear warning and return empty
    try:
        ok, info = client.health_check()
        if not ok:
            print(f"[warn] PI Web API unreachable at {client.base_url}: {info}")
            return pd.DataFrame(columns=["time", "value", "tag"])  # empty
    except Exception as _hc_err:
        # Non-fatal; continue but caller will likely see empty results
        try:
            print(f"[warn] PI Web API health check failed: {_hc_err}")
        except Exception:
            pass

    # Normalize step like "-0.1h" to Web API interval "6m"
    step = step.strip()
    interval = "6m"
    if step.startswith("-"):
        try:
            if step.endswith("h"):
                hours = float(step[1:-1])
                interval = f"{int(round(hours * 60))}m"
            elif step.endswith("m"):
                minutes = float(step[1:-1])
                interval = f"{int(round(minutes))}m"
        except Exception:
            interval = "6m"

    limiter = _RateLimiter(qps) if qps and qps > 0 else None
    frames: List[pd.DataFrame] = []
    tasks: List[Tuple[str]] = []
    for tag in tags:
        s = str(tag).strip()
        if s and not s.startswith('#'):
            tasks.append((s,))

    if not tasks:
        return pd.DataFrame(columns=["time", "value", "tag"])  # empty

    jitter = (0.01, 0.07)  # small randomized delay to avoid sync bursts
    with ThreadPoolExecutor(max_workers=max_workers or 4) as ex:
        futs = [
            ex.submit(
                _fetch_one_tag,
                client,
                t[0],
                server,
                start,
                end,
                interval,
                limiter,
                retries or 2,
                jitter,
            )
            for t in tasks
        ]
        for f in as_completed(futs):
            df = f.result()
            if df is not None and not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["time", "value", "tag"])  # empty
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["time"]).reset_index(drop=True)
    return out
