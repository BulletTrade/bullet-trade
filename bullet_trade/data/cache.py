from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, date as Date, timezone
from typing import Any, Dict, Optional, Callable, List

import pandas as pd
import numpy as np


class CacheManager:
    """
    文件缓存管理器：基于环境变量 DATA_CACHE_DIR 开启/关闭。
    - 针对 provider 的 5 个高频网络方法提供统一缓存：get_price、get_trade_days、get_all_securities、get_index_stocks、get_split_dividend
    - 键生成包含 provider、method 以及归一化后的参数，避免误复用
    - 历史区间永久缓存；包含今天/动态区间采用 TTL（默认 1 天，可用 JQDATA_CACHE_EXPIRE_DAYS 配置）
    - DataFrame 优先 parquet（若失败则自动回退到 pickle），列表/字典使用 json
    """

    def __init__(
        self,
        provider_name: str,
        cache_dir: Optional[str] = None,
        fallback_to_env: bool = True,
    ) -> None:
        self.provider_name = provider_name or "unknown"
        if cache_dir is not None:
            self.cache_dir = cache_dir
        elif fallback_to_env:
            base_dir = os.getenv("DATA_CACHE_DIR", "")
            self.cache_dir = os.path.join(base_dir, self.provider_name) if base_dir else ""
        else:
            self.cache_dir = ""
        self.enabled = bool(self.cache_dir)
        self.expire_days = int(os.getenv("JQDATA_CACHE_EXPIRE_DAYS", "1") or 1)
        self.default_df_format = os.getenv("JQDATA_CACHE_FORMAT", "parquet").lower()
        # 缓存版本：当数据结构/口径调整时可通过环境变量强制失效（默认'2'）
        self.schema_version = os.getenv("JQDATA_CACHE_VERSION", "2")
        if self.enabled:
            os.makedirs(self.cache_dir, exist_ok=True)

    # -------------------------- 公共入口 --------------------------
    def cached_call(
        self,
        method_name: str,
        kwargs: Dict[str, Any],
        fetch_fn: Callable[[Dict[str, Any]], Any],
        result_type: str,
    ) -> Any:
        """
        统一缓存入口：若禁用则直接调用；若启用则尝试读缓存，不命中时调用 fetch_fn 并写入缓存。
        result_type: 'df' | 'list_str' | 'list_dict' | 'list_date'
        """
        if not self.enabled:
            return fetch_fn(kwargs)

        norm_params = self._normalize_params(kwargs)
        dynamic_ttl = self._infer_ttl_days(norm_params)
        key_hash = self._build_key_hash(method_name, norm_params)
        data_path, meta_path = self._resolve_paths(method_name, key_hash, result_type)

        # 读缓存
        cached = self._load_if_valid(data_path, meta_path, dynamic_ttl)
        if cached is not None:
            return self._to_return_type(cached, result_type)

        # 未命中或过期 -> 拉取 + 写入
        result = fetch_fn(kwargs)
        self._atomic_write(method_name, data_path, meta_path, result, result_type, norm_params, dynamic_ttl)
        return result

    # -------------------------- 归一化/键 --------------------------
    @staticmethod
    def _normalize_temporal(value: Optional[Any]) -> Optional[str]:
        """
        归一化时间/日期参数：
        - 仅日期（或 00:00:00）保留到日级，确保老缓存键不受影响
        - 含具体时间的值（datetime 或字符串带时间）保留到秒，避免分钟级行情被同一天覆盖
        """
        if value is None:
            return None
        try:
            dt = pd.to_datetime(value)
        except Exception:
            return None

        if isinstance(dt, pd.Timestamp):
            if dt.tzinfo is not None:
                dt = dt.tz_convert("UTC").tz_localize(None)
            has_time = not (
                dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0
            )
            if has_time:
                return dt.to_pydatetime().isoformat()
            return dt.date().isoformat()

        if isinstance(value, datetime):
            naive = value if value.tzinfo is None else value.astimezone(timezone.utc).replace(tzinfo=None)
            has_time = not (
                naive.hour == 0 and naive.minute == 0 and naive.second == 0 and naive.microsecond == 0
            )
            return naive.isoformat() if has_time else naive.date().isoformat()

        if isinstance(value, Date):
            return value.isoformat()

        return None

    @staticmethod
    def _bool_to_int(b: Optional[bool]) -> Optional[int]:
        if b is None:
            return None
        return 1 if bool(b) else 0

    def _normalize_params(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for k, v in kwargs.items():
            if k in ("start_date", "end_date", "date", "pre_factor_ref_date"):
                params[k] = self._normalize_temporal(v)
            elif k in ("fields",):
                if v is None:
                    params[k] = None
                elif isinstance(v, (list, tuple)):
                    try:
                        params[k] = ",".join(sorted([str(x) for x in v]))
                    except Exception:
                        params[k] = ",".join([str(x) for x in v])
                else:
                    params[k] = str(v)
            elif k in ("security",):
                if isinstance(v, (list, tuple)):
                    params[k] = "|" + "|".join([str(x) for x in v])  # 保留顺序并区分单字符串
                else:
                    params[k] = str(v)
            elif isinstance(v, bool):
                params[k] = self._bool_to_int(v)
            else:
                # 其他类型转字符串，None 留 None
                if v is None:
                    params[k] = None
                else:
                    try:
                        params[k] = str(v)
                    except Exception:
                        params[k] = repr(v)
        return params

    def _build_key_hash(self, method_name: str, params: Dict[str, Any]) -> str:
        payload = {
            "provider": self.provider_name,
            "method": method_name,
            "params": params,
            "ver": self.schema_version,
        }
        s = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    # -------------------------- TTL 推断 --------------------------
    def _infer_ttl_days(self, params: Dict[str, Any]) -> Optional[int]:
        today = pd.Timestamp.today().date()

        # get_price / trade_days / index_stocks / all_securities / split_dividend 的通用判断
        # - end_date >= today 或 end_date 为 None -> 动态区间 -> TTL
        # - 有 count 参数 -> 动态区间 -> TTL
        # - date 为 None 或 >= today -> TTL
        end_date = self._to_date(params.get("end_date")) if params.get("end_date") else None
        start_date = self._to_date(params.get("start_date")) if params.get("start_date") else None
        date_single = self._to_date(params.get("date")) if params.get("date") else None
        has_count = params.get("count") is not None

        if has_count:
            return self.expire_days
        if date_single is None or (date_single and date_single >= today):
            return self.expire_days
        if end_date is None or (end_date and end_date >= today):
            return self.expire_days

        # 历史区间：永久
        return None

    # -------------------------- 路径 & 读写 --------------------------
    def _resolve_paths(self, method_name: str, key_hash: str, result_type: str) -> (str, str):
        base_dir = os.path.join(self.cache_dir, self.provider_name, method_name)
        os.makedirs(base_dir, exist_ok=True)
        # 扩展名先按默认选择；写入时可能回退到 pickle
        ext = "json"
        if result_type == "df":
            ext = "parquet" if self.default_df_format == "parquet" else "pkl"
        data_path = os.path.join(base_dir, f"{key_hash}.{ext}")
        meta_path = os.path.join(base_dir, f"{key_hash}.meta.json")
        return data_path, meta_path

    def _load_if_valid(self, data_path: str, meta_path: str, ttl_days: Optional[int]) -> Optional[Any]:
        try:
            if not os.path.exists(meta_path):
                return None
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)

            # TTL 校验
            if ttl_days is not None:
                expire_at = meta.get("expire_at")
                if expire_at:
                    try:
                        if datetime.fromisoformat(expire_at) < datetime.now():
                            return None
                    except Exception:
                        return None

            # 数据路径优先从 meta 读取，保证扩展名一致
            real_data_path = meta.get("data_file_path") or data_path
            if not os.path.exists(real_data_path):
                return None

            # 读取数据
            fmt = meta.get("format", "json")
            if fmt == "parquet":
                try:
                    return pd.read_parquet(real_data_path)
                except Exception:
                    return None
            elif fmt == "pkl":
                try:
                    return pd.read_pickle(real_data_path)
                except Exception:
                    return None
            else:
                with open(real_data_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            return None

    def _atomic_write(
        self,
        method_name: str,
        data_path: str,
        meta_path: str,
        result: Any,
        result_type: str,
        norm_params: Dict[str, Any],
        ttl_days: Optional[int],
    ) -> None:
        try:
            # 选择格式并写入数据
            fmt = "json"
            tmp_data_path = data_path + ".tmp"
            root, _ = os.path.splitext(data_path)
            if result_type == "df":
                fmt = "parquet"
                try:
                    # 优先 parquet
                    result.to_parquet(tmp_data_path)
                    final_data_path = root + ".parquet"
                except Exception:
                    # 回退 pickle
                    fmt = "pkl"
                    result.to_pickle(tmp_data_path)
                    final_data_path = root + ".pkl"
            else:
                fmt = "json"
                serializable = result
                # 将日期对象转为字符串以便 json
                if result_type in ("list_date", "list_dict"):
                    serializable = self._convert_dates_for_json(result)
                with open(tmp_data_path, "w", encoding="utf-8") as f:
                    json.dump(serializable, f, ensure_ascii=False)
                final_data_path = root + ".json"

            os.replace(tmp_data_path, final_data_path)

            # 写 meta
            expire_at = None
            if ttl_days is not None:
                expire_at = (datetime.fromtimestamp(datetime.now().timestamp() + ttl_days * 24 * 3600).isoformat())
            meta = {
                "provider": self.provider_name,
                "method": self._safe_str(method_name),
                "params": norm_params,
                "format": fmt,
                "type": result_type,
                "created_at": datetime.now().isoformat(),
                "expire_at": expire_at,
                "data_file_path": final_data_path,
            }
            tmp_meta_path = meta_path + ".tmp"
            with open(tmp_meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False)
            os.replace(tmp_meta_path, meta_path)
        except Exception:
            # 忽略缓存写入失败
            pass

    @staticmethod
    def _safe_str(s: Any) -> str:
        try:
            return str(s)
        except Exception:
            return repr(s)

    @staticmethod
    def _convert_dates_for_json(obj: Any) -> Any:
        """
        递归地将数据结构中的日期与 NumPy/Pandas 标量转换为 JSON 友好的原生类型。
        """
        # pandas Index/Series 以及 numpy 数组转为列表
        if isinstance(obj, (pd.Series, pd.Index)):
            obj = obj.tolist()
        elif isinstance(obj, np.ndarray):
            obj = obj.tolist()
        elif isinstance(obj, tuple):
            obj = list(obj)
        elif isinstance(obj, set):
            obj = list(obj)

        if isinstance(obj, list):
            out: List[Any] = []
            for x in obj:
                out.append(CacheManager._convert_dates_for_json(x))
            return out

        if isinstance(obj, dict):
            return {k: CacheManager._convert_dates_for_json(v) for k, v in obj.items()}

        if isinstance(obj, (datetime, Date)):
            try:
                return pd.to_datetime(obj).date().isoformat()
            except Exception:
                return str(obj)

        if isinstance(obj, np.generic):
            try:
                return obj.item()
            except Exception:
                return obj

        return obj

    @staticmethod
    def _to_return_type(obj: Any, result_type: str) -> Any:
        if result_type == "df":
            return obj  # DataFrame
        if result_type == "list_str":
            return list(obj)
        if result_type == "list_dict":
            # 把常见日期字段从字符串转换回日期对象，避免比较失败
            def _convert_mapping(mapping: Dict[Any, Any]) -> Dict[Any, Any]:
                y = dict(mapping)
                for k in ("date", "start_date", "end_date", "ex_date", "record_date"):
                    v = y.get(k)
                    if isinstance(v, str):
                        try:
                            y[k] = pd.to_datetime(v).date()
                        except Exception:
                            pass
                return y

            try:
                if isinstance(obj, dict):
                    return {k: _convert_mapping(v) if isinstance(v, dict) else v for k, v in obj.items()}
                converted = []
                for x in obj:
                    if isinstance(x, dict):
                        converted.append(_convert_mapping(x))
                    else:
                        converted.append(x)
                return converted
            except Exception:
                try:
                    return list(obj)
                except Exception:
                    return obj
        if result_type == "list_date":
            try:
                return [pd.to_datetime(x) for x in obj]
            except Exception:
                return obj
        return obj
    @staticmethod
    def _to_date(value: Optional[Any]) -> Optional[Date]:
        if value is None:
            return None
        if isinstance(value, Date) and not isinstance(value, datetime):
            return value
        try:
            ts = pd.to_datetime(value)
        except Exception:
            return None
        if isinstance(ts, pd.Timestamp):
            return ts.date()
        if isinstance(ts, datetime):
            return ts.date()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).date()
            except Exception:
                return None
        return None
