#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
混沌随机数核心模块（含天气熵源）
提供：
- ChaosEntropyPool: SHA-256 累积熵池
- ProcessSnapshotCollector: 系统进程抖动采集
- WeatherEntropyCollector: 全国省会温度采集
- SeedGenerator: 最终种子合成
"""

import os
import time
import hashlib
import asyncio
import aiohttp
import psutil
from typing import Dict, Tuple, List, Optional

# ======================================================================
# 防御性时间戳工具
# ======================================================================
def _get_safe_timestamp_bytes() -> bytes:
    try:
        ns = time.perf_counter_ns()
        if ns > 10_000_000:
            return ns.to_bytes(16, 'big')
        wall_ns = time.time_ns()
        if wall_ns > 1_577_836_800_000_000_000:
            return wall_ns.to_bytes(16, 'big')
        return os.urandom(16)
    except:
        return os.urandom(16)

# ======================================================================
# 1. 熵池
# ======================================================================
class ChaosEntropyPool:
    def __init__(self):
        self._hasher = hashlib.sha256()
        self._mix(str(time.perf_counter_ns()))

    def _mix(self, data: str) -> None:
        self._hasher.update(data.encode('utf-8'))
        self._hasher.update(_get_safe_timestamp_bytes())

    def mix_event(self, event_str: str) -> None:
        self._mix(event_str)

    def digest(self) -> bytes:
        return self._hasher.digest()

# ======================================================================
# 2. 进程快照采集器（物理熵）
# ======================================================================
class ProcessSnapshotCollector:
    def __init__(self, pool: ChaosEntropyPool):
        self.pool = pool
        self.last_snapshot: Dict[int, Tuple[str, float, float, int]] = {}
        self.is_first_run = True

    def _collect_current(self) -> Dict[int, Tuple[str, float, float, int]]:
        snapshot = {}
        for proc in psutil.process_iter(['pid', 'name', 'cpu_times', 'io_counters']):
            try:
                info = proc.info
                pid = info['pid']
                name = info['name'] or 'unknown'
                ct = info['cpu_times']
                cpu_user = ct.user if ct else 0.0
                cpu_sys = ct.system if ct else 0.0
                io = info['io_counters']
                io_read = io.read_bytes if io else 0
                snapshot[pid] = (name, cpu_user, cpu_sys, io_read)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        return snapshot

    def scan_and_update(self) -> None:
        current = self._collect_current()
        if self.is_first_run:
            self.last_snapshot = current
            self.is_first_run = False
            return

        old = self.last_snapshot
        old_pids = set(old.keys())
        new_pids = set(current.keys())

        died = old_pids - new_pids
        born = new_pids - old_pids
        if died:
            self.pool.mix_event(f"die:{','.join(map(str, died))}")
        if born:
            self.pool.mix_event(f"born:{','.join(map(str, born))}")

        for pid, (name, u1, s1, io1) in current.items():
            if pid in old:
                _, u0, s0, io0 = old[pid]
                delta_cpu = (u1 - u0) + (s1 - s0)
                delta_io = io1 - io0
                if delta_cpu > 0.001 or delta_io > 4096:
                    evt = f"proc:{name}|{pid}|c:{delta_cpu:.6f}|i:{delta_io}"
                    self.pool.mix_event(evt)

        self.last_snapshot = current

# ======================================================================
# 3. 天气熵采集器
# ======================================================================
# 省份 -> 省会城市名（用于 wttr.in）
PROVINCE_CAPITALS = {
    "北京": "beijing", "上海": "shanghai", "天津": "tianjin", "重庆": "chongqing",
    "河北": "shijiazhuang", "山西": "taiyuan", "辽宁": "shenyang", "吉林": "changchun",
    "黑龙江": "haerbin", "江苏": "nanjing", "浙江": "hangzhou", "安徽": "hefei",
    "福建": "fuzhou", "江西": "nanchang", "山东": "jinan", "河南": "zhengzhou",
    "湖北": "wuhan", "湖南": "changsha", "广东": "guangzhou", "海南": "haikou",
    "四川": "chengdu", "贵州": "guiyang", "云南": "kunming", "陕西": "xian",
    "甘肃": "lanzhou", "青海": "xining", "台湾": "taipei", "内蒙古": "huhehaote",
    "广西": "nanning", "西藏": "lasa", "宁夏": "yinchuan", "新疆": "wulumuqi",
    "香港": "hongkong", "澳门": "macau"
}

class WeatherEntropyCollector:
    def __init__(self, pool: ChaosEntropyPool, sample_count: Optional[int] = None):
        """
        :param pool: 熵池实例
        :param sample_count: 随机选取省份数量，None 表示全部省份
        """
        self.pool = pool
        self.sample_count = sample_count
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    async def _fetch_temperature(self, city: str) -> Optional[int]:
        """获取单个城市的实时气温（整数摄氏度）"""
        url = f"https://wttr.in/{city}?format=%t"
        try:
            async with self._session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
                # 解析如 "+23°C" 或 "23°C" 或 "23"
                digits = ''.join(filter(lambda c: c.isdigit() or c == '-', text))
                if digits:
                    return int(digits)
                return None
        except:
            return None

    async def collect(self) -> None:
        """执行一次天气熵采集，并混入池子"""
        if self._session is None:
            self._session = aiohttp.ClientSession()

        # 选择省份
        provinces = list(PROVINCE_CAPITALS.keys())
        if self.sample_count is not None and self.sample_count < len(provinces):
            import random
            selected = random.sample(provinces, self.sample_count)
        else:
            selected = provinces

        # 并发获取温度
        tasks = [self._fetch_temperature(PROVINCE_CAPITALS[p]) for p in selected]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤失败，组装字符串
        valid_pairs = []
        for p, temp in zip(selected, results):
            if isinstance(temp, int):
                valid_pairs.append(f"{p}{temp}")

        if valid_pairs:
            seed_str = "".join(valid_pairs)
            # 直接混入（含时间戳）
            self.pool.mix_event(f"weather:{seed_str}")

    async def start(self, interval: int = 600):
        """后台循环采集，interval 秒间隔"""
        self._running = True
        while self._running:
            try:
                await self.collect()
            except Exception as e:
                # 记录日志（在插件中通过logger输出）
                pass
            await asyncio.sleep(interval)

    def stop(self):
        self._running = False
        if self._session:
            asyncio.create_task(self._session.close())

# ======================================================================
# 4. 种子合成器
# ======================================================================
class SeedGenerator:
    @staticmethod
    def generate(pool: ChaosEntropyPool) -> bytes:
        sys_entropy = os.urandom(32)
        pool_hash = pool.digest()
        try:
            safe_ts = _get_safe_timestamp_bytes()
            time_hash = hashlib.sha256(safe_ts).digest()
        except:
            time_hash = os.urandom(32)
        return bytes(a ^ b ^ c for a, b, c in zip(sys_entropy, pool_hash, time_hash))