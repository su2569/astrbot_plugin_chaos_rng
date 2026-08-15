#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AstrBot 混沌随机数插件（增强版）
- 手动采集聊天上下文（加密哈希后混入）
- 进程物理熵、天气熵自动采集
- TCP 服务 0.0.0.0:18888
"""

import asyncio
import hashlib
import random
import time
from collections import deque
from typing import Optional, List

from astrbot.api.star import Star, Context
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api import logger

from .chaos_rng_core import (
    ChaosEntropyPool,
    ProcessSnapshotCollector,
    WeatherEntropyCollector,
    SeedGenerator
)

# ======================================================================
# 消息缓冲区（环形队列）
# ======================================================================
class MessageBuffer:
    """存储最近的消息，用于手动采集"""
    def __init__(self, maxlen: int = 200):
        self.buffer = deque(maxlen=maxlen)

    def add(self, message: str, group_id: str, user_id: str):
        """添加一条消息，忽略机器人自身的回复"""
        # 简单去重：如果最后一条相同内容则跳过（可选）
        self.buffer.append({
            'msg': message,
            'group': group_id,
            'user': user_id,
            'time': time.time()
        })

    def sample(self, n: int = 10) -> List[str]:
        """随机抽取最多 n 条不同消息的内容（去重基于内容+群）"""
        if not self.buffer:
            return []
        # 去重：使用 (群, 内容) 作为键
        seen = set()
        unique_items = []
        for item in self.buffer:
            key = (item['group'], item['msg'])
            if key not in seen:
                seen.add(key)
                unique_items.append(item['msg'])
        # 随机抽取
        if len(unique_items) <= n:
            return unique_items
        return random.sample(unique_items, n)

# ======================================================================
# LLM 流代理（保留，但可能劫持失败）
# ======================================================================
class LLMStreamProxy:
    def __init__(self, original_llm, pool: ChaosEntropyPool):
        self._llm = original_llm
        self.pool = pool

    def generate_stream(self, *args, **kwargs):
        raw_stream = self._llm.generate_stream(*args, **kwargs)

        async def wrapped():
            chunks = []
            async for chunk in raw_stream:
                chunks.append(chunk)
                yield chunk

            if chunks:
                full_text = "".join(chunks)
                total_len = len(full_text)
                if total_len >= 10:
                    start = int(total_len * 0.6)
                    end = int(total_len * 0.9)
                    if start < end:
                        target = full_text[start:end]
                        self.pool.mix_event(f"llm_mid:{target}_{start}_{total_len}")
                    else:
                        self.pool.mix_event(f"llm_full:{full_text[:100]}")
                else:
                    self.pool.mix_event(f"llm_short:{full_text}")

        return wrapped()

# ======================================================================
# 插件主类
# ======================================================================
class ChaosRNGPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        logger.info("🔥 混沌随机数插件初始化（手动采集版）...")

        self.pool = ChaosEntropyPool()
        self.proc_collector = ProcessSnapshotCollector(self.pool)
        self.proc_collector.scan_and_update()

        # 消息缓冲区
        self.msg_buffer = MessageBuffer(maxlen=200)

        # 天气采集器（随机5省，每10分钟）
        self.weather_collector = WeatherEntropyCollector(self.pool, sample_count=5)
        self.loop = asyncio.get_event_loop()

        # 劫持 LLM（如果可用）
        self.llm_hijacked = False
        if hasattr(context, 'llm') and context.llm:
            try:
                context.llm = LLMStreamProxy(context.llm, self.pool)
                self.llm_hijacked = True
                logger.info("✅ 已劫持 LLM 流")
            except Exception as e:
                logger.warning(f"LLM劫持失败: {e}")
        else:
            logger.warning("⚠️ 未找到 LLM，语义熵采集将仅依赖手动采集")

        # 启动后台任务
        self.daemon_task = self.loop.create_task(self._system_daemon())
        self.weather_task = self.loop.create_task(self._weather_loop())
        self.server_task = self.loop.create_task(self._start_seed_server())

        logger.info("🚀 插件就绪，TCP 服务监听 0.0.0.0:18888")
        logger.info("🌤️ 天气熵已启用（随机5省，采集间隔600秒）")
        logger.info("📝 手动采集命令：/rng_collect")

    # ---------- 消息事件钩子（无需装饰器） ----------
    async def on_message(self, event: AstrMessageEvent):
        """AstrBot 自动调用，存储所有消息（不包括机器人自身）"""
        # 判断是否是机器人自己发出的消息，避免循环
        if event.is_from_self():
            return
        user_id = event.get_sender_id()
        group_id = event.message_obj.group_id or "private"
        msg = event.message_str.strip()
        if msg:
            self.msg_buffer.add(msg, group_id, user_id)

    # ---------- 后台任务 ----------
    async def _system_daemon(self):
        while True:
            try:
                self.proc_collector.scan_and_update()
            except Exception as e:
                logger.error(f"进程采集异常: {e}")
            await asyncio.sleep(2.0)

    async def _weather_loop(self):
        await self.weather_collector.start(interval=600)

    # ---------- TCP 服务 ----------
    async def _start_seed_server(self):
        server = await asyncio.start_server(
            self._handle_client, '0.0.0.0', 18888
        )
        addr = server.sockets[0].getsockname()
        logger.info(f"🌐 种子服务监听: {addr[0]}:{addr[1]}")
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info('peername')
        self.pool.mix_event(f"pull:{client_addr[0]}:{client_addr[1]}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                cmd = data.decode().strip().upper()

                if cmd == 'GET_SEED':
                    seed = SeedGenerator.generate(self.pool)
                    writer.write(len(seed).to_bytes(4, 'big'))
                    writer.write(seed)
                    await writer.drain()
                elif cmd == 'GET_HEX':
                    seed = SeedGenerator.generate(self.pool)
                    writer.write((seed.hex() + '\n').encode())
                    await writer.drain()
                elif cmd == 'PING':
                    writer.write(b'PONG\n')
                    await writer.drain()
                else:
                    writer.write(b'ERROR: Commands: GET_SEED, GET_HEX, PING\n')
                    await writer.drain()
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    # ---------- 手动采集命令 ----------
    @filter.command("rng_collect")
    async def rng_collect(self, event: AstrMessageEvent):
        """手动采集聊天上下文（最多10条随机消息），加密哈希后混入熵池"""
        # 从缓冲区随机抽取最多10条消息（去重）
        messages = self.msg_buffer.sample(10)
        if not messages:
            yield event.plain_result("❌ 缓冲区暂无聊天记录，请稍后再试。")
            return
    
        # 固定加密种子（可自行修改）
        SECRET_KEY = "AstrBot_Chaos_Secret_2024"
        # 拼接所有消息
        combined = SECRET_KEY + "".join(messages)
        # SHA-256 哈希
        hash_bytes = hashlib.sha256(combined.encode('utf-8')).digest()
        # 转换为十六进制字符串
        hash_hex = hash_bytes.hex()
    
        # 混入熵池（带上时间戳，增加随机性）
        self.pool.mix_event(f"manual_collect:{hash_hex}")
    
        # 直接 yield 返回结果，不用 event.send()
        yield event.plain_result(
            f"✅ 已采集 **{len(messages)}** 条聊天消息并混入熵池。\n"
            f"哈希值（隐私保护）: `{hash_hex[:16]}...`"
        )

    # ---------- 其他命令 ----------
    @filter.command("rng_seed")
    async def rng_seed(self, event: AstrMessageEvent):
        seed = SeedGenerator.generate(self.pool)
        yield event.plain_result(f"🎲 **种子 (256bit)**:\n`{seed.hex()}`")

    @filter.command("rng_lucky")
    async def rng_lucky(self, event: AstrMessageEvent):
        seed = SeedGenerator.generate(self.pool)
        rng = random.Random(int.from_bytes(seed, 'big'))
        lucky = rng.randint(1, 100)
        yield event.plain_result(f"🍀 **幸运数字**: **{lucky}** / 100")

    @filter.command("rng_status")
    async def rng_status(self, event: AstrMessageEvent):
        status = (
            "📊 **随机数服务状态**\n"
            f"- 熵池哈希: `{self.pool.digest().hex()[:16]}...`\n"
            f"- TCP 服务: `0.0.0.0:18888` (运行中)\n"
            f"- LLM劫持: {'✅' if self.llm_hijacked else '❌'}\n"
            f"- 进程采集: 每2秒 (运行中)\n"
            f"- 天气熵源: 随机5省，每600秒更新\n"
            f"- 消息缓冲区: {len(self.msg_buffer.buffer)} 条记录\n"
            "🛡️ Y2K/时间回绕防御: 已启用"
        )
        yield event.plain_result(status)

    @filter.command("rng_weather_now")
    async def rng_weather_now(self, event: AstrMessageEvent):
        await event.send("🌤️ 正在采集全国天气熵...")
        await self.weather_collector.collect()
        yield event.plain_result("✅ 天气熵已更新")

    # ---------- 清理 ----------
    async def _cleanup(self):
        self.daemon_task.cancel()
        self.weather_collector.stop()
        self.weather_task.cancel()
        self.server_task.cancel()
        await asyncio.gather(
            self.daemon_task,
            self.weather_task,
            self.server_task,
            return_exceptions=True
        )
        logger.info("🛑 插件已卸载")