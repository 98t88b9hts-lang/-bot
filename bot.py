"""Discord 経済bot(銀行bot)  / discord.py 2.7 系対応

機能:
- ユーザーIDに紐づく残高 (SQLite)
- スラッシュコマンド: 残高確認 / 送金 / 給料確認
- ボタンパネル: 送金・残高確認・給料額確認(月毎)
- ロールに紐づく給料の設定と配布(毎月1日 JST に自動配布 + 手動配布)
- 銀行機能: 管理者が報酬を付与 / 回収、流通総額の確認
- 送金・給料・報酬・回収のたびに bot から DM で通知(金額・送金元・理由)
- ボイスチャンネルの Activity(client/ の画面)用 Web サーバーを同時に起動
- 管理者ロールの設定 / 請求書(DMに届く) / VC接続時間の計測・ロール自動付与と自動削除・報酬
- サーバーブースト時のロール自動付与と報酬

2つのbotに分けて使う(同じ bot.py・同じ DB を使うので、通貨は共通):
  py bot.py admin   … 旧経済bot(管理者だけが使うコマンド。月給配布・VC計測・ブーストなどの自動処理も担当)
  py bot.py member  … 新経済bot(全員が使うコマンド。残高・送金・月給確認・請求書など)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

import aiohttp
import discord
from aiohttp import web
from discord import app_commands
from discord.ext import tasks

# ---------------------------------------------------------------- 設定


def _load_dotenv(filename: str = ".env") -> None:
    """bot.py と同じフォルダの .env を読み込む(追加パッケージ不要)。"""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()
# どちらのbotとして起動するか: 「py bot.py admin」「py bot.py member」(省略すると admin = 旧経済bot)
MODE = next((a for a in sys.argv[1:2] if a in ("admin", "member")), os.environ.get("BOT_MODE", "admin")).strip()
if MODE not in ("admin", "member"):
    raise SystemExit("起動のしかたが正しくありません。「py bot.py admin」か「py bot.py member」で起動してください。")
TOKEN_NAME = "DISCORD_TOKEN" if MODE == "admin" else "MEMBER_DISCORD_TOKEN"
TOKEN = os.environ.get(TOKEN_NAME, "").strip()
if not TOKEN:
    raise SystemExit(
        f"{TOKEN_NAME} が設定されていません。\n"
        "bot.py と同じフォルダの .env ファイルに、次の1行を書いてください:\n"
        f"{TOKEN_NAME}=あなたのトークン"
    )
GUILD_ID = os.environ.get("GUILD_ID")  # 開発用(指定するとそのサーバーのみ。どこでも使うには未設定にする)
DB_PATH = os.environ.get("DB_PATH", "economy.db")
CURRENCY = "コイン"
BANK_NAME = "銀行(bot)"
BOT_VERSION = "2026-10-18 VC操作パネル版"  # 起動したときに表示する。新しい bot.py で動いているかの確認用
TZ = ZoneInfo("Asia/Tokyo")
MAX_AMOUNT = 10**12
# Activity 用(Developer Portal の「一般情報」「OAuth2」にあります。未設定なら Activity は無効)
CLIENT_ID = os.environ.get("DISCORD_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("DISCORD_CLIENT_SECRET", "").strip()
WEB_PORT = int(os.environ.get("WEB_PORT", "8080"))

log = logging.getLogger("economy")


# ---------------------------------------------------------------- DB
class Database:
    """標準ライブラリの sqlite3 を使用(追加パッケージ不要)。
    処理は to_thread で実行してイベントループを止めず、スレッドロックで直列化する。"""

    def __init__(self, path: str):
        self.path = path
        self.conn: sqlite3.Connection | None = None
        self._tlock = threading.Lock()

    # ---- 接続
    def _connect(self) -> None:
        # DB_PATH の書き間違いを、わかりやすく知らせる(間違った場所に空のDBを作ってしまうと、通貨が別々になってしまう)
        folder = os.path.dirname(os.path.abspath(self.path))
        if not os.path.isdir(folder):
            raise SystemExit(
                f"DB_PATH のフォルダが見つかりません: {self.path}\n"
                "旧経済bot(admin)の economy.db がある場所を、.env の DB_PATH に正しく書いてください。(\\ ではなく / で書く)"
            )
        if MODE == "member" and not os.path.exists(self.path):
            raise SystemExit(
                f"economy.db が見つかりません: {self.path}\n"
                "旧経済bot(admin)が使っている economy.db と同じファイルを、.env の DB_PATH に書いてください。\n"
                "(違う場所に新しく作ってしまうと、残高が別々になってしまうため、起動を止めました)"
            )
        # 2つのbot(と、カジノbot)が同じファイルを使うので、待ち時間(timeout)を長めにしておく
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS balances (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0 CHECK (balance >= 0)
            );
            CREATE TABLE IF NOT EXISTS role_salaries (
                guild_id INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                amount   INTEGER NOT NULL CHECK (amount >= 0),
                PRIMARY KEY (guild_id, role_id)
            );
            CREATE TABLE IF NOT EXISTS guild_state (
                guild_id INTEGER PRIMARY KEY,
                last_paid_month TEXT
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                from_id INTEGER,          -- NULL = 銀行
                to_id INTEGER,            -- NULL = 銀行
                amount INTEGER NOT NULL,
                kind TEXT NOT NULL,
                reason TEXT
            );
            -- 管理者ロール(このロールを持つ人は管理者コマンドが使える)
            CREATE TABLE IF NOT EXISTS admin_roles (
                guild_id INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                PRIMARY KEY (guild_id, role_id)
            );
            -- 請求書 (status: pending / paid / declined / cancelled)
            -- due_date = 支払い期限(YYYY-MM-DD・日本時間。なければ NULL) / due_notified = 期限日のお知らせを送ったか(0/1)
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issuer_id INTEGER NOT NULL,
                payer_id  INTEGER NOT NULL,
                amount    INTEGER NOT NULL,
                reason    TEXT,
                status    TEXT NOT NULL,
                created_at TEXT NOT NULL,
                closed_at  TEXT,
                due_date TEXT,
                due_notified INTEGER NOT NULL DEFAULT 0
            );
            -- VC時間を計測するチャンネル
            CREATE TABLE IF NOT EXISTS vc_channels (
                guild_id   INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                PRIMARY KEY (guild_id, channel_id)
            );
            -- VC接続時間の合計(秒)
            CREATE TABLE IF NOT EXISTS vc_time (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                seconds  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            );
            -- VC時間ロールのルール(minutes分で付与 / expire_minutes分後に削除(0=削除しない) / 付与時の報酬)
            CREATE TABLE IF NOT EXISTS vc_rules (
                guild_id INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                minutes  INTEGER NOT NULL,
                expire_minutes INTEGER NOT NULL DEFAULT 0,
                reward   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, role_id)
            );
            -- VC時間ロールの付与記録(同じロールを二重に付与・報酬しないため)
            CREATE TABLE IF NOT EXISTS vc_grants (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                granted_at INTEGER NOT NULL,
                expires_at INTEGER,
                removed  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id, role_id)
            );
            -- VC接続時間(/ボイス時間)を確認できるロール(1つも無ければ全員が確認できる)
            CREATE TABLE IF NOT EXISTS vc_view_roles (
                guild_id INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                PRIMARY KEY (guild_id, role_id)
            );
            -- ブースト設定
            CREATE TABLE IF NOT EXISTS boost_settings (
                guild_id INTEGER PRIMARY KEY,
                role_id  INTEGER,
                reward   INTEGER NOT NULL DEFAULT 0
            );
            -- 銀行bot(旧・新)のユーザーID。起動したときに、自分のIDを書き込む。(/送金 で、どれが銀行botか見分けるため)
            CREATE TABLE IF NOT EXISTS bank_bots (
                mode TEXT PRIMARY KEY,   -- admin(旧経済bot) / member(新経済bot)
                user_id INTEGER NOT NULL
            );
            -- VC作成パネル (kind: dorm=寮VC / secret=シクレVC / arcade=VC作成)
            -- courses = VC作成のコース一覧(JSON。[["コース名", 金額], ...])
            CREATE TABLE IF NOT EXISTS vc_panels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                price INTEGER NOT NULL DEFAULT 0,
                category_id INTEGER,
                courses TEXT
            );
            -- パネルのボタンで作られたVCの記録
            CREATE TABLE IF NOT EXISTS vc_rooms (
                channel_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                price INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at INTEGER   -- 時間制限の終了時刻(UNIX秒)。制限なしは NULL
            );
            """
        )
        # 以前のバージョンで作ったDBには、期限の列がないので、あとから追加する
        invoice_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(invoices)")}
        if "due_date" not in invoice_columns:
            self.conn.execute("ALTER TABLE invoices ADD COLUMN due_date TEXT")
        if "due_notified" not in invoice_columns:
            self.conn.execute("ALTER TABLE invoices ADD COLUMN due_notified INTEGER NOT NULL DEFAULT 0")
        room_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(vc_rooms)")}
        if "expires_at" not in room_columns:
            self.conn.execute("ALTER TABLE vc_rooms ADD COLUMN expires_at INTEGER")
        self.conn.commit()

    async def connect(self) -> None:
        await asyncio.to_thread(self._connect)

    async def close(self) -> None:
        if self.conn:
            await asyncio.to_thread(self.conn.close)

    # ---- 残高
    def _get_balance(self, user_id: int) -> int:
        with self._tlock:
            row = self.conn.execute(
                "SELECT balance FROM balances WHERE user_id = ?", (user_id,)
            ).fetchone()
        return row[0] if row else 0

    async def get_balance(self, user_id: int) -> int:
        return await asyncio.to_thread(self._get_balance, user_id)

    def _total_supply(self) -> tuple[int, int]:
        with self._tlock:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(balance), 0), COUNT(*) FROM balances"
            ).fetchone()
        return row[0], row[1]

    async def total_supply(self) -> tuple[int, int]:
        return await asyncio.to_thread(self._total_supply)

    def _move(
        self,
        from_id: int | None,
        to_id: int | None,
        amount: int,
        kind: str,
        reason: str | None,
    ) -> tuple[bool, int | None, int | None]:
        def bal(uid: int) -> int:
            row = self.conn.execute(
                "SELECT balance FROM balances WHERE user_id = ?", (uid,)
            ).fetchone()
            return row[0] if row else 0

        with self._tlock:
            try:
                for uid in (from_id, to_id):
                    if uid is not None:
                        self.conn.execute(
                            "INSERT OR IGNORE INTO balances (user_id, balance) VALUES (?, 0)",
                            (uid,),
                        )
                if from_id is not None:
                    cur = self.conn.execute(
                        "UPDATE balances SET balance = balance - ? "
                        "WHERE user_id = ? AND balance >= ?",
                        (amount, from_id, amount),
                    )
                    if cur.rowcount == 0:
                        self.conn.rollback()
                        return False, bal(from_id), None
                if to_id is not None:
                    self.conn.execute(
                        "UPDATE balances SET balance = balance + ? WHERE user_id = ?",
                        (amount, to_id),
                    )
                self.conn.execute(
                    "INSERT INTO transactions (ts, from_id, to_id, amount, kind, reason) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (datetime.now(TZ).isoformat(), from_id, to_id, amount, kind, reason),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            return (
                True,
                bal(from_id) if from_id is not None else None,
                bal(to_id) if to_id is not None else None,
            )

    async def move(
        self,
        from_id: int | None,
        to_id: int | None,
        amount: int,
        kind: str,
        reason: str | None,
    ) -> tuple[bool, int | None, int | None]:
        """残高を移動する。from_id / to_id が None の側は銀行。
        戻り値: (成功, 送金元の新残高, 送金先の新残高)"""
        return await asyncio.to_thread(self._move, from_id, to_id, amount, kind, reason)

    # ---- 履歴
    def _history(self, user_id: int, limit: int) -> list[tuple]:
        with self._tlock:
            return self.conn.execute(
                "SELECT ts, from_id, to_id, amount, kind, reason FROM transactions "
                "WHERE from_id = ? OR to_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, user_id, limit),
            ).fetchall()

    async def history(self, user_id: int, limit: int = 10) -> list[tuple]:
        return await asyncio.to_thread(self._history, user_id, limit)

    # ---- 給料
    def _set_salary(self, guild_id: int, role_id: int, amount: int, month: str) -> None:
        with self._tlock:
            self.conn.execute(
                "INSERT INTO role_salaries (guild_id, role_id, amount) VALUES (?, ?, ?) "
                "ON CONFLICT(guild_id, role_id) DO UPDATE SET amount = excluded.amount",
                (guild_id, role_id, amount),
            )
            # 設定した月の自動配布は行わない(初回は翌月1日から)
            self.conn.execute(
                "INSERT OR IGNORE INTO guild_state (guild_id, last_paid_month) VALUES (?, ?)",
                (guild_id, month),
            )
            self.conn.commit()

    async def set_salary(self, guild_id: int, role_id: int, amount: int) -> None:
        await asyncio.to_thread(self._set_salary, guild_id, role_id, amount, month_key())

    def _remove_salary(self, guild_id: int, role_id: int) -> bool:
        with self._tlock:
            cur = self.conn.execute(
                "DELETE FROM role_salaries WHERE guild_id = ? AND role_id = ?",
                (guild_id, role_id),
            )
            self.conn.commit()
            return cur.rowcount > 0

    async def remove_salary(self, guild_id: int, role_id: int) -> bool:
        return await asyncio.to_thread(self._remove_salary, guild_id, role_id)

    def _salaries(self, guild_id: int) -> dict[int, int]:
        with self._tlock:
            rows = self.conn.execute(
                "SELECT role_id, amount FROM role_salaries WHERE guild_id = ?", (guild_id,)
            ).fetchall()
        return {r: a for r, a in rows}

    async def salaries(self, guild_id: int) -> dict[int, int]:
        return await asyncio.to_thread(self._salaries, guild_id)

    def _last_paid_month(self, guild_id: int) -> str | None:
        with self._tlock:
            row = self.conn.execute(
                "SELECT last_paid_month FROM guild_state WHERE guild_id = ?", (guild_id,)
            ).fetchone()
        return row[0] if row else None

    async def last_paid_month(self, guild_id: int) -> str | None:
        return await asyncio.to_thread(self._last_paid_month, guild_id)

    def _mark_paid(self, guild_id: int, month: str) -> None:
        with self._tlock:
            self.conn.execute(
                "INSERT INTO guild_state (guild_id, last_paid_month) VALUES (?, ?) "
                "ON CONFLICT(guild_id) DO UPDATE SET last_paid_month = excluded.last_paid_month",
                (guild_id, month),
            )
            self.conn.commit()

    async def mark_paid(self, guild_id: int, month: str) -> None:
        await asyncio.to_thread(self._mark_paid, guild_id, month)

    # ---- 汎用(SQLを直接渡して使う。新機能はこれを使用)
    def _execute(self, sql: str, params: tuple) -> tuple[int, int | None]:
        with self._tlock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur.rowcount, cur.lastrowid

    async def execute(self, sql: str, params: tuple = ()) -> tuple[int, int | None]:
        """INSERT / UPDATE / DELETE を実行する。戻り値: (変更された行数, 追加された行のID)"""
        return await asyncio.to_thread(self._execute, sql, params)

    def _query(self, sql: str, params: tuple) -> list[tuple]:
        with self._tlock:
            return self.conn.execute(sql, params).fetchall()

    async def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        """SELECT を実行して、結果の行のリストを返す。"""
        return await asyncio.to_thread(self._query, sql, params)


# ---------------------------------------------------------------- ユーティリティ
def month_key() -> str:
    return datetime.now(TZ).strftime("%Y-%m")


def next_payday() -> str:
    now = datetime.now(TZ)
    y, m = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
    return f"{y}年{m}月1日 00:00 (JST)"


def fmt(amount: int) -> str:
    return f"{amount:,} {CURRENCY}"


DUE_NOTICE_HOUR = 9  # 期限日のお知らせDMを送る時刻(日本時間の何時か)


def now_jst() -> datetime:
    return datetime.now(TZ)


def parse_due_date(text: str, today: date) -> date:
    """「2026/10/31」「10/31」「10月31日」などを日付にする。使えない入力なら ValueError(理由つき)。
    年を省略して、その日がもう過ぎていたら、来年の日付として読む。"""
    t = text.strip().translate(str.maketrans("０１２３４５６７８９／－．", "0123456789/-."))
    m = re.fullmatch(r"(?:(\d{4})\s*[/\-.年]\s*)?(\d{1,2})\s*[/\-.月]\s*(\d{1,2})\s*日?", t)
    if not m:
        raise ValueError("期限の書き方が正しくありません。例: 2026/10/31 ・ 10/31 ・ 10月31日")
    year_text, month, day = m.groups()
    year = int(year_text) if year_text else today.year
    try:
        due = date(year, int(month), int(day))
        if not year_text and due < today:
            due = date(year + 1, int(month), int(day))
    except ValueError:
        raise ValueError("存在しない日付です。もう一度確認してください。") from None
    if due < today:
        raise ValueError("期限は、今日以降の日付にしてください。")
    if due > today + timedelta(days=365):
        raise ValueError("期限は、1年以内の日付にしてください。")
    return due


def due_text(due_date: str | None, status: str) -> str:
    """請求書に表示する期限の文字。(請求中のものだけ、残り日数を付ける)"""
    if not due_date:
        return "なし"
    due = date.fromisoformat(due_date)
    base = f"{due.year}年{due.month}月{due.day}日"
    if status != "pending":
        return base
    diff = (due - now_jst().date()).days
    if diff > 0:
        return f"{base}(あと{diff}日)"
    if diff == 0:
        return f"{base}(今日まで)"
    return f"{base}(期限切れ)"


def member_salary(member: discord.Member, salaries: dict[int, int]) -> int:
    """所持している全ロールの給料の合計"""
    return sum(salaries.get(role.id, 0) for role in member.roles)


async def reply(
    interaction: discord.Interaction,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
) -> None:
    kwargs = {"content": content, "embed": embed, "ephemeral": True}
    if view is not None:
        kwargs["view"] = view
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    if interaction.response.is_done():
        await interaction.followup.send(**kwargs)
    else:
        await interaction.response.send_message(**kwargs)


async def send_dm(bot: "EconomyBot", user_id: int, **kwargs) -> bool:
    """ユーザーにDMを送る。送れたら True。(embed=... や view=... を渡せる)"""
    try:
        user = bot.get_user(user_id) or await bot.fetch_user(user_id)
        await user.send(**kwargs)
        return True
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        log.info("DM を送れませんでした: %s", user_id)
        return False


async def user_label(bot: "EconomyBot", user_id: int) -> str:
    """「表示名 (@メンション)」の形の文字列を返す。"""
    try:
        user = bot.get_user(user_id) or await bot.fetch_user(user_id)
        return f"{user.display_name} (<@{user_id}>)"
    except (discord.NotFound, discord.HTTPException):
        return f"<@{user_id}>"


def role_problem(guild: discord.Guild, role: discord.Role) -> str:
    """botがそのロールを付け外しできない理由を返す。問題なければ空文字。"""
    me = guild.me
    if role.is_default() or role.managed:
        return "⚠️ このロール(@everyone・連携用ロール)は付け外しできません。"
    if not me.guild_permissions.manage_roles:
        return "⚠️ botに「ロールの管理」権限がありません。サーバー設定→ロールでbotのロールに権限を付けてください。"
    if role >= me.top_role:
        return "⚠️ botのロールより上(または同じ)順位のロールは付け外しできません。サーバー設定→ロールで、botのロールを上にドラッグしてください。"
    return ""


async def notify_dm(
    bot: "EconomyBot",
    user_id: int,
    *,
    title: str,
    amount: int,
    sender: str,
    reason: str | None,
    balance: int,
    color: discord.Color,
) -> None:
    embed = discord.Embed(title=title, color=color, timestamp=discord.utils.utcnow())
    embed.add_field(name="金額", value=fmt(amount), inline=False)
    embed.add_field(name="送金元", value=sender, inline=False)
    embed.add_field(name="理由", value=reason or "(なし)", inline=False)
    embed.add_field(name="現在の残高", value=fmt(balance), inline=False)
    try:
        user = bot.get_user(user_id) or await bot.fetch_user(user_id)
        await user.send(embed=embed)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        log.info("DM を送れませんでした: %s", user_id)


def balance_embed(user: discord.abc.User, balance: int) -> discord.Embed:
    embed = discord.Embed(title="💰 残高", color=discord.Color.gold())
    embed.add_field(name=user.display_name, value=fmt(balance))
    return embed


async def salary_embed(bot: "EconomyBot", member: discord.Member) -> discord.Embed:
    salaries = await bot.db.salaries(member.guild.id)
    lines, total = [], 0
    for role in member.roles:
        amt = salaries.get(role.id)
        if amt:
            lines.append(f"{role.mention}: {fmt(amt)}")
            total += amt
    embed = discord.Embed(title="📅 月給の確認", color=discord.Color.green())
    embed.description = "\n".join(lines) if lines else "給料が設定されたロールを持っていません。"
    embed.add_field(name="月給合計", value=fmt(total), inline=False)
    embed.add_field(name="次回支給日", value=next_payday(), inline=False)
    return embed


# ---------------------------------------------------------------- 送金処理(コマンド/ボタン共通)
async def is_bank_bot(bot: "EconomyBot", user_id: int) -> bool:
    """銀行bot(旧経済bot・新経済bot)のどちらかなら True。"""
    if bot.user is not None and user_id == bot.user.id:
        return True
    rows = await bot.db.query("SELECT 1 FROM bank_bots WHERE user_id = ?", (user_id,))
    return bool(rows)


async def do_transfer(
    bot: "EconomyBot",
    interaction: discord.Interaction,
    target: discord.abc.User,
    amount: int,
    reason: str | None,
) -> None:
    sender = interaction.user
    to_bank = False  # 銀行botへの送金か
    if target.bot:
        if not await is_bank_bot(bot, target.id):
            return await reply(interaction, "銀行bot以外のbotには送金できません。")
        to_bank = True
    if target.id == sender.id:
        return await reply(interaction, "自分自身には送金できません。")
    if not 1 <= amount <= MAX_AMOUNT:
        return await reply(interaction, f"金額は 1〜{MAX_AMOUNT:,} で指定してください。")

    if to_bank:  # 銀行botへの送金: 銀行にお金が戻る(送金元の残高が減るだけ)
        ok, from_bal, _ = await bot.db.move(sender.id, None, amount, "deposit", reason or "銀行への送金")
        if not ok:
            return await reply(interaction, f"残高が足りません。(現在 {fmt(from_bal or 0)})")
        embed = discord.Embed(title="✅ 銀行への送金完了", color=discord.Color.blue())
        embed.add_field(name="送金先", value=f"{BANK_NAME} ({target.mention})", inline=False)
        embed.add_field(name="金額", value=fmt(amount), inline=False)
        embed.add_field(name="理由", value=reason or "(なし)", inline=False)
        embed.add_field(name="あなたの残高", value=fmt(from_bal), inline=False)
        return await reply(interaction, embed=embed)

    ok, from_bal, to_bal = await bot.db.move(sender.id, target.id, amount, "transfer", reason)
    if not ok:
        return await reply(interaction, f"残高が足りません。(現在 {fmt(from_bal or 0)})")

    embed = discord.Embed(title="✅ 送金完了", color=discord.Color.blue())
    embed.add_field(name="送金先", value=target.mention, inline=False)
    embed.add_field(name="金額", value=fmt(amount), inline=False)
    embed.add_field(name="理由", value=reason or "(なし)", inline=False)
    embed.add_field(name="あなたの残高", value=fmt(from_bal), inline=False)
    await reply(interaction, embed=embed)

    await notify_dm(
        bot, target.id,
        title="💸 送金を受け取りました",
        amount=amount,
        sender=f"{sender.display_name} ({sender.mention})",
        reason=reason,
        balance=to_bal,
        color=discord.Color.blue(),
    )


# ---------------------------------------------------------------- ボタンUI
class TransferModal(discord.ui.Modal):
    amount = discord.ui.TextInput(label="金額", placeholder="例: 1000", max_length=13)
    reason = discord.ui.TextInput(
        label="理由(任意)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=100,
    )

    def __init__(self, bot: "EconomyBot", target: discord.abc.User):
        super().__init__(title=f"{target.display_name} へ送金"[:45])
        self.bot = bot
        self.target = target

    async def on_submit(self, interaction: discord.Interaction) -> None:
        raw = self.amount.value.replace(",", "").strip()
        if not raw.isdigit():
            return await reply(interaction, "金額は半角数字で入力してください。")
        await do_transfer(
            self.bot, interaction, self.target, int(raw), self.reason.value.strip() or None
        )


class TransferSelectView(discord.ui.View):
    def __init__(self, bot: "EconomyBot"):
        super().__init__(timeout=120)
        self.bot = bot

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="送金先を選択してください",
        min_values=1,
        max_values=1,
    )
    async def pick(self, interaction: discord.Interaction, select: discord.ui.UserSelect):
        await interaction.response.send_modal(TransferModal(self.bot, select.values[0]))


class PanelView(discord.ui.View):
    """再起動後も動く常設パネル(timeout=None + custom_id)"""

    def __init__(self, bot: "EconomyBot"):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="残高確認", emoji="💰", style=discord.ButtonStyle.primary, custom_id="eco:balance")
    async def balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        bal = await self.bot.db.get_balance(interaction.user.id)
        await reply(interaction, embed=balance_embed(interaction.user, bal))

    @discord.ui.button(label="送金", emoji="💸", style=discord.ButtonStyle.success, custom_id="eco:transfer")
    async def transfer(self, interaction: discord.Interaction, button: discord.ui.Button):
        await reply(interaction, "送金先を選んでください。", view=TransferSelectView(self.bot))

    @discord.ui.button(label="月給確認", emoji="📅", style=discord.ButtonStyle.secondary, custom_id="eco:salary")
    async def salary(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member):
            return await reply(interaction, "サーバー内で使用してください。")
        await reply(interaction, embed=await salary_embed(self.bot, interaction.user))


# ---------------------------------------------------------------- Activity 用 Web サーバー
STATIC_DIR = Path(__file__).resolve().parent / "client" / "dist"
DISCORD_API = "https://discord.com/api/v10"
KIND_LABELS = {"transfer": "送金", "salary": "給料", "reward": "報酬", "collect": "回収"}
_token_cache: dict[str, tuple[int, float]] = {}  # アクセストークン -> (ユーザーID, 有効期限)
_bg_tasks: set[asyncio.Task] = set()


def json_error(message: str, status: int = 400) -> web.Response:
    return web.json_response({"error": message}, status=status)


def run_in_background(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def current_user_id(request: web.Request) -> int | None:
    """Authorization: Bearer <token> を Discord に問い合わせて、本人のユーザーIDを返す。"""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    now = time.time()
    cached = _token_cache.get(token)
    if cached and cached[1] > now:
        return cached[0]
    bot: EconomyBot = request.app["bot"]
    async with bot.http_session.get(
        f"{DISCORD_API}/users/@me", headers={"Authorization": f"Bearer {token}"}
    ) as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
    user_id = int(data["id"])
    if len(_token_cache) > 1000:
        for key in [k for k, (_, exp) in _token_cache.items() if exp <= now]:
            _token_cache.pop(key, None)
    _token_cache[token] = (user_id, now + 300)
    return user_id


async def find_user(bot: "EconomyBot", user_id: int) -> discord.User | None:
    try:
        return bot.get_user(user_id) or await bot.fetch_user(user_id)
    except (discord.NotFound, discord.HTTPException):
        return None


def name_of(bot: "EconomyBot", user_id: int | None) -> str:
    if user_id is None:
        return BANK_NAME
    user = bot.get_user(user_id)
    return user.display_name if user else "不明なユーザー"


async def api_config(request: web.Request) -> web.Response:
    return web.json_response({"client_id": CLIENT_ID, "currency": CURRENCY, "bank": BANK_NAME})


async def api_token(request: web.Request) -> web.Response:
    """Activity の認可コードをアクセストークンに交換する(クライアントシークレットはここだけで使う)。"""
    try:
        code = (await request.json())["code"]
    except Exception:
        return json_error("コードがありません。")
    bot: EconomyBot = request.app["bot"]
    async with bot.http_session.post(
        f"{DISCORD_API}/oauth2/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
        },
    ) as resp:
        data = await resp.json()
        if resp.status != 200 or "access_token" not in data:
            log.warning("トークン交換に失敗: %s %s", resp.status, data)
            return json_error("ログインに失敗しました。クライアントシークレットを確認してください。", 400)
    return web.json_response({"access_token": data["access_token"]})


async def api_me(request: web.Request) -> web.Response:
    user_id = await current_user_id(request)
    if user_id is None:
        return json_error("認証に失敗しました。", 401)
    bot: EconomyBot = request.app["bot"]
    rows = await bot.db.history(user_id, 10)
    history = []
    for ts, from_id, to_id, amount, kind, reason in rows:
        outgoing = from_id == user_id
        history.append(
            {
                "ts": ts,
                "kind": KIND_LABELS.get(kind, kind),
                "direction": "out" if outgoing else "in",
                "amount": amount,
                "counterpart": name_of(bot, to_id if outgoing else from_id),
                "reason": reason or "",
            }
        )
    return web.json_response({"balance": await bot.db.get_balance(user_id), "history": history})


async def api_members(request: web.Request) -> web.Response:
    user_id = await current_user_id(request)
    if user_id is None:
        return json_error("認証に失敗しました。", 401)
    bot: EconomyBot = request.app["bot"]
    guild = None
    try:
        guild = bot.get_guild(int(request.query.get("guild_id", "")))
    except ValueError:
        pass
    if guild is None:
        return web.json_response({"members": []})
    query = request.query.get("q", "").strip().lower()
    found = []
    for member in sorted(guild.members, key=lambda m: m.display_name.lower()):
        if member.bot or member.id == user_id:
            continue
        if query and query not in member.display_name.lower() and query not in member.name.lower():
            continue
        found.append(
            {
                "id": str(member.id),
                "name": member.display_name,
                "avatar": member.display_avatar.with_size(64).url,
            }
        )
        if len(found) >= 20:
            break
    return web.json_response({"members": found})


async def api_salary(request: web.Request) -> web.Response:
    user_id = await current_user_id(request)
    if user_id is None:
        return json_error("認証に失敗しました。", 401)
    bot: EconomyBot = request.app["bot"]
    empty = {"roles": [], "total": 0, "next_payday": next_payday()}
    try:
        guild = bot.get_guild(int(request.query.get("guild_id", "")))
    except ValueError:
        guild = None
    if guild is None:
        return web.json_response({**empty, "note": "サーバー内のアクティビティで確認できます。"})
    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except (discord.NotFound, discord.HTTPException):
            return web.json_response(empty)
    salaries = await bot.db.salaries(guild.id)
    roles = [
        {"name": role.name, "amount": salaries[role.id]}
        for role in member.roles
        if salaries.get(role.id)
    ]
    return web.json_response(
        {"roles": roles, "total": sum(r["amount"] for r in roles), "next_payday": next_payday()}
    )


async def api_transfer(request: web.Request) -> web.Response:
    user_id = await current_user_id(request)
    if user_id is None:
        return json_error("認証に失敗しました。", 401)
    try:
        body = await request.json()
        to_id = int(body["to_id"])
        amount = int(body["amount"])
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return json_error("入力内容が正しくありません。")
    reason = str(body.get("reason") or "").strip()[:100] or None
    if not 1 <= amount <= MAX_AMOUNT:
        return json_error(f"金額は 1〜{MAX_AMOUNT:,} で指定してください。")
    if to_id == user_id:
        return json_error("自分自身には送金できません。")

    bot: EconomyBot = request.app["bot"]
    target = await find_user(bot, to_id)
    if target is None:
        return json_error("送金先が見つかりません。")
    if target.bot:
        return json_error("botには送金できません。(銀行botへの送金は /送金 コマンドで行ってください)")

    ok, from_bal, to_bal = await bot.db.move(user_id, to_id, amount, "transfer", reason)
    if not ok:
        return json_error(f"残高が足りません。(現在 {fmt(from_bal or 0)})")

    sender = await find_user(bot, user_id)
    sender_label = f"{sender.display_name} ({sender.mention})" if sender else f"<@{user_id}>"
    run_in_background(
        notify_dm(
            bot, to_id,
            title="💸 送金を受け取りました",
            amount=amount,
            sender=sender_label,
            reason=reason,
            balance=to_bal,
            color=discord.Color.blue(),
        )
    )
    return web.json_response(
        {
            "balance": from_bal,
            "message": f"{target.display_name} さんに {fmt(amount)} を送金しました。",
        }
    )


async def index_page(request: web.Request) -> web.StreamResponse:
    index = STATIC_DIR / "index.html"
    if index.exists():
        return web.FileResponse(index)
    return web.Response(
        text="client/dist がありません。client フォルダで npm install と npm run build を実行してください。",
        status=503,
    )


async def start_web(bot: "EconomyBot") -> None:
    if not (CLIENT_ID and CLIENT_SECRET):
        log.info("Activity は無効です(DISCORD_CLIENT_ID / DISCORD_CLIENT_SECRET が未設定)")
        return
    bot.http_session = aiohttp.ClientSession()
    app = web.Application()
    app["bot"] = bot
    app.add_routes(
        [
            web.get("/api/config", api_config),
            web.post("/api/token", api_token),
            web.get("/api/me", api_me),
            web.get("/api/members", api_members),
            web.get("/api/salary", api_salary),
            web.post("/api/transfer", api_transfer),
            web.get("/", index_page),
        ]
    )
    if (STATIC_DIR / "assets").is_dir():
        app.router.add_static("/assets", STATIC_DIR / "assets")
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", WEB_PORT).start()
    bot.web_runner = runner
    log.info("Activity 用 Web サーバー起動: http://localhost:%s", WEB_PORT)


async def stop_web(bot: "EconomyBot") -> None:
    if bot.web_runner:
        await bot.web_runner.cleanup()
    if bot.http_session:
        await bot.http_session.close()


# ---------------------------------------------------------------- 請求書
INVOICE_STATUS = {
    "pending": "請求中",
    "paid": "支払い済み",
    "declined": "拒否されました",  # 以前のバージョンで拒否された請求書の表示用(いまは拒否ボタンはない)
    "cancelled": "取り消されました",
}
INVOICE_COLOR = {
    "pending": discord.Color.orange(),
    "paid": discord.Color.green(),
    "declined": discord.Color.red(),
    "cancelled": discord.Color.light_grey(),
}


async def load_invoice(bot: "EconomyBot", invoice_id: int) -> tuple | None:
    """(番号, 請求元, 請求先, 金額, 理由, 状態, 期限) の順。"""
    rows = await bot.db.query(
        "SELECT id, issuer_id, payer_id, amount, reason, status, due_date FROM invoices WHERE id = ?",
        (invoice_id,),
    )
    return rows[0] if rows else None


async def invoice_embed(bot: "EconomyBot", invoice: tuple) -> discord.Embed:
    inv_id, issuer_id, payer_id, amount, reason, status, due_date = invoice
    embed = discord.Embed(
        title=f"🧾 請求書 #{inv_id}",
        color=INVOICE_COLOR[status],
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="請求元", value=await user_label(bot, issuer_id), inline=False)
    embed.add_field(name="請求先", value=await user_label(bot, payer_id), inline=False)
    embed.add_field(name="金額", value=fmt(amount), inline=False)
    embed.add_field(name="理由", value=reason or "(なし)", inline=False)
    if due_date:
        embed.add_field(name="支払い期限", value=due_text(due_date, status), inline=False)
    embed.add_field(name="状態", value=INVOICE_STATUS[status], inline=False)
    return embed


async def settle_invoice(
    bot: "EconomyBot", user: discord.abc.User, invoice_id: int, action: str = "pay"
) -> tuple[bool, str]:
    """請求書を支払う。戻り値: (成功したか, メッセージ)
    (以前は「拒否」もできたが、いまは「期限延長申請」に変わったので、支払いだけ。action は互換のために残してある)"""
    invoice = await load_invoice(bot, invoice_id)
    if invoice is None:
        return False, "その請求書は見つかりません。"
    _, issuer_id, payer_id, amount, reason, status, _due = invoice
    if user.id != payer_id:
        return False, "この請求書の請求先ではありません。"
    if status != "pending":
        return False, f"この請求書はすでに処理されています({INVOICE_STATUS[status]})。"

    # 先に状態を変えて、ボタンの連打による二重支払いを防ぐ
    changed, _ = await bot.db.execute(
        "UPDATE invoices SET status = 'paid', closed_at = ? WHERE id = ? AND status = 'pending'",
        (datetime.now(TZ).isoformat(), invoice_id),
    )
    if changed == 0:
        return False, "この請求書はすでに処理されています。"

    payer_name = await user_label(bot, payer_id)
    note = f"請求書 #{invoice_id}" + (f": {reason}" if reason else "")
    ok, from_bal, to_bal = await bot.db.move(payer_id, issuer_id, amount, "invoice", note)
    if not ok:
        # 残高不足なので、請求中に戻す
        await bot.db.execute(
            "UPDATE invoices SET status = 'pending', closed_at = NULL WHERE id = ?", (invoice_id,)
        )
        return False, f"残高が足りません。(現在 {fmt(from_bal or 0)})"
    await notify_dm(
        bot, issuer_id,
        title=f"🧾 請求書 #{invoice_id} が支払われました",
        amount=amount,
        sender=payer_name,
        reason=reason,
        balance=to_bal,
        color=discord.Color.green(),
    )
    return True, f"請求書 #{invoice_id} を支払いました。(残高 {fmt(from_bal)})"


def extension_problem(invoice: tuple | None, user: discord.abc.User) -> str:
    """期限延長申請ができない理由を返す。できるなら空文字。"""
    if invoice is None:
        return "その請求書は見つかりません。"
    _, _, payer_id, _, _, status, due_date = invoice
    if user.id != payer_id:
        return "この請求書の請求先ではありません。"
    if status != "pending":
        return f"この請求書はすでに処理されています({INVOICE_STATUS[status]})。"
    if not due_date:
        return "この請求書には期限がないため、期限延長申請はできません。"
    return ""


async def request_extension(
    bot: "EconomyBot", user: discord.abc.User, invoice_id: int, reason: str
) -> tuple[bool, str]:
    """期限延長申請を、請求元にDMで送る。理由は必須。戻り値: (成功したか, メッセージ)"""
    reason = reason.strip()
    if not reason:
        return False, "理由を入力してください。(理由がないと、期限延長申請はできません)"
    invoice = await load_invoice(bot, invoice_id)
    problem = extension_problem(invoice, user)
    if problem:
        return False, problem
    _, issuer_id, payer_id, amount, inv_reason, status, due_date = invoice
    embed = discord.Embed(
        title="📩 期限延長申請がきました",
        description=f"請求書 #{invoice_id} の支払い期限を延ばしてほしい、という申請です。",
        color=discord.Color.orange(),
    )
    embed.add_field(name="申請した人(請求先)", value=await user_label(bot, payer_id), inline=False)
    embed.add_field(name="金額", value=fmt(amount), inline=False)
    embed.add_field(name="請求の理由", value=inv_reason or "(なし)", inline=False)
    embed.add_field(name="いまの期限", value=due_text(due_date, status), inline=False)
    embed.add_field(name="期限延長を申請する理由", value=reason, inline=False)
    if not await send_dm(bot, issuer_id, embed=embed):
        return False, "請求元にDMを送れませんでした。直接連絡してください。"
    return True, f"請求書 #{invoice_id} の期限延長申請を、請求元にDMで送りました。"


class ExtensionModal(discord.ui.Modal, title="期限延長申請"):
    """「期限延長申請をする」を押したときに開く入力画面。理由は必須。"""

    reason = discord.ui.TextInput(
        label="期限延長を申請する理由(必須)",
        style=discord.TextStyle.paragraph,
        placeholder="例: 給料日が来週のため、それまで待ってほしいです",
        required=True,
        min_length=1,
        max_length=500,
    )

    def __init__(self, invoice_id: int):
        super().__init__()
        self.invoice_id = invoice_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        _, message = await request_extension(
            interaction.client, interaction.user, self.invoice_id, str(self.reason.value)
        )
        await reply(interaction, message)


class InvoiceButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"eco:inv:(?P<action>pay|extend|decline):(?P<id>[0-9]+)",
):
    """請求書DMの「支払う」「期限延長申請をする」ボタン。
    custom_id に請求書番号を入れておくので、bot を再起動した後でも押せる。
    (以前の「拒否する」ボタン(decline)が残っているDMは、期限延長申請として扱う)"""

    def __init__(self, action: str, invoice_id: int):
        super().__init__(
            discord.ui.Button(
                label="支払う" if action == "pay" else "期限延長申請をする",
                style=discord.ButtonStyle.success if action == "pay" else discord.ButtonStyle.secondary,
                custom_id=f"eco:inv:{action}:{invoice_id}",
            )
        )
        self.action = action
        self.invoice_id = invoice_id

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item, match, /):
        return cls(match["action"], int(match["id"]))

    async def callback(self, interaction: discord.Interaction) -> None:
        bot = interaction.client
        if self.action != "pay":  # 期限延長申請 → 理由の入力画面を開く
            problem = extension_problem(await load_invoice(bot, self.invoice_id), interaction.user)
            if problem:
                return await reply(interaction, problem)
            return await interaction.response.send_modal(ExtensionModal(self.invoice_id))
        ok, message = await settle_invoice(bot, interaction.user, self.invoice_id, "pay")
        if not ok:
            return await reply(interaction, message)
        invoice = await load_invoice(bot, self.invoice_id)
        # DMの請求書を「支払い済み」の表示に書き換えて、ボタンを消す
        await interaction.response.edit_message(embed=await invoice_embed(bot, invoice), view=None)
        await interaction.followup.send(message, ephemeral=True)


def invoice_view(invoice_id: int, has_due: bool = False) -> discord.ui.View:
    """請求書DMのボタン。「期限延長申請をする」は、期限がある請求書だけに付く。"""
    view = discord.ui.View(timeout=None)
    view.add_item(InvoiceButton("pay", invoice_id))
    if has_due:
        view.add_item(InvoiceButton("extend", invoice_id))
    return view


# ---------------------------------------------------------------- VC作成パネル(寮VC・シクレVC・VC作成)
VC_KINDS = {
    "dorm": {"name": "寮VC", "button": "寮VC作成", "style": discord.ButtonStyle.success},
    "secret": {"name": "シクレVC", "button": "シクレVC作成", "style": discord.ButtonStyle.secondary},
    "arcade": {"name": "VC作成", "button": "VC作成", "style": discord.ButtonStyle.primary},
}
MAX_COURSES = 25  # Discord のメッセージに付けられるボタンの最大数
MAX_DURATION_MINUTES = 30 * 24 * 60  # 時間制限は最大30日
ROOM_EXTEND_SECONDS = 12 * 60 * 60  # 「時間延長」ボタンで延びる時間(固定で12時間)
MAX_USER_LIMIT = 99  # Discord のVCの人数制限の最大
ALLOW_SELECT_MAX = 25  # 「入室許可するメンバー」で一度に選べる人数

# VCの中に置く操作パネルのボタン: 種類ごとに、どのボタンを置くか
ROOM_ACTIONS = {
    "dorm": ["rename", "delete"],
    "arcade": ["rename", "limit", "extend", "delete"],
    "secret": ["rename", "limit", "allow", "public", "private", "delete"],
}
ROOM_ACTION_LABEL = {
    "rename": "VC名変更",
    "limit": "人数制限",
    "extend": "時間延長(+12時間)",
    "delete": "部屋を削除",
    "allow": "入室を許可するメンバー",
    "public": "公開にする",
    "private": "非公開にする",
}


def parse_price(text: str) -> int:
    """「500」「1,000」「500コイン」などを金額にする。0は無料。使えない入力なら ValueError。"""
    t = text.strip().translate(str.maketrans("０１２３４５６７８９，", "0123456789,"))
    t = t.replace(",", "").replace("コイン", "").strip()
    if not t.isdigit():
        raise ValueError("金額は、数字で入力してください。(例: 500 / 0で無料)")
    value = int(t)
    if value > MAX_AMOUNT:
        raise ValueError("金額が大きすぎます。")
    return value


def parse_duration(text: str) -> int | None:
    """「30分」「1時間」「1時間30分」などを、分にする。「なし」「無制限」や空欄は None(時間制限なし)。
    使えない入力なら ValueError。"""
    t = text.strip().translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    if t in ("", "なし", "無制限", "制限なし"):
        return None
    m = re.fullmatch(r"(?:(\d+)\s*(?:時間|h|H))?\s*(?:(\d+)\s*(?:分|m|M))?", t)
    if not m or not (m.group(1) or m.group(2)):
        raise ValueError("時間の書き方が正しくありません。例: 30分 ・ 1時間 ・ 1時間30分 (制限なしなら「なし」)")
    minutes = int(m.group(1) or 0) * 60 + int(m.group(2) or 0)
    if minutes <= 0:
        raise ValueError("時間は1分以上にしてください。")
    if minutes > MAX_DURATION_MINUTES:
        raise ValueError("時間は30日以内にしてください。")
    return minutes


def format_duration(minutes: int) -> str:
    """分を「1時間30分」の形の文字にする。"""
    hours, mins = divmod(minutes, 60)
    return (f"{hours}時間" if hours else "") + (f"{mins}分" if mins else "")


def parse_courses(text: str) -> list[tuple[str, int, int | None]]:
    """VC作成パネルのコース一覧を読み取る。1行に1コース、「コース名,金額,時間」の形。(時間は省略できる=制限なし)
    戻り値は (コース名, 金額, 時間の分 または None) のリスト。使えない入力なら ValueError。"""
    courses: list[tuple[str, int, int | None]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in re.split(r"[,、，:：=＝]", line)]
        if len(parts) not in (2, 3) or not parts[0]:
            raise ValueError(
                f"{number}行目の書き方が正しくありません。「コース名,金額,時間」の形で、1行に1コースずつ書いてください。"
                "(例: 1時間コース,900,1時間)"
            )
        if len(parts[0]) > 50:
            raise ValueError(f"{number}行目のコース名が長すぎます。(50文字まで)")
        try:
            minutes = parse_duration(parts[2]) if len(parts) == 3 else None
            courses.append((parts[0], parse_price(parts[1]), minutes))
        except ValueError as e:
            raise ValueError(f"{number}行目: {e}") from None
    if not courses:
        raise ValueError("コースを1つ以上入力してください。(例: 1時間コース,900,1時間)")
    if len(courses) > MAX_COURSES:
        raise ValueError(f"コースは{MAX_COURSES}個までです。")
    return courses


def course_text(name: str, price: int, minutes: int | None) -> str:
    return f"・{name} … " + (fmt(price) if price else "無料") + " / " + (
        f"時間制限 {format_duration(minutes)}" if minutes else "時間制限なし"
    )


def room_name(kind: str, member: discord.Member, minutes: int | None = None) -> str:
    """作った直後のVCの名前。(VCの名前は、作った本人があとから変えられる)
    VC作成(旧ゲーセンVC)は「ユーザー名 時間制限」だけ。"""
    name = member.display_name
    if kind == "dorm":
        text = f"{name}の部屋"
    elif kind == "secret":
        text = f"{name}のシクレVC"
    else:
        text = f"{name} {format_duration(minutes)}" if minutes else name
    return text[:100]


def room_overwrites(kind: str, guild: discord.Guild, member: discord.Member) -> dict:
    """VCの権限。作った本人には「チャンネルの管理」を与える。
    シクレVCは、他の人からは見えない。(見せる人は、VC内のパネルから選べる)"""
    overwrites: dict = {}
    if guild.me is not None:  # bot 自身も、見えなくなったVCを操作できるようにしておく
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True, connect=True, manage_channels=True, manage_roles=True,
            send_messages=True, embed_links=True,
        )
    owner = discord.PermissionOverwrite(
        view_channel=True, connect=True, speak=True, manage_channels=True
    )
    if kind == "secret":
        overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)
        owner.manage_roles = True
    overwrites[member] = owner
    return overwrites


async def create_vc_room(interaction: discord.Interaction, panel_id: int, index: int) -> None:
    """パネルのボタンが押されたとき: 料金を引き落として、VCを作り、VCの中に操作パネルを置く。"""
    bot = interaction.client
    guild, member = interaction.guild, interaction.user
    if guild is None or not isinstance(member, discord.Member):
        return await reply(interaction, "このボタンはサーバー内で使ってください。")
    rows = await bot.db.query(
        "SELECT kind, price, category_id, courses FROM vc_panels WHERE id = ?", (panel_id,)
    )
    if not rows:
        return await reply(interaction, "このパネルは使えなくなっています。管理者に伝えてください。")
    kind, price, category_id, courses_json = rows[0]
    minutes = None
    course = None
    if kind == "arcade":
        courses = json.loads(courses_json or "[]")
        if not 0 <= index < len(courses):
            return await reply(interaction, "このコースは使えなくなっています。管理者に伝えてください。")
        course = courses[index]
        price = int(course[1])
        minutes = int(course[2]) if len(course) > 2 and course[2] else None  # 以前のパネルには時間がない
    label = VC_KINDS[kind]["name"] + (f"({course[0]})" if course else "")

    key = (guild.id, member.id)
    if key in bot.vc_busy:  # ボタンの連打で、二重に引き落とされないようにする
        return await reply(interaction, "いま作成中です。少し待ってください。")
    bot.vc_busy.add(key)
    try:
        await interaction.response.defer(ephemeral=True)
        balance = None
        if price > 0:
            ok, from_bal, _ = await bot.db.move(member.id, None, price, "vc_room", f"{label}の作成")
            if not ok:
                return await reply(
                    interaction, f"残高が足りません。(必要 {fmt(price)} / 現在 {fmt(from_bal or 0)})"
                )
            balance = from_bal
        category = guild.get_channel(category_id) if category_id else None
        try:
            channel = await guild.create_voice_channel(
                room_name(kind, member, minutes),
                category=category,
                overwrites=room_overwrites(kind, guild, member),
                user_limit=2 if kind == "dorm" else 0,  # 寮VCは2人専用。それ以外は人数無制限(あとから変えられる)
                reason=f"{label}パネル({member})",
            )
        except (discord.Forbidden, discord.HTTPException):
            log.exception("VCを作成できませんでした")
            if price > 0:  # 作れなかったので、引き落とした分を返す
                await bot.db.move(None, member.id, price, "refund", f"{label}の作成に失敗したため返金")
            return await reply(
                interaction,
                "VCを作成できませんでした。(引き落とした料金は返金しました)\n"
                "botに「チャンネルの管理」と「ロールの管理」の権限があるか、管理者に確認してもらってください。",
            )
        expires_at = int(time.time()) + minutes * 60 if minutes else None
        await bot.db.execute(
            "INSERT OR REPLACE INTO vc_rooms (channel_id, guild_id, owner_id, kind, price, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (channel.id, guild.id, member.id, kind, price, datetime.now(TZ).isoformat(), expires_at),
        )
        lines = [f"💸 {price:,}コインが自動引き落としされました。(残高 {fmt(balance)})" if price > 0
                 else "無料でVCを作成しました。"]
        lines.append(f"✅ {label}を作成しました: {channel.mention}")
        if kind == "secret":
            lines.append("このVCは、あなた以外には見えません。VC内のパネルの「入室を許可するメンバー」で、入れる人を選べます。")
        try:  # VCの中のチャットに、操作パネルを置く
            await channel.send(
                embed=room_panel_embed(kind, member, expires_at), view=room_panel_view(channel.id, kind)
            )
            lines.append("VC内のチャットに、操作パネル(名前変更など)を置きました。")
        except Exception as e:
            log.exception("VCの操作パネルを置けませんでした")
            lines.append(
                f"⚠ VC内に操作パネルを置けませんでした。({type(e).__name__}) "
                "botに「メッセージを送信」「埋め込みリンク」の権限があるか確認してください。"
            )
        if expires_at:
            lines.append(f"終了予定: <t:{expires_at}:F>(<t:{expires_at}:R>) ※時間になるとVCは自動で削除されます")
        await reply(interaction, "\n".join(lines))
    finally:
        bot.vc_busy.discard(key)


class VcRoomButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"eco:vc:(?P<panel>[0-9]+):(?P<idx>[0-9]+)",
):
    """VC作成パネルのボタン。custom_id にパネル番号とボタン番号を入れておくので、bot を再起動した後でも押せる。"""

    def __init__(self, panel_id: int, index: int, label: str = "VC作成",
                 style: discord.ButtonStyle = discord.ButtonStyle.primary):
        super().__init__(
            discord.ui.Button(label=label[:80], style=style, custom_id=f"eco:vc:{panel_id}:{index}")
        )
        self.panel_id = panel_id
        self.index = index

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item, match, /):
        return cls(int(match["panel"]), int(match["idx"]))

    async def callback(self, interaction: discord.Interaction) -> None:
        await create_vc_room(interaction, self.panel_id, self.index)


def vc_panel_view(panel_id: int, kind: str, courses: list) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    info = VC_KINDS[kind]
    if kind == "arcade":
        for i, course in enumerate(courses):
            name, price = course[0], course[1]
            minutes = course[2] if len(course) > 2 else None
            text = f"{name}({price:,}コイン" + (f"・{format_duration(minutes)}" if minutes else "") + ")"
            view.add_item(VcRoomButton(panel_id, i, text, info["style"]))
    else:
        view.add_item(VcRoomButton(panel_id, 0, info["button"], info["style"]))
    return view


async def post_vc_panel(
    interaction: discord.Interaction, kind: str, title: str, description: str,
    price: int, courses: list, category_id: int | None,
) -> None:
    """パネルを、いまのチャンネルに設置して、設定をDBに保存する。"""
    _, panel_id = await interaction.client.db.execute(
        "INSERT INTO vc_panels (guild_id, channel_id, kind, title, description, price, category_id, courses) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (interaction.guild_id, interaction.channel_id, kind, title, description, price, category_id,
         json.dumps(courses, ensure_ascii=False) if kind == "arcade" else None),
    )
    embed = discord.Embed(title=title, description=description or None, color=discord.Color.teal())
    if kind == "arcade":
        embed.add_field(name="コース", value="\n".join(course_text(*c) for c in courses), inline=False)
    else:
        embed.add_field(name="料金", value=fmt(price) if price else "無料", inline=False)
    embed.set_footer(text="ボタンを押すと、料金が自動で引き落とされてVCが作られます")
    await interaction.channel.send(embed=embed, view=vc_panel_view(panel_id, kind, courses))
    await reply(interaction, f"{VC_KINDS[kind]['name']}のパネルを設置しました。(パネル番号 {panel_id})")


class VcPanelModal(discord.ui.Modal):
    """寮VC・シクレVCのパネルの設定画面(パネルの名前・説明文・金額)。"""

    panel_title = discord.ui.TextInput(label="パネルの名前", max_length=100, required=True)
    panel_description = discord.ui.TextInput(
        label="パネルの説明文", style=discord.TextStyle.paragraph, max_length=1000, required=False,
        placeholder="パネルに表示する説明を書きます",
    )
    price = discord.ui.TextInput(
        label="設定する金額(コイン)", max_length=12, required=True, placeholder="例: 500 (0で無料)"
    )

    def __init__(self, kind: str, category_id: int | None):
        super().__init__(title=f"{VC_KINDS[kind]['name']}パネルの設定")
        self.kind = kind
        self.category_id = category_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            price = parse_price(str(self.price.value))
        except ValueError as e:
            return await reply(interaction, f"{e}\nもう一度、コマンドからやり直してください。")
        await post_vc_panel(
            interaction, self.kind, str(self.panel_title.value).strip(),
            str(self.panel_description.value or "").strip(), price, [], self.category_id,
        )


class ArcadePanelModal(discord.ui.Modal):
    """VC作成パネルの設定画面(パネルの名前・説明文・コース)。"""

    panel_title = discord.ui.TextInput(label="パネルの名前", max_length=100, required=True)
    panel_description = discord.ui.TextInput(
        label="パネルの説明文", style=discord.TextStyle.paragraph, max_length=1000, required=False,
        placeholder="パネルに表示する説明を書きます",
    )
    courses = discord.ui.TextInput(
        label="コース(1行に1つ。コース名,金額,時間)", style=discord.TextStyle.paragraph, max_length=1500,
        required=True, placeholder="30分コース,500,30分\n1時間コース,900,1時間\n(1行が1つのボタン。時間は省略すると制限なし)",
    )

    def __init__(self, category_id: int | None):
        super().__init__(title="VC作成パネルの設定")
        self.category_id = category_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            courses = parse_courses(str(self.courses.value))
        except ValueError as e:
            return await reply(interaction, f"{e}\nもう一度、コマンドからやり直してください。")
        await post_vc_panel(
            interaction, "arcade", str(self.panel_title.value).strip(),
            str(self.panel_description.value or "").strip(), 0, courses, self.category_id,
        )


# ---------------------------------------------------------------- 作られたVCの中の操作パネル
def room_panel_embed(kind: str, owner: discord.Member, expires_at: int | None) -> discord.Embed:
    embed = discord.Embed(
        title=f"🛠 {owner.display_name} さんの部屋の操作パネル",
        description="この部屋を作った人だけが、下のボタンで部屋の設定を変えられます。",
        color=discord.Color.teal(),
    )
    buttons = "\n".join(f"・{ROOM_ACTION_LABEL[a]}" for a in ROOM_ACTIONS[kind])
    embed.add_field(name="使えるボタン", value=buttons, inline=False)
    if expires_at:
        embed.add_field(name="終了予定", value=f"<t:{expires_at}:F>(<t:{expires_at}:R>)", inline=False)
    return embed


def room_panel_view(channel_id: int, kind: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    for action in ROOM_ACTIONS[kind]:
        view.add_item(RoomButton(action, channel_id))
    return view


async def room_context(interaction: discord.Interaction, channel_id: int):
    """操作できる人か確認して、(部屋の記録, VC) を返す。だめなら案内を出して None を返す。"""
    bot = interaction.client
    rows = await bot.db.query(
        "SELECT owner_id, kind, expires_at FROM vc_rooms WHERE channel_id = ?", (channel_id,)
    )
    if not rows:
        await reply(interaction, "この部屋は、もう操作できません。")
        return None
    owner_id, kind, expires_at = rows[0]
    if interaction.user.id != owner_id:
        await reply(interaction, "この部屋を作った人だけが使えます。")
        return None
    channel = interaction.guild.get_channel(channel_id) if interaction.guild else None
    if channel is None:
        await bot.db.execute("DELETE FROM vc_rooms WHERE channel_id = ?", (channel_id,))
        await reply(interaction, "このVCが見つかりません。(すでに削除されたようです)")
        return None
    return (owner_id, kind, expires_at), channel


async def edit_room(interaction: discord.Interaction, channel, **changes) -> bool:
    """VCの設定を変える。Discordの制限で待たされるとき(名前変更は10分に2回まで)は、案内を出す。"""
    try:
        await asyncio.wait_for(channel.edit(**changes), timeout=20)
        return True
    except asyncio.TimeoutError:
        await reply(interaction, "Discordの制限で、いまは変更できません。(VC名の変更は、10分間に2回までです)少し待ってからやり直してください。")
    except (discord.Forbidden, discord.HTTPException):
        log.exception("VCの設定を変更できませんでした")
        await reply(interaction, "変更できませんでした。botに「チャンネルの管理」の権限があるか確認してください。")
    return False


class RoomRenameModal(discord.ui.Modal):
    new_name = discord.ui.TextInput(label="新しいVC名", max_length=100, required=True)

    def __init__(self, channel_id: int):
        super().__init__(title="VC名変更")
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ctx = await room_context(interaction, self.channel_id)
        if ctx is None:
            return
        name = str(self.new_name.value).strip()
        if not name:
            return await reply(interaction, "VC名を入力してください。")
        await interaction.response.defer(ephemeral=True)
        if await edit_room(interaction, ctx[1], name=name[:100]):
            await reply(interaction, f"VC名を「{name[:100]}」に変更しました。")


class RoomLimitModal(discord.ui.Modal):
    limit = discord.ui.TextInput(label="人数制限(0〜99。0で無制限)", max_length=2, required=True, placeholder="例: 5")

    def __init__(self, channel_id: int):
        super().__init__(title="人数制限")
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ctx = await room_context(interaction, self.channel_id)
        if ctx is None:
            return
        text = str(self.limit.value).strip().translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        if not text.isdigit() or not 0 <= int(text) <= MAX_USER_LIMIT:
            return await reply(interaction, f"人数は、0〜{MAX_USER_LIMIT}の数字で入力してください。(0で無制限)")
        await interaction.response.defer(ephemeral=True)
        if await edit_room(interaction, ctx[1], user_limit=int(text)):
            await reply(interaction, "人数制限を「無制限」にしました。" if int(text) == 0 else f"人数制限を {int(text)}人 にしました。")


def allowed_members(channel, owner_id: int, bot_id: int) -> list:
    """シクレVCで、入室を許可されているメンバー(作った本人とbotは除く)。"""
    return [
        target for target, ow in channel.overwrites.items()
        if isinstance(target, discord.Member) and target.id not in (owner_id, bot_id) and ow.view_channel
    ]


async def room_extend(interaction: discord.Interaction, channel_id: int, expires_at: int | None) -> None:
    if not expires_at:
        return await reply(interaction, "この部屋には時間制限がないので、延長はできません。")
    new_expires = expires_at + ROOM_EXTEND_SECONDS
    await interaction.client.db.execute(
        "UPDATE vc_rooms SET expires_at = ? WHERE channel_id = ?", (new_expires, channel_id)
    )
    message = getattr(interaction, "message", None)
    if message is not None:  # 操作パネルの「終了予定」も書き換える
        try:
            embed = message.embeds[0]
            embed.clear_fields()
            embed.add_field(name="使えるボタン", value="\n".join(f"・{ROOM_ACTION_LABEL[a]}" for a in ROOM_ACTIONS["arcade"]), inline=False)
            embed.add_field(name="終了予定", value=f"<t:{new_expires}:F>(<t:{new_expires}:R>)", inline=False)
            await message.edit(embed=embed)
        except Exception:
            log.warning("操作パネルの終了予定を書き換えられませんでした", exc_info=True)
    await reply(interaction, f"時間を12時間延長しました。新しい終了予定: <t:{new_expires}:F>(<t:{new_expires}:R>)")


async def room_allow(interaction: discord.Interaction, channel, owner_id: int) -> None:
    """「入室を許可するメンバー」を選ぶ画面を出す。"""
    me_id = interaction.guild.me.id if interaction.guild.me else 0
    current = allowed_members(channel, owner_id, me_id)
    try:  # いま許可している人を、最初から選んだ状態にする
        select = discord.ui.UserSelect(
            placeholder="入室を許可するメンバーを選ぶ", min_values=0, max_values=ALLOW_SELECT_MAX,
            default_values=current[:ALLOW_SELECT_MAX],
        )
    except Exception:  # 選んだ状態にできないときは、空の状態で出す
        select = discord.ui.UserSelect(
            placeholder="入室を許可するメンバーを選ぶ", min_values=0, max_values=ALLOW_SELECT_MAX
        )

    async def on_select(inter: discord.Interaction) -> None:
        if inter.user.id != owner_id:
            return await reply(inter, "この部屋を作った人だけが使えます。")
        chosen = [m for m in select.values if m.id not in (owner_id, me_id)]
        try:
            for member in chosen:
                await channel.set_permissions(
                    member, overwrite=discord.PermissionOverwrite(view_channel=True, connect=True, speak=True)
                )
            chosen_ids = {m.id for m in chosen}
            for old in allowed_members(channel, owner_id, me_id):
                if old.id not in chosen_ids:  # 選ばれなくなった人は、許可を取り消す
                    await channel.set_permissions(old, overwrite=None)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("入室許可を変更できませんでした")
            return await reply(inter, "変更できませんでした。botに「ロールの管理」の権限があるか確認してください。")
        text = " ".join(m.mention for m in chosen) if chosen else "(なし)"
        await inter.response.edit_message(
            content=f"入室を許可するメンバーを更新しました: {text}\n(許可していない人には、このVCは見えません)", view=None
        )

    select.callback = on_select
    view = discord.ui.View(timeout=120)
    view.add_item(select)
    now_text = " ".join(m.mention for m in current) if current else "(なし)"
    await reply(
        interaction,
        f"入室を許可するメンバーを選んでください。(最大{ALLOW_SELECT_MAX}人。選んだ人だけが入れるようになります)\n"
        f"いま許可している人: {now_text}",
        view=view,
    )


async def room_set_public(interaction: discord.Interaction, channel, public: bool) -> None:
    try:
        await channel.set_permissions(
            interaction.guild.default_role, overwrite=discord.PermissionOverwrite(view_channel=public)
        )
    except (discord.Forbidden, discord.HTTPException):
        log.exception("公開設定を変更できませんでした")
        return await reply(interaction, "変更できませんでした。botに「ロールの管理」の権限があるか確認してください。")
    await reply(
        interaction,
        "このVCを公開にしました。誰でも見えます。" if public
        else "このVCを非公開にしました。許可したメンバー以外には見えません。",
    )


async def room_delete(interaction: discord.Interaction, channel, owner_id: int) -> None:
    """部屋を削除する。(押し間違いを防ぐため、もう一度確認する)"""
    bot = interaction.client
    confirm = discord.ui.Button(label="削除する", style=discord.ButtonStyle.danger)

    async def on_confirm(inter: discord.Interaction) -> None:
        if inter.user.id != owner_id:
            return await reply(inter, "この部屋を作った人だけが使えます。")
        await bot.db.execute("DELETE FROM vc_rooms WHERE channel_id = ?", (channel.id,))
        try:
            await channel.delete(reason="作成者が削除")
        except discord.NotFound:
            pass
        except (discord.Forbidden, discord.HTTPException):
            log.exception("VCを削除できませんでした")
            return await reply(inter, "削除できませんでした。botに「チャンネルの管理」の権限があるか確認してください。")
        await inter.response.edit_message(content="部屋を削除しました。", view=None)

    confirm.callback = on_confirm
    view = discord.ui.View(timeout=60)
    view.add_item(confirm)
    await reply(interaction, "この部屋を削除します。元には戻せません。よろしいですか?", view=view)


class RoomButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"eco:room:(?P<action>rename|limit|extend|delete|allow|public|private):(?P<ch>[0-9]+)",
):
    """VCの中の操作パネルのボタン。作った本人だけが使える。"""

    def __init__(self, action: str, channel_id: int):
        super().__init__(
            discord.ui.Button(
                label=ROOM_ACTION_LABEL[action],
                style=discord.ButtonStyle.danger if action == "delete" else discord.ButtonStyle.secondary,
                custom_id=f"eco:room:{action}:{channel_id}",
            )
        )
        self.action = action
        self.channel_id = channel_id

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item, match, /):
        return cls(match["action"], int(match["ch"]))

    async def callback(self, interaction: discord.Interaction) -> None:
        ctx = await room_context(interaction, self.channel_id)
        if ctx is None:
            return
        (owner_id, kind, expires_at), channel = ctx
        if self.action not in ROOM_ACTIONS.get(kind, []):
            return await reply(interaction, "この部屋では使えない操作です。")
        if self.action == "rename":
            await interaction.response.send_modal(RoomRenameModal(self.channel_id))
        elif self.action == "limit":
            await interaction.response.send_modal(RoomLimitModal(self.channel_id))
        elif self.action == "extend":
            await room_extend(interaction, self.channel_id, expires_at)
        elif self.action == "allow":
            await room_allow(interaction, channel, owner_id)
        elif self.action in ("public", "private"):
            await room_set_public(interaction, channel, self.action == "public")
        elif self.action == "delete":
            await room_delete(interaction, channel, owner_id)


# ---------------------------------------------------------------- Bot 本体
class EconomyBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.members = True  # 給料配布のためメンバー一覧が必要(開発者ポータルで要有効化)
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.db = Database(DB_PATH)
        self.http_session: aiohttp.ClientSession | None = None
        self.web_runner: web.AppRunner | None = None
        # VC計測用: {(サーバーID, ユーザーID): 最後に時間を加算した時刻}  ← VCにいる間だけ入っている
        self.vc_sessions: dict[tuple[int, int], float] = {}
        # 計測するチャンネル: {サーバーID: {チャンネルID, ...}}
        self.vc_tracked: dict[int, set[int]] = {}
        # VC作成の処理中の人: {(サーバーID, ユーザーID)}  ← ボタン連打による二重引き落とし防止
        self.vc_busy: set[tuple[int, int]] = set()

    async def setup_hook(self) -> None:
        await self.db.connect()
        if MODE == "member":  # Activity 用の画面は、全員が使う新経済botで動かす
            await start_web(self)
        self.add_view(PanelView(self))
        self.add_dynamic_items(VcRoomButton, RoomButton)  # VC作成パネルと、VC内の操作パネルのボタン
        self.add_dynamic_items(InvoiceButton)  # 請求書のボタン(以前に送った請求書のボタンが旧botに残っているので、両方で受け付ける)
        await self.load_vc_channels()
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.sync_global_commands()
        # 自動処理は、二重に動かないように、片方のbotだけで動かす
        if MODE == "admin":
            self.salary_loop.start()
            self.vc_loop.start()
            self.room_loop.start()
        else:
            self.invoice_due_loop.start()

    async def sync_global_commands(self) -> None:
        """グローバルコマンドを同期する。

        Developer Portal で Activity を有効にすると、Discord が「エントリーポイントコマンド」
        (Activity の起動用コマンド)を自動で作る。これは一括更新で消せないため、
        既存のものを取得して、そのまま一緒に送る。
        """
        keep: list[dict] = []
        try:
            existing = await self.http.get_global_commands(self.application_id)
            fields = (
                "id", "name", "description", "type", "handler", "integration_types",
                "contexts", "default_member_permissions", "nsfw",
                "name_localizations", "description_localizations",
            )
            keep = [
                {k: c[k] for k in fields if k in c}
                for c in existing
                if c.get("type") == 4  # PRIMARY_ENTRY_POINT
            ]
        except Exception:
            log.exception("既存のコマンド一覧を取得できませんでした")

        if not keep:
            await self.tree.sync()
            return

        original = self.http.bulk_upsert_global_commands

        async def with_entry_point(application_id, payload):
            return await original(application_id, payload=[*payload, *keep])

        self.http.bulk_upsert_global_commands = with_entry_point
        try:
            await self.tree.sync()
        finally:
            del self.http.bulk_upsert_global_commands

    async def close(self) -> None:
        self.salary_loop.cancel()
        self.vc_loop.cancel()
        self.invoice_due_loop.cancel()
        self.room_loop.cancel()
        for key in list(self.vc_sessions):  # 終了前に、VCにいる人の時間を保存する
            try:
                await self.end_vc(key)
            except Exception:
                log.exception("VC時間の保存に失敗しました")
        await stop_web(self)
        await self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        log.info("ログイン: %s (%s) モード=%s", self.user, self.user.id, "新経済bot(member)" if MODE == "member" else "旧経済bot(admin)")
        log.info("bot.py のバージョン: %s", BOT_VERSION)
        # 自分が銀行botだと、もう一方のbotにも分かるように、IDを記録しておく
        await self.db.execute(
            "INSERT OR REPLACE INTO bank_bots (mode, user_id) VALUES (?, ?)", (MODE, self.user.id)
        )
        log.info("登録されたコマンド: %s", [c.name for c in self.tree.get_commands()])
        if MODE != "admin":
            return
        for guild in self.guilds:  # 起動した時点でVCにいる人の計測を始める
            await self.reconcile_vc(guild)

    # ---- VC接続時間の計測
    async def load_vc_channels(self) -> None:
        """計測するチャンネルの一覧をデータベースから読み込む。"""
        rows = await self.db.query("SELECT guild_id, channel_id FROM vc_channels")
        self.vc_tracked = {}
        for guild_id, channel_id in rows:
            self.vc_tracked.setdefault(guild_id, set()).add(channel_id)

    async def reconcile_vc(self, guild: discord.Guild) -> None:
        """「いま計測対象のVCにいる人」と「計測中の人」を一致させる。"""
        tracked = self.vc_tracked.get(guild.id, set())
        present = set()
        for channel in [*guild.voice_channels, *guild.stage_channels]:
            if channel.id in tracked:
                for member in channel.members:
                    if not member.bot:
                        present.add((guild.id, member.id))
        now = time.time()
        for key in present:
            self.vc_sessions.setdefault(key, now)
        for key in [k for k in self.vc_sessions if k[0] == guild.id and k not in present]:
            await self.end_vc(key)

    async def credit_vc(self, key: tuple[int, int], now: float | None = None) -> None:
        """前回から今までの経過時間(秒)を、その人のVC時間に足す。"""
        last = self.vc_sessions.get(key)
        if last is None:
            return
        seconds = int((now or time.time()) - last)
        if seconds <= 0:
            return
        self.vc_sessions[key] = last + seconds
        await self.db.execute(
            "INSERT INTO vc_time (guild_id, user_id, seconds) VALUES (?, ?, ?) "
            "ON CONFLICT(guild_id, user_id) DO UPDATE SET seconds = seconds + excluded.seconds",
            (key[0], key[1], seconds),
        )

    async def end_vc(self, key: tuple[int, int]) -> None:
        """計測を終了する(残りの時間を足してから、計測中の一覧から外す)。"""
        last = self.vc_sessions.pop(key, None)
        if last is None:
            return
        seconds = int(time.time() - last)
        if seconds > 0:
            await self.db.execute(
                "INSERT INTO vc_time (guild_id, user_id, seconds) VALUES (?, ?, ?) "
                "ON CONFLICT(guild_id, user_id) DO UPDATE SET seconds = seconds + excluded.seconds",
                (key[0], key[1], seconds),
            )

    async def on_voice_state_update(
        self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
    ) -> None:
        if member.bot or MODE != "admin":  # VC計測は旧経済bot(admin)だけが行う
            return
        tracked = self.vc_tracked.get(member.guild.id, set())
        was_in = before.channel is not None and before.channel.id in tracked
        now_in = after.channel is not None and after.channel.id in tracked
        key = (member.guild.id, member.id)
        if now_in and not was_in:  # 計測対象のVCに入った
            self.vc_sessions.setdefault(key, time.time())
        elif was_in and not now_in:  # 計測対象のVCから出た
            await self.end_vc(key)
            await self.check_vc_rules(member.guild.id, member.id)

    async def check_vc_rules(self, guild_id: int, user_id: int) -> None:
        """VC時間が条件を満たしたロールを付与し、報酬を払う。(1人につき同じロールは1回だけ)"""
        guild = self.get_guild(guild_id)
        if guild is None:
            return
        rules = await self.db.query(
            "SELECT role_id, minutes, expire_minutes, reward FROM vc_rules WHERE guild_id = ?",
            (guild_id,),
        )
        if not rules:
            return
        rows = await self.db.query(
            "SELECT seconds FROM vc_time WHERE guild_id = ? AND user_id = ?", (guild_id, user_id)
        )
        seconds = rows[0][0] if rows else 0
        now = int(time.time())
        for role_id, minutes, expire_minutes, reward in rules:
            if seconds < minutes * 60:
                continue
            expires_at = now + expire_minutes * 60 if expire_minutes else None
            inserted, _ = await self.db.execute(
                "INSERT OR IGNORE INTO vc_grants "
                "(guild_id, user_id, role_id, granted_at, expires_at, removed) VALUES (?, ?, ?, ?, ?, 0)",
                (guild_id, user_id, role_id, now, expires_at),
            )
            if not inserted:  # すでに付与済み
                continue
            member = guild.get_member(user_id)
            role = guild.get_role(role_id)
            if member is None:
                continue
            if role is not None:
                try:
                    await member.add_roles(role, reason=f"VC接続時間 {minutes}分 達成")
                except (discord.Forbidden, discord.HTTPException):
                    log.warning("ロールを付与できません(botのロール順位/権限を確認): %s", role.name)
            if reward > 0:
                _, _, bal = await self.db.move(
                    None, user_id, reward, "reward", f"VC接続時間 {minutes}分 達成の報酬"
                )
                await notify_dm(
                    self, user_id,
                    title="🎙️ VC接続時間の報酬",
                    amount=reward,
                    sender=BANK_NAME,
                    reason=f"VC接続時間 {minutes}分 達成" + (f"(ロール: {role.name})" if role else ""),
                    balance=bal,
                    color=discord.Color.purple(),
                )

    async def expire_vc_roles(self, now: int) -> None:
        """自動削除の時刻になったVCロールを外す。"""
        rows = await self.db.query(
            "SELECT guild_id, user_id, role_id FROM vc_grants "
            "WHERE removed = 0 AND expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        )
        for guild_id, user_id, role_id in rows:
            await self.db.execute(
                "UPDATE vc_grants SET removed = 1 WHERE guild_id = ? AND user_id = ? AND role_id = ?",
                (guild_id, user_id, role_id),
            )
            guild = self.get_guild(guild_id)
            member = guild.get_member(user_id) if guild else None
            role = guild.get_role(role_id) if guild else None
            if member and role and role in member.roles:
                try:
                    await member.remove_roles(role, reason="VCロールの有効期限")
                except (discord.Forbidden, discord.HTTPException):
                    log.warning("ロールを外せません(botのロール順位/権限を確認): %s", role.name)

    @tasks.loop(seconds=60)
    async def vc_loop(self) -> None:
        now = time.time()
        for key in list(self.vc_sessions):
            try:
                await self.credit_vc(key, now)
                await self.check_vc_rules(*key)
            except Exception:
                log.exception("VC時間の処理に失敗: %s", key)
        try:
            await self.expire_vc_roles(int(now))
        except Exception:
            log.exception("VCロールの自動削除に失敗")

    @vc_loop.before_loop
    async def _before_vc_loop(self) -> None:
        await self.wait_until_ready()

    async def expire_vc_rooms(self, now: int) -> int:
        """時間制限が来たVCを削除する。削除した数を返す。"""
        rows = await self.db.query(
            "SELECT channel_id, guild_id FROM vc_rooms WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        )
        count = 0
        for channel_id, guild_id in rows:
            guild = self.get_guild(guild_id)
            channel = guild.get_channel(channel_id) if guild else None
            if channel is not None:
                try:
                    await channel.delete(reason="VCの時間制限")
                except discord.NotFound:
                    pass  # すでに削除されている
                except (discord.Forbidden, discord.HTTPException):
                    log.warning("時間切れのVCを削除できません(botの権限を確認): %s", channel_id)
                    continue  # 次の回にもう一度ためす
            await self.db.execute("DELETE FROM vc_rooms WHERE channel_id = ?", (channel_id,))
            count += 1
        return count

    @tasks.loop(seconds=60)
    async def room_loop(self) -> None:
        try:
            await self.expire_vc_rooms(int(time.time()))
        except Exception:
            log.exception("時間切れのVCの削除に失敗しました")

    @room_loop.before_loop
    async def _before_room_loop(self) -> None:
        await self.wait_until_ready()

    # ---- サーバーブースト
    async def on_member_update(self, before: discord.Member, after: discord.Member) -> None:
        if after.bot or MODE != "admin":  # ブースト処理は旧経済bot(admin)だけが行う
            return
        started = before.premium_since is None and after.premium_since is not None
        ended = before.premium_since is not None and after.premium_since is None
        if not (started or ended):
            return
        rows = await self.db.query(
            "SELECT role_id, reward FROM boost_settings WHERE guild_id = ?", (after.guild.id,)
        )
        if not rows:
            return
        role_id, reward = rows[0]
        role = after.guild.get_role(role_id) if role_id else None
        try:
            if started:
                if role is not None:
                    await after.add_roles(role, reason="サーバーブースト")
            elif role is not None and role in after.roles:
                await after.remove_roles(role, reason="サーバーブーストの終了")
        except (discord.Forbidden, discord.HTTPException):
            log.warning("ブーストロールを付け外しできません(botのロール順位/権限を確認)")
        if started and reward > 0:
            _, _, bal = await self.db.move(None, after.id, reward, "reward", "サーバーブーストのお礼")
            await notify_dm(
                self, after.id,
                title="💎 サーバーブーストありがとうございます",
                amount=reward,
                sender=BANK_NAME,
                reason=f"{after.guild.name} をブーストしてくれたお礼",
                balance=bal,
                color=discord.Color.purple(),
            )

    # ---- 給料配布
    async def pay_salaries(self, guild: discord.Guild, month: str, reason: str) -> tuple[int, int]:
        salaries = await self.db.salaries(guild.id)
        if not salaries:
            return 0, 0
        if not guild.chunked:
            await guild.chunk()
        count = total = 0
        for member in guild.members:
            if member.bot:
                continue
            amount = member_salary(member, salaries)
            if amount <= 0:
                continue
            ok, _, bal = await self.db.move(None, member.id, amount, "salary", reason)
            if not ok:
                continue
            count += 1
            total += amount
            await notify_dm(
                self, member.id,
                title="💼 給料が支給されました",
                amount=amount,
                sender=BANK_NAME,
                reason=reason,
                balance=bal,
                color=discord.Color.green(),
            )
            await asyncio.sleep(0.3)  # DM のレート制限対策
        await self.db.mark_paid(guild.id, month)
        return count, total

    @tasks.loop(minutes=10)
    async def salary_loop(self) -> None:
        month = month_key()
        for guild in self.guilds:
            try:
                if await self.db.last_paid_month(guild.id) in (None, month):
                    continue
                if not await self.db.salaries(guild.id):
                    continue
                await self.pay_salaries(guild, month, f"{month} 月給")
            except Exception:
                log.exception("給料自動配布に失敗: guild=%s", guild.id)

    @salary_loop.before_loop
    async def _before_salary_loop(self) -> None:
        await self.wait_until_ready()

    async def notify_due_invoices(self) -> int:
        """期限日になった(まだ支払われていない)請求書のお知らせを、DMで送る。送った件数を返す。
        お知らせは、期限日の朝9時(日本時間)以降に、1つの請求書につき1回だけ送る。"""
        now = now_jst()
        rows = await self.db.query(
            "SELECT id FROM invoices WHERE status = 'pending' AND due_notified = 0 "
            "AND due_date IS NOT NULL AND due_date <= ?",
            (now.strftime("%Y-%m-%d"),),
        )
        count = 0
        for (invoice_id,) in rows:
            invoice = await load_invoice(self, invoice_id)
            due = date.fromisoformat(invoice[6])
            if now < datetime(due.year, due.month, due.day, DUE_NOTICE_HOUR, tzinfo=TZ):
                continue  # まだ期限日の朝9時になっていない
            # 先に「送った」印を付けて、二重に送らないようにする
            changed, _ = await self.db.execute(
                "UPDATE invoices SET due_notified = 1 WHERE id = ? AND due_notified = 0 AND status = 'pending'",
                (invoice_id,),
            )
            if changed == 0:
                continue
            _, issuer_id, payer_id, amount, reason, status, due_date = invoice
            if due == now.date():
                title, lead = f"⏰ 請求書 #{invoice_id} の期限〆日です", "本日が、この請求書の支払い期限(〆日)です。"
            else:
                title, lead = f"⏰ 請求書 #{invoice_id} の期限日を過ぎています", "この請求書の支払い期限(〆日)を過ぎています。"
            payer_embed = await invoice_embed(self, invoice)
            payer_embed.title, payer_embed.description = title, lead + "\nお早めにお支払いください。"
            payer_embed.color = discord.Color.red()
            await send_dm(self, payer_id, embed=payer_embed, view=invoice_view(invoice_id, True))
            issuer_embed = await invoice_embed(self, invoice)
            issuer_embed.title, issuer_embed.description = title, lead + "\nまだ支払われていません。"
            issuer_embed.color = discord.Color.red()
            await send_dm(self, issuer_id, embed=issuer_embed)
            count += 1
            await asyncio.sleep(0.3)  # DM のレート制限対策
        return count

    @tasks.loop(minutes=10)
    async def invoice_due_loop(self) -> None:
        try:
            await self.notify_due_invoices()
        except Exception:
            log.exception("請求書の期限のお知らせに失敗しました")

    @invoice_due_loop.before_loop
    async def _before_invoice_due_loop(self) -> None:
        await self.wait_until_ready()


bot = EconomyBot()
tree = bot.tree
admin_only = app_commands.default_permissions(administrator=True)


def everywhere(func):
    """サーバー・DM・グループDMのどこでも使え、ユーザーインストールにも対応する。"""
    func = app_commands.allowed_installs(guilds=True, users=True)(func)
    func = app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)(func)
    return func


def server_admin_only(func):
    """Discordの「管理者」権限を持つ人だけ。(管理者ロールの設定など、一番大事なコマンド用)"""
    func = app_commands.guild_only()(func)
    func = app_commands.allowed_installs(guilds=True, users=False)(func)
    func = admin_only(func)
    return func


async def is_bank_admin(interaction: discord.Interaction) -> bool:
    """Discordの管理者権限を持つ人、または /管理者ロール追加 で登録したロールを持つ人なら True"""
    user = interaction.user
    if not isinstance(user, discord.Member):
        return False
    if user.guild_permissions.administrator:
        return True
    admin_ids = {
        row[0]
        for row in await bot.db.query(
            "SELECT role_id FROM admin_roles WHERE guild_id = ?", (interaction.guild_id,)
        )
    }
    return any(role.id in admin_ids for role in user.roles)


def bank_admin(func):
    """管理者(Discordの管理者権限、または管理者ロール)だけが使えるサーバー内コマンド。"""
    func = app_commands.guild_only()(func)
    func = app_commands.allowed_installs(guilds=True, users=False)(func)
    func = app_commands.check(is_bank_admin)(func)
    return func


def server_only(func):
    """誰でも使えるが、サーバーの中だけで使えるコマンド。"""
    func = app_commands.guild_only()(func)
    func = app_commands.allowed_installs(guilds=True, users=False)(func)
    return func


# ---------------------------------------------------------------- 一般コマンド
@tree.command(name="残高", description="自分の残高を確認します")
@everywhere
async def balance_cmd(interaction: discord.Interaction):
    bal = await bot.db.get_balance(interaction.user.id)
    await reply(interaction, embed=balance_embed(interaction.user, bal))


@tree.command(name="送金", description="他のユーザーに送金します")
@everywhere
@app_commands.describe(user="送金先", amount="金額", reason="理由(任意)")
@app_commands.rename(user="ユーザー", amount="金額", reason="理由")
async def pay_cmd(
    interaction: discord.Interaction,
    user: discord.User,
    amount: app_commands.Range[int, 1, MAX_AMOUNT],
    reason: app_commands.Range[str, 0, 100] | None = None,
):
    await do_transfer(bot, interaction, user, amount, reason)


@tree.command(name="月給確認", description="自分の月給(ロール別・合計)を確認します")
@everywhere
async def salary_check_cmd(interaction: discord.Interaction):
    if not isinstance(interaction.user, discord.Member):
        return await reply(interaction, "月給の確認はサーバー内で使用してください。")
    await reply(interaction, embed=await salary_embed(bot, interaction.user))


# ---------------------------------------------------------------- 管理者コマンド
@tree.command(name="パネル", description="操作パネル(ボタン)をこのチャンネルに設置します")
@bank_admin
async def panel_cmd(interaction: discord.Interaction):
    embed = discord.Embed(
        title=f"🏦 {BANK_NAME}",
        description="下のボタンから残高確認・送金・月給確認ができます。",
        color=discord.Color.gold(),
    )
    await interaction.channel.send(embed=embed, view=PanelView(bot))
    await reply(interaction, "パネルを設置しました。")


@tree.command(name="月給設定", description="ロールの月給を設定します")
@bank_admin
@app_commands.describe(role="対象ロール", amount="月給(0で支給なし)")
@app_commands.rename(role="ロール", amount="金額")
async def salary_set_cmd(
    interaction: discord.Interaction,
    role: discord.Role,
    amount: app_commands.Range[int, 0, MAX_AMOUNT],
):
    await bot.db.set_salary(interaction.guild_id, role.id, amount)
    await reply(interaction, f"{role.mention} の月給を {fmt(amount)} に設定しました。")


@tree.command(name="月給削除", description="ロールの給料設定を削除します")
@bank_admin
@app_commands.rename(role="ロール")
async def salary_remove_cmd(interaction: discord.Interaction, role: discord.Role):
    removed = await bot.db.remove_salary(interaction.guild_id, role.id)
    await reply(interaction, "削除しました。" if removed else "そのロールには設定がありません。")


@tree.command(name="月給一覧", description="サーバーの給料設定一覧を表示します")
@bank_admin
async def salary_list_cmd(interaction: discord.Interaction):
    salaries = await bot.db.salaries(interaction.guild_id)
    if not salaries:
        return await reply(interaction, "給料設定はありません。")
    lines = [f"<@&{rid}>: {fmt(amt)}" for rid, amt in salaries.items()]
    last = await bot.db.last_paid_month(interaction.guild_id)
    embed = discord.Embed(title="給料設定一覧", description="\n".join(lines), color=discord.Color.green())
    embed.set_footer(text=f"最終配布月: {last or '未配布'} / 複数ロール所持時は合算")
    await reply(interaction, embed=embed)


@tree.command(name="月給配布", description="今すぐ給料を配布します(通常は毎月1日に自動配布)")
@bank_admin
@app_commands.describe(force="今月すでに配布済み(または設定月)でも配布する")
@app_commands.rename(force="強制配布")
async def salary_pay_cmd(interaction: discord.Interaction, force: bool = False):
    month = month_key()
    last = await bot.db.last_paid_month(interaction.guild_id)
    if last == month and not force:
        return await reply(
            interaction,
            f"{month} は配布済み(または今月設定済み)です。配布する場合は 強制配布 を「True」にしてください。",
        )
    await interaction.response.defer(ephemeral=True)
    count, total = await bot.pay_salaries(interaction.guild, month, f"{month} 月給")
    await reply(interaction, f"{count}人に合計 {fmt(total)} を配布しました。")


@tree.command(name="報酬付与", description="銀行からユーザーへ報酬を付与します")
@bank_admin
@app_commands.describe(user="対象", amount="金額", reason="理由")
@app_commands.rename(user="ユーザー", amount="金額", reason="理由")
async def reward_cmd(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: app_commands.Range[int, 1, MAX_AMOUNT],
    reason: app_commands.Range[str, 1, 100],
):
    if user.bot:
        return await reply(interaction, "botには付与できません。")
    _, _, bal = await bot.db.move(None, user.id, amount, "reward", reason)
    await reply(interaction, f"{user.mention} に {fmt(amount)} を付与しました。(残高 {fmt(bal)})")
    await notify_dm(
        bot, user.id,
        title="🎁 報酬が付与されました",
        amount=amount,
        sender=BANK_NAME,
        reason=reason,
        balance=bal,
        color=discord.Color.purple(),
    )


@tree.command(name="銀行回収", description="ユーザーから銀行へ回収(徴収)します")
@bank_admin
@app_commands.describe(user="対象", amount="金額", reason="理由")
@app_commands.rename(user="ユーザー", amount="金額", reason="理由")
async def bank_take_cmd(
    interaction: discord.Interaction,
    user: discord.Member,
    amount: app_commands.Range[int, 1, MAX_AMOUNT],
    reason: app_commands.Range[str, 1, 100],
):
    ok, bal, _ = await bot.db.move(user.id, None, amount, "collect", reason)
    if not ok:
        return await reply(interaction, f"残高不足です。(対象の残高 {fmt(bal or 0)})")
    await reply(interaction, f"{user.mention} から {fmt(amount)} を回収しました。(残高 {fmt(bal)})")
    try:
        await user.send(
            embed=discord.Embed(
                title="🏦 銀行による回収",
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow(),
            )
            .add_field(name="金額", value=fmt(amount), inline=False)
            .add_field(name="請求元", value=BANK_NAME, inline=False)
            .add_field(name="理由", value=reason, inline=False)
            .add_field(name="現在の残高", value=fmt(bal), inline=False)
        )
    except (discord.Forbidden, discord.HTTPException):
        pass


@tree.command(name="銀行状況", description="銀行の状況(流通総額・口座数)を表示します")
@bank_admin
async def bank_status_cmd(interaction: discord.Interaction):
    total, accounts = await bot.db.total_supply()
    embed = discord.Embed(title=f"🏦 {BANK_NAME}", color=discord.Color.gold())
    embed.add_field(name="流通総額", value=fmt(total))
    embed.add_field(name="口座数", value=f"{accounts:,}")
    await reply(interaction, embed=embed)


# ---------------------------------------------------------------- 管理者ロールの設定
@tree.command(name="管理者ロール追加", description="管理者ロールを追加します(このロールの人も管理者コマンドが使えます)")
@server_admin_only
@app_commands.describe(role="管理者にするロール")
@app_commands.rename(role="ロール")
async def admin_role_add_cmd(interaction: discord.Interaction, role: discord.Role):
    if role.is_default():
        return await reply(interaction, "@everyone は管理者ロールにできません。")
    await bot.db.execute(
        "INSERT OR IGNORE INTO admin_roles (guild_id, role_id) VALUES (?, ?)",
        (interaction.guild_id, role.id),
    )
    await reply(interaction, f"{role.mention} を管理者ロールに追加しました。")


@tree.command(name="管理者ロール削除", description="管理者ロールを外します")
@server_admin_only
@app_commands.describe(role="外すロール")
@app_commands.rename(role="ロール")
async def admin_role_remove_cmd(interaction: discord.Interaction, role: discord.Role):
    removed, _ = await bot.db.execute(
        "DELETE FROM admin_roles WHERE guild_id = ? AND role_id = ?", (interaction.guild_id, role.id)
    )
    await reply(interaction, "外しました。" if removed else "そのロールは管理者ロールではありません。")


@tree.command(name="管理者ロール一覧", description="管理者ロールの一覧を表示します")
@bank_admin
async def admin_role_list_cmd(interaction: discord.Interaction):
    rows = await bot.db.query(
        "SELECT role_id FROM admin_roles WHERE guild_id = ?", (interaction.guild_id,)
    )
    text = "\n".join(f"<@&{r[0]}>" for r in rows) or "管理者ロールは設定されていません。"
    await reply(interaction, embed=discord.Embed(title="管理者ロール", description=text, color=discord.Color.blurple()))


# ---------------------------------------------------------------- 請求書
@tree.command(name="請求書作成", description="請求書を作成し、相手にDMで送ります")
@everywhere
@app_commands.describe(
    user="請求先", amount="金額", reason="理由(任意)",
    due="支払い期限の日付(任意)。例: 2026/10/31 ・ 10/31 ・ 10月31日",
)
@app_commands.rename(user="ユーザー", amount="金額", reason="理由", due="期限")
async def invoice_cmd(
    interaction: discord.Interaction,
    user: discord.User,
    amount: app_commands.Range[int, 1, MAX_AMOUNT],
    reason: app_commands.Range[str, 0, 100] | None = None,
    due: app_commands.Range[str, 0, 30] | None = None,
):
    if user.bot:
        return await reply(interaction, "botには請求できません。")
    if user.id == interaction.user.id:
        return await reply(interaction, "自分自身には請求できません。")
    due_date = None
    if due and due.strip():
        try:
            due_date = parse_due_date(due, now_jst().date()).isoformat()
        except ValueError as e:
            return await reply(interaction, str(e))
    await interaction.response.defer(ephemeral=True)
    _, invoice_id = await bot.db.execute(
        "INSERT INTO invoices (issuer_id, payer_id, amount, reason, status, created_at, due_date) "
        "VALUES (?, ?, ?, ?, 'pending', ?, ?)",
        (interaction.user.id, user.id, amount, reason, datetime.now(TZ).isoformat(), due_date),
    )
    invoice = await load_invoice(bot, invoice_id)
    sent = await send_dm(
        bot, user.id, embed=await invoice_embed(bot, invoice), view=invoice_view(invoice_id, bool(due_date))
    )
    if not sent:
        await bot.db.execute("UPDATE invoices SET status = 'cancelled' WHERE id = ?", (invoice_id,))
        return await reply(
            interaction, "相手にDMを送れなかったため、請求書を取り消しました。(相手がDMを許可していない可能性があります)"
        )
    # 請求した本人にも、botからDMで控えを送る
    copy = await invoice_embed(bot, invoice)
    copy.title = f"📤 請求書 #{invoice_id} を送りました"
    await send_dm(bot, interaction.user.id, embed=copy)
    await reply(interaction, f"請求書 #{invoice_id} を {user.mention} にDMで送りました。({fmt(amount)})")


@tree.command(name="請求書一覧", description="請求中の請求書(届いたもの・送ったもの)を表示します")
@everywhere
async def invoices_cmd(interaction: discord.Interaction):
    me = interaction.user.id
    rows = await bot.db.query(
        "SELECT id, issuer_id, payer_id, amount, reason, due_date FROM invoices "
        "WHERE status = 'pending' AND (issuer_id = ? OR payer_id = ?) ORDER BY id DESC LIMIT 20",
        (me, me),
    )
    if not rows:
        return await reply(interaction, "請求中の請求書はありません。")
    lines = []
    for inv_id, issuer_id, payer_id, amount, reason, due_date in rows:
        due_part = f" 期限 {due_text(due_date, 'pending')}" if due_date else ""
        if payer_id == me:
            lines.append(f"#{inv_id} 【届いた】<@{issuer_id}> から {fmt(amount)} {reason or ''}{due_part}")
        else:
            lines.append(f"#{inv_id} 【送った】<@{payer_id}> へ {fmt(amount)} {reason or ''}{due_part}")
    embed = discord.Embed(title="🧾 請求中の請求書", description="\n".join(lines), color=discord.Color.orange())
    embed.set_footer(text="支払い: /請求書支払い 番号 / 取り消し: /請求書取消 番号")
    await reply(interaction, embed=embed)


@tree.command(name="請求書支払い", description="届いた請求書を番号で支払います")
@everywhere
@app_commands.describe(number="請求書の番号(/請求書一覧 で確認できます)")
@app_commands.rename(number="番号")
async def invoice_pay_cmd(interaction: discord.Interaction, number: int):
    _, message = await settle_invoice(bot, interaction.user, number, "pay")
    await reply(interaction, message)


@tree.command(name="請求書取消", description="自分が送った請求書を取り消します")
@everywhere
@app_commands.describe(number="請求書の番号(/請求書一覧 で確認できます)")
@app_commands.rename(number="番号")
async def invoice_cancel_cmd(interaction: discord.Interaction, number: int):
    changed, _ = await bot.db.execute(
        "UPDATE invoices SET status = 'cancelled', closed_at = ? "
        "WHERE id = ? AND issuer_id = ? AND status = 'pending'",
        (datetime.now(TZ).isoformat(), number, interaction.user.id),
    )
    await reply(
        interaction,
        f"請求書 #{number} を取り消しました。" if changed else "取り消せる請求書が見つかりません。(自分が送った請求中のものだけ取り消せます)",
    )


# ---------------------------------------------------------------- VC接続時間
VoiceLike = Union[discord.VoiceChannel, discord.StageChannel]


@tree.command(name="ボイス計測チャンネル追加", description="VC接続時間を計測するチャンネルを追加します")
@bank_admin
@app_commands.describe(channel="計測するボイスチャンネル")
@app_commands.rename(channel="チャンネル")
async def vc_channel_add_cmd(interaction: discord.Interaction, channel: VoiceLike):
    await bot.db.execute(
        "INSERT OR IGNORE INTO vc_channels (guild_id, channel_id) VALUES (?, ?)",
        (interaction.guild_id, channel.id),
    )
    await bot.load_vc_channels()
    await bot.reconcile_vc(interaction.guild)
    await reply(interaction, f"{channel.mention} の接続時間を計測します。")


@tree.command(name="ボイス計測チャンネル削除", description="VC接続時間の計測チャンネルを外します")
@bank_admin
@app_commands.describe(channel="外すボイスチャンネル")
@app_commands.rename(channel="チャンネル")
async def vc_channel_remove_cmd(interaction: discord.Interaction, channel: VoiceLike):
    removed, _ = await bot.db.execute(
        "DELETE FROM vc_channels WHERE guild_id = ? AND channel_id = ?", (interaction.guild_id, channel.id)
    )
    await bot.load_vc_channels()
    await bot.reconcile_vc(interaction.guild)
    await reply(interaction, "外しました。" if removed else "そのチャンネルは計測対象ではありません。")


@tree.command(name="ボイス計測チャンネル一覧", description="VC接続時間を計測するチャンネルの一覧")
@bank_admin
async def vc_channel_list_cmd(interaction: discord.Interaction):
    ids = bot.vc_tracked.get(interaction.guild_id, set())
    text = "\n".join(f"<#{i}>" for i in ids) or "計測するチャンネルが設定されていません。/ボイス計測チャンネル追加 で追加してください。"
    await reply(interaction, embed=discord.Embed(title="🎙️ 計測チャンネル", description=text, color=discord.Color.blurple()))


@tree.command(name="ボイスロール設定", description="VC接続時間が○分に達したらロールを付与する設定をします")
@bank_admin
@app_commands.describe(
    role="付与するロール",
    minutes="必要なVC接続時間(分)",
    expire_minutes="付与してから何分後にロールを外すか(0で外さない)",
    reward="達成したときの報酬(省略すると今の設定のまま)",
)
@app_commands.rename(role="ロール", minutes="分", expire_minutes="削除までの分", reward="報酬")
async def vc_role_set_cmd(
    interaction: discord.Interaction,
    role: discord.Role,
    minutes: app_commands.Range[int, 1, 525600],
    expire_minutes: app_commands.Range[int, 0, 525600] = 0,
    reward: app_commands.Range[int, 0, MAX_AMOUNT] | None = None,
):
    await bot.db.execute(
        "INSERT INTO vc_rules (guild_id, role_id, minutes, expire_minutes, reward) "
        "VALUES (?, ?, ?, ?, COALESCE(?, 0)) "
        "ON CONFLICT(guild_id, role_id) DO UPDATE SET minutes = excluded.minutes, "
        "expire_minutes = excluded.expire_minutes, reward = COALESCE(?, reward)",
        (interaction.guild_id, role.id, minutes, expire_minutes, reward, reward),
    )
    expire_text = f"{expire_minutes}分後に自動で外します" if expire_minutes else "自動では外しません"
    text = f"{role.mention}: VC接続 {minutes}分で付与 / {expire_text}"
    problem = role_problem(interaction.guild, role)
    if problem:
        text += "\n" + problem
    if not bot.vc_tracked.get(interaction.guild_id):
        text += "\n⚠️ 計測するチャンネルが未設定です。/ボイス計測チャンネル追加 で追加してください。"
    await reply(interaction, text)


@tree.command(name="ボイス報酬設定", description="VC時間ロールの達成報酬を設定します")
@bank_admin
@app_commands.describe(role="/ボイスロール設定 で設定済みのロール", reward="報酬(0で報酬なし)")
@app_commands.rename(role="ロール", reward="報酬")
async def vc_reward_set_cmd(
    interaction: discord.Interaction,
    role: discord.Role,
    reward: app_commands.Range[int, 0, MAX_AMOUNT],
):
    changed, _ = await bot.db.execute(
        "UPDATE vc_rules SET reward = ? WHERE guild_id = ? AND role_id = ?",
        (reward, interaction.guild_id, role.id),
    )
    if not changed:
        return await reply(interaction, "先に /ボイスロール設定 でそのロールのVC時間設定を作ってください。")
    await reply(interaction, f"{role.mention} の達成報酬を {fmt(reward)} に設定しました。")


@tree.command(name="ボイスロール削除", description="VC時間ロールの設定を削除します")
@bank_admin
@app_commands.describe(role="削除するロール設定")
@app_commands.rename(role="ロール")
async def vc_role_remove_cmd(interaction: discord.Interaction, role: discord.Role):
    removed, _ = await bot.db.execute(
        "DELETE FROM vc_rules WHERE guild_id = ? AND role_id = ?", (interaction.guild_id, role.id)
    )
    await reply(interaction, "削除しました。(すでに付与されたロールはそのままです)" if removed else "そのロールの設定はありません。")


@tree.command(name="ボイスロール一覧", description="VC時間ロールの設定一覧")
@bank_admin
async def vc_role_list_cmd(interaction: discord.Interaction):
    rows = await bot.db.query(
        "SELECT role_id, minutes, expire_minutes, reward FROM vc_rules WHERE guild_id = ? ORDER BY minutes",
        (interaction.guild_id,),
    )
    if not rows:
        return await reply(interaction, "VC時間ロールの設定はありません。")
    lines = []
    for role_id, minutes, expire_minutes, reward in rows:
        expire = f"{expire_minutes}分後に削除" if expire_minutes else "削除なし"
        lines.append(f"<@&{role_id}>: {minutes}分で付与 / {expire} / 報酬 {fmt(reward)}")
    await reply(interaction, embed=discord.Embed(title="🎙️ VC時間ロール", description="\n".join(lines), color=discord.Color.blurple()))


@tree.command(name="ボイス時間", description="VC接続時間を確認します")
@server_only
@app_commands.describe(user="確認する人(省略すると自分)")
@app_commands.rename(user="ユーザー")
async def vc_time_cmd(interaction: discord.Interaction, user: discord.Member | None = None):
    # 確認できるロールが設定されている場合は、そのロールを持つ人だけ(このコマンドに管理者権限は不要)
    view_ids = {
        row[0]
        for row in await bot.db.query(
            "SELECT role_id FROM vc_view_roles WHERE guild_id = ?", (interaction.guild_id,)
        )
    }
    if view_ids and not any(role.id in view_ids for role in interaction.user.roles):
        roles_text = " ".join(f"<@&{i}>" for i in view_ids)
        return await reply(interaction, f"VC接続時間を確認できるのは、次のロールを持つ人だけです: {roles_text}")
    target = user or interaction.user
    key = (interaction.guild_id, target.id)
    rows = await bot.db.query(
        "SELECT seconds FROM vc_time WHERE guild_id = ? AND user_id = ?", key
    )
    seconds = rows[0][0] if rows else 0
    if key in bot.vc_sessions:  # いま通話中なら、まだ保存されていない分も足す
        seconds += int(time.time() - bot.vc_sessions[key])
    embed = discord.Embed(title="🎙️ VC接続時間", color=discord.Color.blurple())
    embed.add_field(name=target.display_name, value=f"{seconds // 3600}時間 {seconds % 3600 // 60}分", inline=False)
    rules = await bot.db.query(
        "SELECT role_id, minutes FROM vc_rules WHERE guild_id = ? ORDER BY minutes", (interaction.guild_id,)
    )
    for role_id, minutes in rules:
        left = minutes * 60 - seconds
        state = "達成!" if left <= 0 else f"あと {left // 60 + 1}分"
        embed.add_field(name=f"{minutes}分", value=f"<@&{role_id}> … {state}", inline=True)
    await reply(interaction, embed=embed)


@tree.command(name="ボイス時間確認ロール追加", description="VC接続時間(/ボイス時間)を確認できるロールを追加します")
@bank_admin
@app_commands.describe(role="確認できるようにするロール")
@app_commands.rename(role="ロール")
async def vc_view_role_add_cmd(interaction: discord.Interaction, role: discord.Role):
    await bot.db.execute(
        "INSERT OR IGNORE INTO vc_view_roles (guild_id, role_id) VALUES (?, ?)",
        (interaction.guild_id, role.id),
    )
    await reply(
        interaction,
        f"{role.mention} を、VC接続時間を確認できるロールに追加しました。\n"
        "(1つでも設定すると、このロールを持つ人だけが /ボイス時間 を使えます)",
    )


@tree.command(name="ボイス時間確認ロール削除", description="VC接続時間を確認できるロールを外します")
@bank_admin
@app_commands.describe(role="外すロール")
@app_commands.rename(role="ロール")
async def vc_view_role_remove_cmd(interaction: discord.Interaction, role: discord.Role):
    removed, _ = await bot.db.execute(
        "DELETE FROM vc_view_roles WHERE guild_id = ? AND role_id = ?", (interaction.guild_id, role.id)
    )
    await reply(interaction, "外しました。" if removed else "そのロールは設定されていません。")


@tree.command(name="ボイス時間確認ロール一覧", description="VC接続時間を確認できるロールの一覧")
@bank_admin
async def vc_view_role_list_cmd(interaction: discord.Interaction):
    rows = await bot.db.query(
        "SELECT role_id FROM vc_view_roles WHERE guild_id = ?", (interaction.guild_id,)
    )
    text = "\n".join(f"<@&{r[0]}>" for r in rows) or "設定なし(全員が /ボイス時間 を使えます)"
    await reply(interaction, embed=discord.Embed(title="🎙️ VC時間を確認できるロール", description=text, color=discord.Color.blurple()))


@tree.command(name="ボイス時間リセット",description="ユーザーのVC接続時間と付与記録をリセットします(テスト用)")
@bank_admin
@app_commands.describe(user="対象")
@app_commands.rename(user="ユーザー")
async def vc_reset_cmd(interaction: discord.Interaction, user: discord.Member):
    key = (interaction.guild_id, user.id)
    bot.vc_sessions.pop(key, None)
    await bot.db.execute("DELETE FROM vc_time WHERE guild_id = ? AND user_id = ?", key)
    await bot.db.execute("DELETE FROM vc_grants WHERE guild_id = ? AND user_id = ?", key)
    await bot.reconcile_vc(interaction.guild)
    await reply(interaction, f"{user.mention} のVC接続時間と付与記録をリセットしました。")


# ---------------------------------------------------------------- サーバーブースト
@tree.command(name="ブースト設定", description="サーバーをブーストしたときのロールと報酬を設定します")
@bank_admin
@app_commands.describe(role="自動で付与するロール", reward="報酬(0で報酬なし)")
@app_commands.rename(role="ロール", reward="報酬")
async def boost_set_cmd(
    interaction: discord.Interaction,
    role: discord.Role | None = None,
    reward: app_commands.Range[int, 0, MAX_AMOUNT] | None = None,
):
    if role is None and reward is None:
        return await reply(interaction, "ロール か 報酬 のどちらか(または両方)を指定してください。")
    await bot.db.execute(
        "INSERT INTO boost_settings (guild_id, role_id, reward) VALUES (?, ?, COALESCE(?, 0)) "
        "ON CONFLICT(guild_id) DO UPDATE SET role_id = COALESCE(?, role_id), reward = COALESCE(?, reward)",
        (interaction.guild_id, role.id if role else None, reward, role.id if role else None, reward),
    )
    text = "ブースト設定を保存しました。/ブースト設定確認 で確認できます。"
    if role is not None:
        problem = role_problem(interaction.guild, role)
        if problem:
            text += "\n" + problem
    await reply(interaction, text)


@tree.command(name="ブースト設定削除", description="ブースト設定を削除します")
@bank_admin
async def boost_clear_cmd(interaction: discord.Interaction):
    removed, _ = await bot.db.execute("DELETE FROM boost_settings WHERE guild_id = ?", (interaction.guild_id,))
    await reply(interaction, "削除しました。" if removed else "ブースト設定はありません。")


@tree.command(name="ブースト設定確認", description="ブースト設定を表示します")
@bank_admin
async def boost_status_cmd(interaction: discord.Interaction):
    rows = await bot.db.query(
        "SELECT role_id, reward FROM boost_settings WHERE guild_id = ?", (interaction.guild_id,)
    )
    if not rows:
        return await reply(interaction, "ブースト設定はありません。/ブースト設定 で設定してください。")
    role_id, reward = rows[0]
    embed = discord.Embed(title="💎 ブースト設定", color=discord.Color.purple())
    embed.add_field(name="自動付与ロール", value=f"<@&{role_id}>" if role_id else "(なし)", inline=False)
    embed.add_field(name="報酬", value=fmt(reward), inline=False)
    embed.set_footer(text="ブーストをやめるとロールは自動で外れます")
    await reply(interaction, embed=embed)


# ---------------------------------------------------------------- VC作成パネル
def panel_category_id(interaction: discord.Interaction, category) -> int | None:
    """作ったVCを入れるカテゴリ。指定がなければ、パネルを置くチャンネルと同じカテゴリ。"""
    if category is not None:
        return category.id
    return getattr(interaction.channel, "category_id", None)


@tree.command(name="寮vcパネル", description="寮VC(2人専用VC)を作るボタンのパネルを設置します")
@bank_admin
@app_commands.describe(category="作ったVCを入れるカテゴリ(省略すると、このチャンネルと同じカテゴリ)")
@app_commands.rename(category="カテゴリ")
async def dorm_panel_cmd(interaction: discord.Interaction, category: discord.CategoryChannel | None = None):
    await interaction.response.send_modal(VcPanelModal("dorm", panel_category_id(interaction, category)))


@tree.command(name="シクレvcパネル作成", description="シクレVC(作った本人だけに見えるVC)を作るボタンのパネルを設置します")
@bank_admin
@app_commands.describe(category="作ったVCを入れるカテゴリ(省略すると、このチャンネルと同じカテゴリ)")
@app_commands.rename(category="カテゴリ")
async def secret_panel_cmd(interaction: discord.Interaction, category: discord.CategoryChannel | None = None):
    await interaction.response.send_modal(VcPanelModal("secret", panel_category_id(interaction, category)))


@tree.command(name="vc作成パネル作成", description="VC作成(人数無制限)をコース別のボタンで作るパネルを設置します")
@bank_admin
@app_commands.describe(category="作ったVCを入れるカテゴリ(省略すると、このチャンネルと同じカテゴリ)")
@app_commands.rename(category="カテゴリ")
async def arcade_panel_cmd(interaction: discord.Interaction, category: discord.CategoryChannel | None = None):
    await interaction.response.send_modal(ArcadePanelModal(panel_category_id(interaction, category)))


# ---------------------------------------------------------------- エラー処理
# ---------------------------------------------------------------- 2つのbotへのコマンドの振り分け
# 新経済bot(member)に入れるコマンド = 全員が使うもの。それ以外はすべて旧経済bot(admin)のコマンド。
MEMBER_COMMANDS = {
    "残高", "送金", "月給確認",
    "請求書作成", "請求書一覧", "請求書支払い", "請求書取消",
    "ボイス時間",
}


def apply_bot_mode(mode: str) -> None:
    """起動するbotに関係ないコマンドを、登録から外す。(外したコマンドは、そのbotには出てこない)"""
    for command in list(tree.get_commands()):
        belongs_to_member = command.name in MEMBER_COMMANDS
        if (mode == "member") != belongs_to_member:
            tree.remove_command(command.name)


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):  # 管理者ではない人が管理者コマンドを使った
        return await reply(interaction, "このコマンドは管理者(または管理者ロールを持つ人)のみ使用できます。")
    log.exception("コマンドエラー", exc_info=error)
    await reply(interaction, "エラーが発生しました。しばらくしてからもう一度お試しください。")


if __name__ == "__main__":
    discord.utils.setup_logging(level=logging.INFO)
    apply_bot_mode(MODE)
    try:
        bot.run(TOKEN, log_handler=None)
    except discord.LoginFailure:
        raise SystemExit(
            "トークンが正しくありません。Developer Portal の Bot ページで "
            "「Reset Token」を押し、新しいトークンを .env に貼り直してください。"
        )
    except discord.PrivilegedIntentsRequired:
        raise SystemExit(
            "Developer Portal の Bot ページで「SERVER MEMBERS INTENT」をオンにしてください。"
        )
