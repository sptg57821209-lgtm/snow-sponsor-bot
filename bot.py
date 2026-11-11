import asyncio
import io
import json
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("sponsor-bot")

TOKEN = os.getenv("DISCORD_TOKEN", "")
PREFIX = os.getenv("COMMAND_PREFIX", "!")
DB_PATH = Path(os.getenv("BOT_DATABASE", "bot_data.sqlite3"))
ADMIN_ROLE_ID = int(os.getenv("ADMIN_ROLE_ID", "0") or "0")
VERIFIED_ROLE_NAME = os.getenv("VERIFIED_ROLE_NAME", "유저")
UNVERIFIED_ROLE_NAME = os.getenv("UNVERIFIED_ROLE_NAME", "인증대기")
VOICE_CREATE_CHANNEL_NAME = os.getenv(
    "VOICE_CREATE_CHANNEL_NAME", "🎙️ 보이스 생성방"
)
VOICE_SLEEP_CREATE_CHANNEL_NAME = os.getenv(
    "VOICE_SLEEP_CREATE_CHANNEL_NAME", "🎙️ 잠수방 생성"
)
VOICE_ROOM_NAME = "⌁﹒🏠 . 𝟐인 숙소"
VOICE_ROOM_USER_LIMIT = 2
VOICE_SLEEP_ROOM_NAME = "⌁﹒💤 ．𝟏인 잠수방"
VOICE_SLEEP_ROOM_USER_LIMIT = 1
SECURITY_LOG_CHANNEL_NAME = os.getenv("SECURITY_LOG_CHANNEL_NAME", "🚨-보안-로그")
DM_INVITE_PHRASES = ("뒷메", "디엠 확인", "디엠 봐")
PAYMENT_INFO = os.getenv("SPONSOR_PAYMENT_INFO", "후원 계좌는 서버 운영진에게 문의해 주세요.")
SERVER_TAG = os.getenv("SERVER_TAG", "[태그]").strip()
TAG_COUNT_CHANNEL_PREFIX = "꒰ঌ✨-ㅣ𝕋𝔸𝔾𝕆ℕ𝔼꒱: "
TICKET_PANEL_TITLE = "🌸 설화 (雪花) | 후원 및 상담 센터"
TICKET_PANEL_DESCRIPTION = (
    "설화 서버에 따뜻한 온기를 더해주시는 모든 분께 감사드립니다.\n"
    "여러분의 소중한 후원은 서버를 더욱 아름답게 피워내는 밑거름이 됩니다.\n"
    "아래 **설화 피우기 (티켓 열기)** 버튼을 누르시면\n"
    "스태프와 1:1로 소통할 수 있는 안전한 비밀 채널이 열립니다.\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "📌 **안내 사항**\n"
    "ㆍ 모든 후원 내역은 스태프가 확인 후 수동으로 지급해 드립니다.\n"
    "ㆍ 티켓이 생성되면 **[후원자명 / 후원 금액 / 결제 수단]**을 남겨주세요.\n"
    "ㆍ 처리는 접수된 순서대로 신속하게 도와드리겠습니다.\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "✨ *설화와 함께해 주셔서 언제나 감사드립니다.*"
)
DEFAULT_SPONSOR_GOAL = int(os.getenv("SPONSOR_GOAL", "1000000") or "1000000")
REPEAT_MESSAGE_LIMIT = 20
REPEAT_MESSAGE_INTERVAL = 0.3
REPEAT_MESSAGE_TIMEOUT = timedelta(days=1)
TAG_COUNTER_RECONCILE_INTERVAL = 1.0
ticket_creation_locks = {}
repeat_message_state = {}
tagged_member_ids = {}
native_tag_by_member = {}
tag_counter_locks = {}
tag_counter_channel_locks = {}
tag_counter_requested_names = {}
tag_counter_update_tasks = {}
tag_counter_reconcile_task = None

intents = discord.Intents.default()
intents.members = True
intents.message_content = True


@contextmanager
def connect_db():
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA journal_mode=WAL")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize_db():
    with connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS warnings (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS sponsor_points (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                total INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS temp_voice_rooms (
                channel_id INTEGER PRIMARY KEY,
                owner_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS user_coins (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                balance INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS sponsor_dashboards (
                guild_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                target_goal INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ticket_approvals (
                channel_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                approved_by INTEGER NOT NULL,
                approved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER NOT NULL,
                setting_key TEXT NOT NULL,
                setting_value TEXT NOT NULL,
                PRIMARY KEY (guild_id, setting_key)
            );
            CREATE TABLE IF NOT EXISTS guild_backups (
                backup_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                snapshot_json TEXT NOT NULL
            );
            """
        )
        db.execute(
            """
            UPDATE user_coins
            SET balance = COALESCE(
                (
                    SELECT total FROM sponsor_points
                    WHERE sponsor_points.guild_id = user_coins.guild_id
                      AND sponsor_points.user_id = user_coins.user_id
                ),
                0
            )
            """
        )
        db.execute(
            """
            INSERT INTO user_coins (guild_id, user_id, balance)
            SELECT guild_id, user_id, total
            FROM sponsor_points
            WHERE NOT EXISTS (
                SELECT 1 FROM user_coins
                WHERE user_coins.guild_id = sponsor_points.guild_id
                  AND user_coins.user_id = sponsor_points.user_id
            )
            """
        )


def get_setting(guild_id: int, key: str, default: str | None = None) -> str | None:
    with connect_db() as db:
        row = db.execute(
            "SELECT setting_value FROM guild_settings WHERE guild_id = ? AND setting_key = ?",
            (guild_id, key),
        ).fetchone()
    return row[0] if row else default


def set_setting(guild_id: int, key: str, value: str):
    with connect_db() as db:
        db.execute(
            """
            INSERT INTO guild_settings (guild_id, setting_key, setting_value)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, setting_key) DO UPDATE SET setting_value = excluded.setting_value
            """,
            (guild_id, key, value),
        )


def reset_setting(guild_id: int, key: str):
    with connect_db() as db:
        db.execute(
            "DELETE FROM guild_settings WHERE guild_id = ? AND setting_key = ?",
            (guild_id, key),
        )


def get_filter_words(guild_id: int):
    default_words = tuple(
        word.strip().casefold()
        for word in os.getenv("FILTER_WORDS", "욕설1,욕설2,도배글,테러링크").split(",")
        if word.strip()
    )
    saved = get_setting(guild_id, "filter_words")
    return tuple(json.loads(saved)) if saved is not None else default_words


def add_warning(guild_id: int, user_id: int) -> int:
    with connect_db() as db:
        db.execute(
            """
            INSERT INTO warnings (guild_id, user_id, count) VALUES (?, ?, 1)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET count = count + 1
            """,
            (guild_id, user_id),
        )
        return db.execute(
            "SELECT count FROM warnings WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()[0]


def track_repeated_message(
    guild_id: int, user_id: int, content: str, created_at: datetime
) -> int:
    key = (guild_id, user_id)
    if not content:
        repeat_message_state.pop(key, None)
        return 0

    previous_content, previous_count, previous_time = repeat_message_state.get(
        key, (None, 0, None)
    )
    interval = (
        (created_at - previous_time).total_seconds()
        if previous_time is not None
        else None
    )
    if (
        content == previous_content
        and interval is not None
        and 0 <= interval <= REPEAT_MESSAGE_INTERVAL + 1e-6
    ):
        count = previous_count + 1
    else:
        count = 1

    if count >= REPEAT_MESSAGE_LIMIT:
        repeat_message_state.pop(key, None)
        return count

    repeat_message_state[key] = (content, count, created_at)
    return count


def get_warning_count(guild_id: int, user_id: int) -> int:
    with connect_db() as db:
        row = db.execute(
            "SELECT count FROM warnings WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
    return row[0] if row else 0


def set_coin_balance(
    db: sqlite3.Connection, guild_id: int, user_id: int, balance: int
):
    db.execute(
        """
        INSERT INTO user_coins (guild_id, user_id, balance) VALUES (?, ?, ?)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET balance = excluded.balance
        """,
        (guild_id, user_id, balance),
    )


def add_sponsor_points(guild_id: int, user_id: int, amount: int) -> int:
    with connect_db() as db:
        db.execute(
            """
            INSERT INTO sponsor_points (guild_id, user_id, total) VALUES (?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET total = total + excluded.total
            """,
            (guild_id, user_id, amount),
        )
        total = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()[0]
        set_coin_balance(db, guild_id, user_id, total)
        return total


def get_sponsor_total(guild_id: int, user_id: int) -> int:
    with connect_db() as db:
        row = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
    return row[0] if row else 0


def set_member_sponsor_total(guild_id: int, user_id: int, amount: int) -> tuple[int, int]:
    with connect_db() as db:
        guild_total = db.execute(
            "SELECT COALESCE(SUM(total), 0) FROM sponsor_points WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()[0]
        row = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        previous_amount = row[0] if row else 0

        if amount == 0:
            db.execute(
                "DELETE FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
                (guild_id, user_id),
            )
        else:
            db.execute(
                """
                INSERT INTO sponsor_points (guild_id, user_id, total)
                VALUES (?, ?, ?)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET total = excluded.total
                """,
                (guild_id, user_id, amount),
            )
        set_coin_balance(db, guild_id, user_id, amount)

    return previous_amount, guild_total - previous_amount + amount


def set_guild_sponsor_total_by_adjusting_member(
    guild_id: int, user_id: int, target_total: int
) -> tuple[int, int, int]:
    with connect_db() as db:
        guild_total = db.execute(
            "SELECT COALESCE(SUM(total), 0) FROM sponsor_points WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()[0]
        row = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        member_total = row[0] if row else 0
        if guild_total == 0 and target_total > 0:
            raise ValueError("no_sponsor_records")
        difference = target_total - guild_total
        adjusted_member_total = member_total + difference

        if adjusted_member_total < 0:
            raise ValueError("selected_member_total_too_low")

        if adjusted_member_total == 0:
            db.execute(
                "DELETE FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
                (guild_id, user_id),
            )
        else:
            db.execute(
                """
                INSERT INTO sponsor_points (guild_id, user_id, total)
                VALUES (?, ?, ?)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET total = excluded.total
                """,
                (guild_id, user_id, adjusted_member_total),
            )
        set_coin_balance(db, guild_id, user_id, adjusted_member_total)

    return guild_total, target_total, adjusted_member_total


def get_sponsor_ranking(guild_id: int):
    with connect_db() as db:
        return db.execute(
            """
            SELECT user_id, total FROM sponsor_points
            WHERE guild_id = ? ORDER BY total DESC, user_id ASC LIMIT 5
            """,
            (guild_id,),
        ).fetchall()


def approve_ticket_sponsorship(
    guild_id: int, channel_id: int, user_id: int, amount: int, approved_by: int
):
    coins_earned = amount
    with connect_db() as db:
        db.execute(
            """
            INSERT INTO ticket_approvals (channel_id, guild_id, user_id, amount, approved_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (channel_id, guild_id, user_id, amount, approved_by),
        )
        db.execute(
            """
            INSERT INTO sponsor_points (guild_id, user_id, total) VALUES (?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET total = total + excluded.total
            """,
            (guild_id, user_id, amount),
        )
        total = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()[0]
        set_coin_balance(db, guild_id, user_id, total)
        balance = db.execute(
            "SELECT balance FROM user_coins WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()[0]
    return coins_earned, total, balance


def get_coin_balance(guild_id: int, user_id: int) -> int:
    with connect_db() as db:
        row = db.execute(
            "SELECT balance FROM user_coins WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
    return row[0] if row else 0


def deduct_member_coins_and_sponsorship(
    guild_id: int, user_id: int, amount: int
) -> tuple[int, int]:
    with connect_db() as db:
        coin_row = db.execute(
            "SELECT balance FROM user_coins WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        sponsor_row = db.execute(
            "SELECT total FROM sponsor_points WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        coin_balance = coin_row[0] if coin_row else 0
        sponsor_total = sponsor_row[0] if sponsor_row else 0

        if coin_balance < amount:
            raise ValueError("insufficient_coins")
        if sponsor_total < amount:
            raise ValueError("insufficient_sponsorship")

        coin_update = db.execute(
            """
            UPDATE user_coins SET balance = balance - ?
            WHERE guild_id = ? AND user_id = ? AND balance >= ?
            """,
            (amount, guild_id, user_id, amount),
        )
        sponsor_update = db.execute(
            """
            UPDATE sponsor_points SET total = total - ?
            WHERE guild_id = ? AND user_id = ? AND total >= ?
            """,
            (amount, guild_id, user_id, amount),
        )
        if coin_update.rowcount != 1 or sponsor_update.rowcount != 1:
            raise RuntimeError("Coin or sponsorship balances changed during deduction.")

    return coin_balance - amount, sponsor_total - amount


def get_dashboard(guild_id: int):
    with connect_db() as db:
        return db.execute(
            "SELECT channel_id, message_id, target_goal FROM sponsor_dashboards WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()


def save_dashboard(guild_id: int, channel_id: int, message_id: int, target_goal: int):
    with connect_db() as db:
        db.execute(
            """
            INSERT INTO sponsor_dashboards (guild_id, channel_id, message_id, target_goal)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                channel_id = excluded.channel_id,
                message_id = excluded.message_id,
                target_goal = excluded.target_goal
            """,
            (guild_id, channel_id, message_id, target_goal),
        )


def update_dashboard_goal(guild_id: int, target_goal: int):
    with connect_db() as db:
        db.execute(
            "UPDATE sponsor_dashboards SET target_goal = ? WHERE guild_id = ?",
            (target_goal, guild_id),
        )


def get_guild_sponsor_stats(guild_id: int):
    with connect_db() as db:
        total_sponsors = db.execute(
            "SELECT COALESCE(SUM(total), 0), COUNT(*) FROM sponsor_points WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()
        total_coins = db.execute(
            "SELECT COALESCE(SUM(balance), 0) FROM user_coins WHERE guild_id = ?",
            (guild_id,),
        ).fetchone()[0]
        top_sponsors = db.execute(
            """
            SELECT sponsor_points.user_id, sponsor_points.total
            FROM sponsor_points
            JOIN user_coins
              ON user_coins.guild_id = sponsor_points.guild_id
             AND user_coins.user_id = sponsor_points.user_id
            WHERE sponsor_points.guild_id = ? AND user_coins.balance > 0
            ORDER BY sponsor_points.total DESC, sponsor_points.user_id ASC
            LIMIT 3
            """,
            (guild_id,),
        ).fetchall()
    return total_sponsors[0], total_sponsors[1], total_coins, top_sponsors


def make_premium_bar_graph(current: int, target: int) -> str:
    target = max(target, 1)
    percentage = min(int(current * 100 / target), 100)
    filled = int(percentage / 5)
    return (
        f"{'🔮' * filled}{'⬜' * (20 - filled)} **{percentage}%**\n"
        f"*현재 누적 {current:,}원 / 목표 {target:,}원*"
    )


async def update_live_dashboard(guild: discord.Guild):
    saved = get_dashboard(guild.id)
    if not saved:
        return
    channel_id, message_id, target_goal = saved
    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return
    current, contributors, coins, top_sponsors = get_guild_sponsor_stats(guild.id)
    medals = ("👑 **GOLD**", "✨ **SILVER**", "❄️ **BRONZE**")
    rank_lines = []
    for rank, (user_id, amount) in enumerate(top_sponsors):
        rank_lines.append(
            f"{medals[rank]} — <@{user_id}> (`{amount:,}원` 기여)"
        )
    rank_text = "\n".join(rank_lines) or "아직 후원자가 없습니다. 첫 번째 기여자가 되어 주세요!"
    embed = discord.Embed(
        title="🌌 설화 후원 실시간 라이브 전광판 🌌",
        description="설화 커뮤니티를 지탱하는 유저분들의 소중한 발자취입니다.",
        color=0x4C00C7,
    )
    embed.add_field(name="💵 누적 총 후원금액", value=f"🏦 KRW {current:,}원", inline=True)
    embed.add_field(name="🪙 유통 총 코인잔액", value=f"🪙 {coins:,} SNOW", inline=True)
    embed.add_field(
        name="📊 시즌 목표 달성도",
        value=make_premium_bar_graph(current, target_goal),
        inline=False,
    )
    embed.add_field(name="🏅 명예의 전당 TOP 3", value=rank_text, inline=False)
    embed.set_footer(text=f"후원 참여 {contributors}명 • 실시간 갱신")
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    try:
        message = await channel.fetch_message(message_id)
    except discord.NotFound:
        logger.warning("Sponsor dashboard message %s was deleted", message_id)
        return
    await message.edit(embed=embed, view=ServerMainDashboardView())


def set_temp_voice_room(channel_id: int, owner_id: int):
    with connect_db() as db:
        db.execute(
            "INSERT OR REPLACE INTO temp_voice_rooms (channel_id, owner_id) VALUES (?, ?)",
            (channel_id, owner_id),
        )


def remove_temp_voice_room(channel_id: int):
    with connect_db() as db:
        db.execute("DELETE FROM temp_voice_rooms WHERE channel_id = ?", (channel_id,))


def get_temp_voice_room_ids():
    with connect_db() as db:
        return [row[0] for row in db.execute("SELECT channel_id FROM temp_voice_rooms")]


def get_role(guild: discord.Guild, role_name: str):
    if role_name == "관리자":
        role_id = get_setting(guild.id, "admin_role_id", str(ADMIN_ROLE_ID) if ADMIN_ROLE_ID else None)
        if role_id:
            return guild.get_role(int(role_id))
    elif role_name == VERIFIED_ROLE_NAME:
        role_id = get_setting(guild.id, "verified_role_id")
        if role_id:
            return guild.get_role(int(role_id))
    elif role_name == UNVERIFIED_ROLE_NAME:
        role_id = get_setting(guild.id, "unverified_role_id")
        if role_id:
            return guild.get_role(int(role_id))
    return discord.utils.get(guild.roles, name=role_name)


def build_ticket_panel_embed() -> discord.Embed:
    return discord.Embed(
        title=TICKET_PANEL_TITLE,
        description=TICKET_PANEL_DESCRIPTION,
        color=0xF5A6C8,
    )


def build_ticket_welcome_embed() -> discord.Embed:
    embed = build_ticket_panel_embed()
    embed.add_field(name="💳 후원 계좌 안내", value=PAYMENT_INFO, inline=False)
    embed.add_field(
        name="📝 접수 정보",
        value="**후원자명 / 후원 금액 / 결제 수단**을 남겨주세요.",
        inline=False,
    )
    embed.set_footer(text="✨ 설화와 함께해 주셔서 언제나 감사드립니다.")
    return embed


async def send_interaction_message(
    interaction: discord.Interaction,
    *,
    content: str = None,
    embed: discord.Embed = None,
    ephemeral: bool = False,
):
    kwargs = {"content": content, "embed": embed, "ephemeral": ephemeral}
    if interaction.response.is_done():
        return await interaction.followup.send(**kwargs)
    return await interaction.response.send_message(**kwargs)


def is_interaction_acknowledgement_error(error: BaseException) -> bool:
    pending = [error]
    seen = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, discord.NotFound) and current.code == 10062:
            return True
        if isinstance(current, discord.HTTPException) and current.code == 40060:
            return True
        original = getattr(current, "original", None)
        cause = current.__cause__
        if isinstance(original, BaseException):
            pending.append(original)
        if isinstance(cause, BaseException):
            pending.append(cause)
    return False


async def send_log(guild: discord.Guild, message: str = None, embed: discord.Embed = None):
    channel_id = get_setting(guild.id, "security_log_channel_id")
    channel = guild.get_channel(int(channel_id)) if channel_id else None
    if channel is None:
        channel = discord.utils.get(guild.text_channels, name=SECURITY_LOG_CHANNEL_NAME)
    if isinstance(channel, discord.TextChannel):
        await channel.send(content=message, embed=embed)


async def create_support_ticket(interaction: discord.Interaction):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            logger.warning(
                "Ticket button interaction %s expired before acknowledgement",
                interaction.id,
            )
            return

    guild = interaction.guild
    if guild is None:
        await send_interaction_message(
            interaction,
            content="서버 안에서만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return

    lock_key = (guild.id, interaction.user.id)
    lock = ticket_creation_locks.setdefault(lock_key, asyncio.Lock())
    async with lock:
        existing = discord.utils.find(
            lambda channel: channel.topic
            and channel.topic.startswith(f"sponsor-ticket:{interaction.user.id}:"),
            guild.text_channels,
        )
        if existing:
            await send_interaction_message(
                interaction,
                content=f"이미 열린 티켓이 있습니다: {existing.mention}",
                ephemeral=True,
            )
            return

        bot_member = guild.me
        if bot_member is None:
            await send_interaction_message(
                interaction,
                content="봇의 서버 정보를 가져오지 못했습니다.",
                ephemeral=True,
            )
            return

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, attach_files=True, read_message_history=True
            ),
            bot_member: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, manage_channels=True, read_message_history=True
            ),
        }
        admin_role = get_role(guild, "관리자")
        if admin_role:
            overwrites[admin_role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True
            )
        category = interaction.channel.category if interaction.channel else None
        channel_name = re.sub(r"[^a-zA-Z0-9가-힣_-]", "-", interaction.user.name.lower())[:70]
        ticket = None
        try:
            ticket = await guild.create_text_channel(
                name=f"ticket-{channel_name}",
                category=category,
                overwrites=overwrites,
                topic=f"sponsor-ticket:{interaction.user.id}:pending",
            )
            await ticket.send(
                content=interaction.user.mention,
                embed=build_ticket_welcome_embed(),
                view=TicketControlView(),
            )
        except discord.HTTPException:
            logger.exception(
                "Could not create or initialize ticket for user %s in guild %s",
                interaction.user.id,
                guild.id,
            )
            if ticket:
                try:
                    await ticket.delete(reason="Ticket setup failed")
                except discord.HTTPException:
                    logger.exception("Could not delete incomplete ticket %s", ticket.id)
            await send_interaction_message(
                interaction,
                content="티켓을 만들지 못했습니다. 봇의 채널 관리 권한을 확인하고 다시 시도해 주세요.",
                ephemeral=True,
            )
            return

        await send_interaction_message(
            interaction,
            content=f"전용 티켓 채널을 만들었습니다: {ticket.mention}",
            ephemeral=True,
        )
        try:
            await interaction.user.send(
                f"{guild.name} 서버에 후원 문의 채널이 만들어졌습니다: {ticket.mention}"
            )
        except discord.Forbidden:
            logger.info("Could not DM ticket notification to user %s", interaction.user.id)


class SafeInteractionView(discord.ui.View):
    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: discord.ui.Item,
    ):
        logger.error(
            "UI callback failed for %s",
            item.custom_id or item.__class__.__name__,
            exc_info=(type(error), error, error.__traceback__),
        )
        if interaction.is_expired() or is_interaction_acknowledgement_error(error):
            logger.warning(
                "Skipping UI error response for interaction %s",
                interaction.id,
            )
            return
        try:
            await send_interaction_message(
                interaction,
                content="요청 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
                ephemeral=True,
            )
        except discord.HTTPException:
            logger.exception(
                "Could not send UI error response for interaction %s",
                interaction.id,
            )


class TicketSetupView(SafeInteractionView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="설화 피우기 (티켓 열기)",
        style=discord.ButtonStyle.primary,
        custom_id="sponsor_bot:open_ticket",
    )
    async def open_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await create_support_ticket(interaction)


class TicketControlView(SafeInteractionView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="후원 금액 승인",
        style=discord.ButtonStyle.success,
        custom_id="sponsor_bot:approve_sponsorship",
    )
    async def approve_sponsorship(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "후원 승인 권한은 서버 관리자 전용입니다.", ephemeral=True
            )
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "후원 티켓 채널에서만 사용할 수 있습니다.", ephemeral=True
            )
            return
        topic = interaction.channel.topic or ""
        if not topic.startswith("sponsor-ticket:"):
            await interaction.response.send_message(
                "이 채널은 후원 티켓이 아닙니다.", ephemeral=True
            )
            return
        await interaction.response.send_modal(SponsorApprovalModal())

    @discord.ui.button(
        label="티켓 닫기",
        style=discord.ButtonStyle.danger,
        custom_id="sponsor_bot:close_sponsor_ticket",
    )
    async def close_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "티켓 채널에서만 사용할 수 있습니다.", ephemeral=True
            )
            return
        topic = interaction.channel.topic or ""
        try:
            owner_id = int(topic.split(":", 2)[1])
        except (IndexError, ValueError):
            owner_id = None
        is_owner = interaction.user.id == owner_id
        is_admin = interaction.user.guild_permissions.manage_channels
        if not is_owner and not is_admin:
            await interaction.response.send_message(
                "티켓 작성자 또는 채널 관리 권한이 있는 운영진만 종료할 수 있습니다.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message("티켓을 종료합니다.")
        await asyncio.sleep(3)
        await interaction.channel.delete(reason="Ticket closed")


class SponsorApprovalModal(discord.ui.Modal, title="후원 금액 승인"):
    amount = discord.ui.TextInput(
        label="승인할 후원 금액 (숫자만)",
        placeholder="예: 50000",
        min_length=1,
        max_length=10,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        channel = interaction.channel
        if guild is None or not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message(
                "서버의 후원 티켓에서만 사용할 수 있습니다.", ephemeral=True
            )
            return
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "후원 승인 권한은 서버 관리자 전용입니다.", ephemeral=True
            )
            return
        try:
            amount = int(self.amount.value)
        except ValueError:
            await interaction.response.send_message(
                "금액은 숫자만 입력해 주세요.", ephemeral=True
            )
            return
        if amount <= 0 or amount > 1_000_000_000:
            await interaction.response.send_message(
                "금액은 1원 이상 10억 원 이하로 입력해 주세요.", ephemeral=True
            )
            return

        try:
            owner_id = int((channel.topic or "").split(":", 2)[1])
        except (IndexError, ValueError):
            await interaction.response.send_message(
                "티켓 생성자 정보를 찾을 수 없습니다.", ephemeral=True
            )
            return
        await interaction.response.defer(thinking=True)
        try:
            coins_earned, total, balance = approve_ticket_sponsorship(
                guild.id, channel.id, owner_id, amount, interaction.user.id
            )
        except sqlite3.IntegrityError:
            await interaction.edit_original_response(
                content="이 티켓의 후원은 이미 승인되었습니다. 중복 적립은 처리되지 않았습니다.",
            )
            return

        opener = guild.get_member(owner_id)
        embed = discord.Embed(title="후원 승인 완료", color=0x2ECC71)
        embed.description = f"<@{owner_id}>님의 후원이 등록되었습니다."
        embed.add_field(name="승인 금액", value=f"{amount:,}원", inline=True)
        embed.add_field(
            name="지급 코인 (후원액과 동일)",
            value=f"+{coins_earned:,} SNOW",
            inline=True,
        )
        embed.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
        await interaction.edit_original_response(embed=embed)
        await update_live_dashboard(guild)

        if opener:
            receipt = discord.Embed(title="후원 승인 영수증", color=0xFFD700)
            receipt.add_field(name="이번 후원", value=f"{amount:,}원", inline=True)
            receipt.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
            receipt.add_field(name="코인 잔액", value=f"{balance:,} SNOW", inline=False)
            try:
                await opener.send(embed=receipt)
            except discord.Forbidden:
                logger.info("Could not DM sponsor receipt to user %s", owner_id)


class ServerMainDashboardView(SafeInteractionView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="설화 피우기 (티켓 열기)",
        style=discord.ButtonStyle.primary,
        custom_id="sponsor_bot:dashboard_open_ticket",
    )
    async def open_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await create_support_ticket(interaction)


class TicketCloseView(SafeInteractionView):
    def __init__(self, owner_id: int = 0):
        super().__init__(timeout=None)
        self.owner_id = owner_id

    @discord.ui.button(
        label="상담 종료 및 채널 삭제",
        style=discord.ButtonStyle.danger,
        custom_id="sponsor_bot:close_ticket",
    )
    async def close_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("티켓 채널에서만 사용할 수 있습니다.", ephemeral=True)
            return
        topic_owner_id = None
        if interaction.channel.topic and interaction.channel.topic.startswith("sponsor-ticket:"):
            try:
                topic_owner_id = int(interaction.channel.topic.partition(":")[2])
            except ValueError:
                logger.warning("Invalid sponsor ticket topic on channel %s", interaction.channel.id)
        owner_id = topic_owner_id or self.owner_id
        is_owner = interaction.user.id == owner_id
        is_admin = interaction.user.guild_permissions.manage_channels
        if not is_owner and not is_admin:
            await interaction.response.send_message(
                "티켓 작성자 또는 채널 관리 권한이 있는 운영진만 종료할 수 있습니다.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message("티켓을 종료합니다.")
        await asyncio.sleep(3)
        await interaction.channel.delete(reason="Ticket closed")


class CaptchaVerifyView(SafeInteractionView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="인증 완료",
        style=discord.ButtonStyle.success,
        custom_id="sponsor_bot:verify",
    )
    async def verify(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        guild = interaction.guild
        member = interaction.user
        if guild is None or not isinstance(member, discord.Member):
            await interaction.response.send_message(
                "서버 안에서만 사용할 수 있습니다.", ephemeral=True
            )
            return
        verified_role = get_role(guild, VERIFIED_ROLE_NAME)
        if verified_role is None:
            await interaction.response.send_message(
                f"'{VERIFIED_ROLE_NAME}' 역할을 찾지 못했습니다.", ephemeral=True
            )
            return

        roles_to_add = [verified_role]
        unverified_role = get_role(guild, UNVERIFIED_ROLE_NAME)
        try:
            await member.add_roles(*roles_to_add, reason="Captcha verification")
            if unverified_role and unverified_role in member.roles:
                await member.remove_roles(unverified_role, reason="Captcha verification")
        except discord.Forbidden:
            await interaction.response.send_message(
                "역할을 변경할 권한이 없습니다. 봇 역할 순서와 권한을 확인해 주세요.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message("인증이 완료되었습니다.", ephemeral=True)
        try:
            await member.send(f"{guild.name} 서버 인증이 완료되었습니다. 환영합니다!")
        except discord.Forbidden:
            logger.info("Could not DM verification notification to user %s", member.id)


def serialize_permission_overwrites(overwrites):
    serialized = []
    for target, overwrite in overwrites.items():
        target_type = "role" if isinstance(target, discord.Role) else "member"
        allow, deny = overwrite.pair()
        serialized.append(
            {
                "target_type": target_type,
                "target_id": target.id,
                "target_name": target.name if target_type == "role" else None,
                "allow": allow.value,
                "deny": deny.value,
            }
        )
    return serialized


def create_guild_snapshot(guild: discord.Guild) -> dict:
    roles = [
        {
            "id": role.id,
            "name": role.name,
            "permissions": role.permissions.value,
            "color": role.color.value,
            "hoist": role.hoist,
            "mentionable": role.mentionable,
            "position": role.position,
        }
        for role in sorted(guild.roles, key=lambda item: item.position)
        if role != guild.default_role and not role.managed
    ]
    categories = [
        {
            "id": category.id,
            "name": category.name,
            "position": category.position,
            "overwrites": serialize_permission_overwrites(category.overwrites),
        }
        for category in sorted(guild.categories, key=lambda item: item.position)
    ]
    channels = []
    unsupported_channels = 0
    supported_types = (
        discord.TextChannel,
        discord.VoiceChannel,
        discord.StageChannel,
    )
    for channel in sorted(guild.channels, key=lambda item: item.position):
        if isinstance(channel, discord.CategoryChannel):
            continue
        if not isinstance(channel, supported_types):
            unsupported_channels += 1
            continue
        channel_data = {
            "id": channel.id,
            "name": channel.name,
            "type": (
                "stage"
                if isinstance(channel, discord.StageChannel)
                else "voice"
                if isinstance(channel, discord.VoiceChannel)
                else "text"
            ),
            "position": channel.position,
            "category_id": channel.category_id,
            "category_name": channel.category.name if channel.category else None,
            "overwrites": serialize_permission_overwrites(channel.overwrites),
        }
        if isinstance(channel, discord.TextChannel):
            channel_data.update(
                topic=channel.topic,
                nsfw=channel.nsfw,
                slowmode_delay=channel.slowmode_delay,
                default_auto_archive_duration=channel.default_auto_archive_duration,
            )
        else:
            channel_data.update(
                bitrate=channel.bitrate,
                user_limit=channel.user_limit,
                rtc_region=channel.rtc_region,
                video_quality_mode=channel.video_quality_mode.value,
                nsfw=channel.nsfw,
            )
        channels.append(channel_data)

    return {
        "version": 1,
        "guild_id": guild.id,
        "guild_name": guild.name,
        "default_role_id": guild.default_role.id,
        "default_role_permissions": guild.default_role.permissions.value,
        "roles": roles,
        "categories": categories,
        "channels": channels,
        "unsupported_channel_count": unsupported_channels,
    }


def save_guild_backup(guild_id: int, snapshot: dict) -> int:
    snapshot_json = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    with connect_db() as db:
        cursor = db.execute(
            "INSERT INTO guild_backups (guild_id, snapshot_json) VALUES (?, ?)",
            (guild_id, snapshot_json),
        )
        return cursor.lastrowid


def get_latest_guild_backup(guild_id: int):
    with connect_db() as db:
        row = db.execute(
            """
            SELECT backup_id, created_at, snapshot_json
            FROM guild_backups
            WHERE guild_id = ?
            ORDER BY backup_id DESC
            LIMIT 1
            """,
            (guild_id,),
        ).fetchone()
    if row is None:
        return None
    backup_id, created_at, snapshot_json = row
    return backup_id, created_at, json.loads(snapshot_json)


def resolve_backup_overwrites(
    guild: discord.Guild,
    serialized_overwrites: list[dict],
    role_map: dict[int, discord.Role],
):
    overwrites = {}
    for item in serialized_overwrites:
        if item["target_type"] == "role":
            target_id = item["target_id"]
            target = guild.default_role if target_id == guild.default_role.id else role_map.get(target_id)
            if target is None:
                target = discord.utils.get(guild.roles, name=item.get("target_name"))
        else:
            target = guild.get_member(item["target_id"])
            if target is None:
                target = discord.Object(id=item["target_id"])
        if target is None:
            continue
        allow = discord.Permissions(item["allow"])
        deny = discord.Permissions(item["deny"])
        overwrites[target] = discord.PermissionOverwrite.from_pair(allow, deny)
    return overwrites


def find_backup_role(guild: discord.Guild, role_data: dict):
    role = guild.get_role(role_data["id"])
    if role and role != guild.default_role and not role.managed:
        return role
    return discord.utils.get(guild.roles, name=role_data["name"], managed=False)


def find_backup_channel(guild: discord.Guild, channel_data: dict):
    channel_id = channel_data["id"]
    channel = guild.get_channel(channel_id)
    expected_type = channel_data["type"]
    type_matches = (
        isinstance(channel, discord.TextChannel)
        if expected_type == "text"
        else isinstance(channel, discord.StageChannel)
        if expected_type == "stage"
        else isinstance(channel, discord.VoiceChannel)
    )
    if channel and type_matches:
        return channel
    for candidate in guild.channels:
        if candidate.name != channel_data["name"]:
            continue
        if expected_type == "text" and isinstance(candidate, discord.TextChannel):
            return candidate
        if expected_type == "stage" and isinstance(candidate, discord.StageChannel):
            return candidate
        if (
            expected_type == "voice"
            and isinstance(candidate, discord.VoiceChannel)
            and not isinstance(candidate, discord.StageChannel)
        ):
            return candidate
    return None


async def apply_guild_backup(guild: discord.Guild, snapshot: dict) -> dict:
    if snapshot.get("version") != 1 or snapshot.get("guild_id") != guild.id:
        raise ValueError("backup_server_mismatch")

    result = {
        "guild_name_updated": False,
        "default_role_updated": False,
        "roles_created": 0,
        "roles_updated": 0,
        "categories_created": 0,
        "categories_updated": 0,
        "channels_created": 0,
        "channels_updated": 0,
        "errors": [],
    }
    if snapshot.get("guild_name") and snapshot["guild_name"] != guild.name:
        try:
            await guild.edit(
                name=snapshot["guild_name"],
                reason="Restore server name from server backup",
            )
            result["guild_name_updated"] = True
        except discord.HTTPException as error:
            logger.exception("Could not restore guild name for guild %s", guild.id)
            result["errors"].append(f"서버 이름: {error}")
    default_role_permissions = snapshot.get("default_role_permissions")
    if (
        default_role_permissions is not None
        and guild.default_role.permissions.value != default_role_permissions
    ):
        try:
            await guild.default_role.edit(
                permissions=discord.Permissions(default_role_permissions),
                reason="Restore @everyone permissions from server backup",
            )
            result["default_role_updated"] = True
        except discord.HTTPException as error:
            logger.exception(
                "Could not restore @everyone permissions in guild %s", guild.id
            )
            result["errors"].append(f"@everyone 권한: {error}")

    role_map = {}
    role_positions = {}
    for role_data in snapshot["roles"]:
        role = find_backup_role(guild, role_data)
        try:
            if role is None:
                role = await guild.create_role(
                    name=role_data["name"],
                    permissions=discord.Permissions(role_data["permissions"]),
                    color=discord.Colour(role_data["color"]),
                    hoist=role_data["hoist"],
                    mentionable=role_data["mentionable"],
                    reason="Restore role from server backup",
                )
                result["roles_created"] += 1
            else:
                role = await role.edit(
                    name=role_data["name"],
                    permissions=discord.Permissions(role_data["permissions"]),
                    color=discord.Colour(role_data["color"]),
                    hoist=role_data["hoist"],
                    mentionable=role_data["mentionable"],
                    reason="Restore role from server backup",
                )
                result["roles_updated"] += 1
            role_map[role_data["id"]] = role
            role_positions[role] = role_data["position"]
        except discord.HTTPException as error:
            logger.exception(
                "Could not restore role %r in guild %s",
                role_data["name"],
                guild.id,
            )
            result["errors"].append(f"역할 {role_data['name']}: {error}")

    if role_positions:
        try:
            await guild.edit_role_positions(
                role_positions,
                reason="Restore role order from server backup",
            )
        except discord.HTTPException as error:
            logger.exception("Could not restore role positions in guild %s", guild.id)
            result["errors"].append(f"역할 순서: {error}")

    category_map = {}
    for category_data in snapshot["categories"]:
        category = guild.get_channel(category_data["id"])
        if not isinstance(category, discord.CategoryChannel):
            category = discord.utils.get(
                guild.categories, name=category_data["name"]
            )
        overwrites = resolve_backup_overwrites(
            guild, category_data["overwrites"], role_map
        )
        try:
            if category is None:
                category = await guild.create_category(
                    name=category_data["name"],
                    overwrites=overwrites,
                    position=category_data["position"],
                    reason="Restore category from server backup",
                )
                result["categories_created"] += 1
            else:
                await category.edit(
                    name=category_data["name"],
                    overwrites=overwrites,
                    position=category_data["position"],
                    reason="Restore category from server backup",
                )
                result["categories_updated"] += 1
            category_map[category_data["id"]] = category
        except discord.HTTPException as error:
            logger.exception(
                "Could not restore category %r in guild %s",
                category_data["name"],
                guild.id,
            )
            result["errors"].append(f"카테고리 {category_data['name']}: {error}")

    for channel_data in snapshot["channels"]:
        channel_type = channel_data["type"]
        category = category_map.get(channel_data.get("category_id"))
        if category is None and channel_data.get("category_name"):
            category = discord.utils.get(
                guild.categories, name=channel_data["category_name"]
            )
        overwrites = resolve_backup_overwrites(
            guild, channel_data["overwrites"], role_map
        )
        channel = find_backup_channel(guild, channel_data)
        common_options = {
            "name": channel_data["name"],
            "category": category,
            "position": channel_data["position"],
            "overwrites": overwrites,
            "reason": "Restore channel from server backup",
        }
        if channel_type == "text":
            specific_options = {
                "topic": channel_data["topic"],
                "nsfw": channel_data["nsfw"],
                "slowmode_delay": channel_data["slowmode_delay"],
                "default_auto_archive_duration": channel_data[
                    "default_auto_archive_duration"
                ],
            }
        else:
            specific_options = {
                "bitrate": channel_data["bitrate"],
                "user_limit": channel_data["user_limit"],
                "rtc_region": channel_data["rtc_region"],
                "video_quality_mode": discord.VideoQualityMode(
                    channel_data["video_quality_mode"]
                ),
                "nsfw": channel_data["nsfw"],
            }
        try:
            if channel is None:
                if channel_type == "text":
                    channel = await guild.create_text_channel(
                        **common_options, **specific_options
                    )
                elif channel_type == "stage":
                    channel = await guild.create_stage_channel(
                        **common_options, **specific_options
                    )
                else:
                    channel = await guild.create_voice_channel(
                        **common_options, **specific_options
                    )
                result["channels_created"] += 1
            else:
                await channel.edit(
                    **common_options,
                    **specific_options,
                )
                result["channels_updated"] += 1
        except discord.HTTPException as error:
            logger.exception(
                "Could not restore channel %r in guild %s",
                channel_data["name"],
                guild.id,
            )
            result["errors"].append(f"채널 {channel_data['name']}: {error}")
    result["unsupported_channel_count"] = snapshot.get(
        "unsupported_channel_count", 0
    )
    return result


class GuildBackupRestoreView(SafeInteractionView):
    def __init__(
        self, guild_id: int, requester_id: int, snapshot: dict, backup_id: int
    ):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.requester_id = requester_id
        self.snapshot = snapshot
        self.backup_id = backup_id

    def is_authorized(self, interaction: discord.Interaction) -> bool:
        return (
            interaction.guild_id == self.guild_id
            and interaction.user.id == self.requester_id
            and interaction.user.guild_permissions.administrator
        )

    @discord.ui.button(label="복구 시작", style=discord.ButtonStyle.danger)
    async def confirm_restore(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if not self.is_authorized(interaction):
            await interaction.response.send_message(
                "복구를 시작할 권한이 없습니다.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await apply_guild_backup(interaction.guild, self.snapshot)
        except ValueError as error:
            if str(error) == "backup_server_mismatch":
                await interaction.edit_original_response(
                    content="이 백업은 현재 서버에서 만든 백업이 아니므로 복구할 수 없습니다."
                )
                return
            raise
        await interaction.edit_original_response(
            content=format_guild_restore_result(self.backup_id, result),
            view=None,
        )
        self.stop()

    @discord.ui.button(label="취소", style=discord.ButtonStyle.secondary)
    async def cancel_restore(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        if not self.is_authorized(interaction):
            await interaction.response.send_message(
                "이 확인창을 취소할 권한이 없습니다.", ephemeral=True
            )
            return
        await interaction.response.edit_message(
            content="서버 복구를 취소했습니다.", view=None
        )
        self.stop()


def format_guild_restore_result(backup_id: int, result: dict) -> str:
    restored_settings = []
    if result.get("guild_name_updated"):
        restored_settings.append("서버 이름 복구")
    if result.get("default_role_updated"):
        restored_settings.append("@everyone 권한 복구")
    settings_summary = (
        " ".join(restored_settings) + ". " if restored_settings else ""
    )
    summary = (
        f"백업 #{backup_id} 복구 작업을 마쳤습니다. {settings_summary}"
        f"역할 {result['roles_created']}개 생성/{result['roles_updated']}개 갱신, "
        f"카테고리 {result['categories_created']}개 생성/"
        f"{result['categories_updated']}개 갱신, "
        f"채널 {result['channels_created']}개 생성/"
        f"{result['channels_updated']}개 갱신."
    )
    notes = ["기존 백업에 없는 역할이나 채널은 삭제하지 않았습니다."]
    if result["unsupported_channel_count"]:
        notes.append(
            f"지원하지 않는 채널 {result['unsupported_channel_count']}개는 건너뛰었습니다."
        )
    if result["errors"]:
        notes.append(f"실패 {len(result['errors'])}건: " + "; ".join(result["errors"][:5]))
        if len(result["errors"]) > 5:
            notes.append(f"그 외 실패 {len(result['errors']) - 5}건은 봇 로그를 확인하세요.")
    return (summary + "\n" + "\n".join(notes))[:1900]


class SponsorBot(commands.Bot):
    async def setup_hook(self):
        initialize_db()
        self.add_view(TicketSetupView())
        self.add_view(TicketCloseView())
        self.add_view(TicketControlView())
        self.add_view(ServerMainDashboardView())
        self.add_view(CaptchaVerifyView())
        synced_commands = await self.tree.sync()
        logger.info("Synced %s Discord application commands", len(synced_commands))
        self.synced_guild_ids = set()

    async def sync_guild_commands(self):
        for guild in self.guilds:
            if guild.id in self.synced_guild_ids:
                continue
            target = discord.Object(id=guild.id)
            self.tree.copy_global_to(guild=target)
            try:
                synced_commands = await self.tree.sync(guild=target)
            except discord.HTTPException:
                logger.exception(
                    "Could not sync application commands to guild %s",
                    guild.id,
                )
                continue
            self.synced_guild_ids.add(guild.id)
            logger.info(
                "Synced %s application commands to guild %s",
                len(synced_commands),
                guild.id,
            )


bot = SponsorBot(command_prefix=PREFIX, intents=intents, help_command=None)


@bot.tree.command(name="도움말", description="봇 명령어를 안내합니다.")
@discord.app_commands.guild_only()
async def slash_help_command(interaction: discord.Interaction):
    embed = discord.Embed(title="봇 명령어 안내", color=0x5865F2)
    embed.description = (
        "서버원 명령어는 `/서버원명령어`, 관리자 명령어는 "
        "`/관리자명령어`를 입력해 확인하세요.\n"
        f"기존 접두사 명령어도 사용할 수 있습니다. (현재 접두사: `{PREFIX}`)"
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="서버원명령어", description="서버원이 사용할 수 있는 명령어를 보여줍니다.")
@discord.app_commands.guild_only()
async def slash_member_help(interaction: discord.Interaction):
    embed = discord.Embed(title="서버원 명령어", color=0x5865F2)
    embed.description = (
        "`/후원랭킹` — 서버 후원 랭킹\n"
        "`/코인잔액` — 내 코인 잔액 또는 지정한 유저의 잔액\n"
        "`/유저정보` — 가입일, 경고, 후원 및 역할 정보\n"
        "`/도움말` — 명령어 안내"
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="관리자명령어", description="관리자 전용 명령어를 보여줍니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_admin_help(interaction: discord.Interaction):
    embed = discord.Embed(title="관리자 명령어", color=0xE67E22)
    embed.add_field(
        name="패널 및 후원 관리",
        value=(
            "`/티켓패널생성` — 후원/문의 패널\n"
            "`/설화전광판설치` — 후원 대시보드\n"
            "`/설화목표설정` — 시즌 목표 설정\n"
            "`/설화누적설정 @유저 금액` — 현재 누적액을 지정하고 해당 유저 기록에 차액 반영\n"
            "`/후원금액수정 @유저 금액` — TOP 3 표시 후원액 수정\n"
            "`/후원등록` — 후원 금액 수동 등록 (후원액과 같은 코인 지급)\n"
            "`/코인삭제 @유저 수량` — 코인 및 같은 금액의 후원액 차감\n"
            "`/후원차감 @유저 금액` — 입력한 금액만큼 후원액과 코인 차감\n"
            "`/태그목록` — 태그 완료/미착용 전체 명단 파일 제공\n"
            "`/서버저장`, `/서버복구` — 서버 구조 백업 및 확인 후 복구\n"
            "`/인증패널배포` — 신규 유저 인증 패널\n"
            "후원 티켓의 **후원 금액 승인** 버튼 — 후원 등록 및 후원액과 같은 코인 지급"
        ),
        inline=False,
    )
    embed.add_field(
        name="서버 설정",
        value=(
            "`/설정 보기` — 현재 서버 설정 확인\n"
            "`/설정 관리자역할`, `/설정 인증역할`, `/설정 대기역할`\n"
            "`/설정 보이스생성방`, `/설정 잠수방생성방`, `/설정 로그채널`\n"
            "`/설정 태그현황채널 #음성채널` — 실시간 태그 인원 수 표시\n"
            "`/설정 금칙어추가`, `/설정 금칙어삭제`, `/설정 초기화`\n"
            "`/공지생성` — 공지 임베드 게시"
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="서버저장", description="현재 서버 역할, 카테고리, 채널 구조를 백업 파일로 저장합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_save_guild_backup(interaction: discord.Interaction):
    snapshot = create_guild_snapshot(interaction.guild)
    backup_id = save_guild_backup(interaction.guild_id, snapshot)
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    backup_file = discord.File(
        io.BytesIO(payload),
        filename=f"server_backup_{interaction.guild_id}_{backup_id}.json",
    )
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        await interaction.followup.send(
            content=(
                f"서버 구조 백업 #{backup_id}을 저장하고 JSON 파일을 첨부했습니다.\n"
                f"역할 {len(snapshot['roles'])}개, 카테고리 {len(snapshot['categories'])}개, "
                f"지원 채널 {len(snapshot['channels'])}개를 저장했습니다. "
                f"지원하지 않는 채널 {snapshot['unsupported_channel_count']}개는 제외했습니다. "
                "첨부 파일은 봇 PC와 별도 위치에도 보관해 주세요."
            ),
            file=backup_file,
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception("Could not send guild backup file for guild %s", interaction.guild_id)
        await interaction.followup.send(
            f"백업 #{backup_id}은 봇 데이터베이스에 저장했지만 파일 첨부에 실패했습니다. "
            "서버 규모가 커 파일 크기 제한을 넘었을 수 있습니다.",
            ephemeral=True,
        )
    finally:
        backup_file.close()


@bot.tree.command(name="서버복구", description="가장 최근 서버 구조 백업을 확인하고 복구합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_restore_guild_backup(interaction: discord.Interaction):
    latest = get_latest_guild_backup(interaction.guild_id)
    if latest is None:
        await interaction.response.send_message(
            "저장된 서버 백업이 없습니다. 먼저 `/서버저장`을 실행해 주세요.",
            ephemeral=True,
        )
        return
    backup_id, created_at, snapshot = latest
    if snapshot.get("guild_id") != interaction.guild_id:
        await interaction.response.send_message(
            "이 백업은 현재 서버에서 만든 백업이 아니므로 복구할 수 없습니다.",
            ephemeral=True,
        )
        return
    preview = (
        f"백업 #{backup_id} ({created_at})을 복구할까요?\n"
        f"서버 이름과 @everyone 권한, 역할 {len(snapshot['roles'])}개, "
        f"카테고리 {len(snapshot['categories'])}개, "
        f"채널 {len(snapshot['channels'])}개가 대상입니다.\n"
        "백업에 없는 기존 항목은 삭제하지 않습니다. 메시지, 멤버 역할 배정, "
        "이모지 및 연동은 복구 대상이 아닙니다. 복구는 확인 후 시작됩니다."
    )
    await interaction.response.send_message(
        content=preview,
        view=GuildBackupRestoreView(
            interaction.guild_id,
            interaction.user.id,
            snapshot,
            backup_id,
        ),
        ephemeral=True,
    )


@bot.command(name="서버저장")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def save_guild_backup_command(ctx: commands.Context):
    snapshot = create_guild_snapshot(ctx.guild)
    backup_id = save_guild_backup(ctx.guild.id, snapshot)
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    backup_file = discord.File(
        io.BytesIO(payload),
        filename=f"server_backup_{ctx.guild.id}_{backup_id}.json",
    )
    try:
        await ctx.send(
            content=(
                f"서버 구조 백업 #{backup_id}을 저장했습니다. "
                f"역할 {len(snapshot['roles'])}개, 카테고리 {len(snapshot['categories'])}개, "
                f"지원 채널 {len(snapshot['channels'])}개. "
                f"지원하지 않는 채널 {snapshot['unsupported_channel_count']}개는 제외했습니다. "
                "첨부 JSON은 봇 PC와 별도 위치에도 보관해 주세요."
            ),
            file=backup_file,
        )
    except discord.HTTPException:
        logger.exception("Could not send guild backup file for guild %s", ctx.guild.id)
        await ctx.send(
            f"백업 #{backup_id}은 봇 데이터베이스에 저장했지만 파일 첨부에 실패했습니다. "
            "서버 규모가 커 파일 크기 제한을 넘었을 수 있습니다."
        )
    finally:
        backup_file.close()


@bot.command(name="서버복구")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def restore_guild_backup_command(ctx: commands.Context):
    latest = get_latest_guild_backup(ctx.guild.id)
    if latest is None:
        await ctx.send("저장된 서버 백업이 없습니다. 먼저 `!서버저장`을 실행해 주세요.")
        return
    backup_id, created_at, snapshot = latest
    if snapshot.get("guild_id") != ctx.guild.id:
        await ctx.send("이 백업은 현재 서버에서 만든 백업이 아니므로 복구할 수 없습니다.")
        return
    await ctx.send(
        content=(
            f"백업 #{backup_id} ({created_at})을 복구할까요?\n"
            f"서버 이름과 @everyone 권한, 역할 {len(snapshot['roles'])}개, "
            f"카테고리 {len(snapshot['categories'])}개, "
            f"채널 {len(snapshot['channels'])}개가 대상입니다.\n"
            "백업에 없는 기존 항목은 삭제하지 않습니다. 메시지, 멤버 역할 배정, "
            "이모지 및 연동은 복구 대상이 아닙니다. 버튼을 눌러 복구를 확인해 주세요."
        ),
        view=GuildBackupRestoreView(
            ctx.guild.id,
            ctx.author.id,
            snapshot,
            backup_id,
        ),
    )


@bot.tree.command(name="후원랭킹", description="서버의 후원 랭킹을 보여줍니다.")
@discord.app_commands.guild_only()
async def slash_sponsor_ranking(interaction: discord.Interaction):
    rows = get_sponsor_ranking(interaction.guild_id)
    embed = discord.Embed(title="후원 명예의 전당 Top 5", color=0xFFD700)
    if not rows:
        embed.description = "아직 등록된 후원 내역이 없습니다."
    else:
        medals = ("🥇", "🥈", "🥉", "🏅", "🏅")
        embed.description = "\n".join(
            f"{medals[rank - 1]} **{rank}위** <@{user_id}> — `{amount:,}원`"
            for rank, (user_id, amount) in enumerate(rows, start=1)
        )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="코인잔액", description="본인 또는 선택한 서버원의 코인 잔액을 확인합니다.")
@discord.app_commands.guild_only()
async def slash_coin_balance(
    interaction: discord.Interaction,
    member: discord.Member = None,
):
    member = member or interaction.user
    balance = get_coin_balance(interaction.guild_id, member.id)
    await interaction.response.send_message(
        f"{member.mention}님의 보유 코인: **{balance:,} SNOW**"
    )


@bot.tree.command(name="코인삭제", description="관리자가 서버원의 코인을 삭제하고 후원 합계에서도 차감합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_deduct_coins(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: discord.app_commands.Range[int, 1, 1_000_000_000],
):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        balance, sponsor_total = deduct_member_coins_and_sponsorship(
            interaction.guild_id, member.id, amount
        )
    except ValueError as error:
        if str(error) == "insufficient_coins":
            message = f"{member.mention}님의 코인이 부족합니다. 현재 잔액은 `{get_coin_balance(interaction.guild_id, member.id):,} SNOW`입니다."
        else:
            message = (
                f"{member.mention}님의 누적 후원액이 차감할 금액보다 적어 처리하지 않았습니다. "
                f"현재 후원액은 `{get_sponsor_total(interaction.guild_id, member.id):,}원`입니다."
            )
        await interaction.edit_original_response(content=message)
        return

    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(
        content=(
            f"{member.mention}님의 코인 `{amount:,} SNOW`와 후원액 `{amount:,}원`을 차감했습니다.\n"
            f"남은 코인: `{balance:,} SNOW` · 남은 누적 후원액: `{sponsor_total:,}원`"
        )
    )


@bot.tree.command(name="후원차감", description="입력한 금액만큼 코인과 누적 후원액을 차감합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_deduct_sponsorship(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: discord.app_commands.Range[int, 1, 1_000_000_000],
):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        balance, sponsor_total = deduct_member_coins_and_sponsorship(
            interaction.guild_id, member.id, amount
        )
    except ValueError as error:
        if str(error) == "insufficient_coins":
            message = (
                f"{member.mention}님의 코인이 부족합니다. "
                f"현재 잔액은 `{get_coin_balance(interaction.guild_id, member.id):,} SNOW`입니다."
            )
        else:
            message = (
                f"{member.mention}님의 누적 후원액이 차감할 금액보다 적어 처리하지 않았습니다. "
                f"현재 후원액은 `{get_sponsor_total(interaction.guild_id, member.id):,}원`입니다."
            )
        await interaction.edit_original_response(content=message)
        return

    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(
        content=(
            f"{member.mention}님의 후원액 `{amount:,}원`과 코인 `{amount:,} SNOW`를 차감했습니다.\n"
            f"남은 후원액: `{sponsor_total:,}원` · 남은 코인: `{balance:,} SNOW`"
        )
    )


@bot.tree.command(name="유저정보", description="서버원의 가입일, 경고, 후원 및 역할 정보를 확인합니다.")
@discord.app_commands.guild_only()
async def slash_user_info(
    interaction: discord.Interaction,
    member: discord.Member = None,
):
    member = member or interaction.user
    embed = discord.Embed(title=f"{member.display_name} 유저 정보", color=member.color)
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="계정 ID", value=str(member.id), inline=True)
    embed.add_field(
        name="서버 가입일",
        value=discord.utils.format_dt(member.joined_at, "D") if member.joined_at else "확인 불가",
        inline=True,
    )
    embed.add_field(
        name="누적 경고",
        value=f"{get_warning_count(interaction.guild_id, member.id)}회",
        inline=True,
    )
    embed.add_field(
        name="누적 후원",
        value=f"{get_sponsor_total(interaction.guild_id, member.id):,}원",
        inline=True,
    )
    roles = [role.mention for role in member.roles if role != interaction.guild.default_role]
    embed.add_field(name="역할", value=", ".join(roles) or "없음", inline=False)
    await interaction.response.send_message(embed=embed)


def build_tag_report_files(tagged_lines: list[str], untagged_lines: list[str]):
    max_file_bytes = 8 * 1024 * 1024
    sections = [
        "=== 태그 완료 명단 ===\n",
        *tagged_lines,
        "\n=== 태그 미착용 명단 ===\n",
        *untagged_lines,
    ]
    chunks = []
    current = bytearray()
    for line in sections:
        encoded = line.encode("utf-8")
        if current and len(current) + len(encoded) > max_file_bytes:
            chunks.append(bytes(current))
            current.clear()
        current.extend(encoded)
    if current or not chunks:
        chunks.append(bytes(current))
    return [
        discord.File(
            io.BytesIO(chunk),
            filename=(
                "tag_check_result.txt"
                if len(chunks) == 1
                else f"tag_check_result_{index}.txt"
            ),
        )
        for index, chunk in enumerate(chunks, start=1)
    ]


def get_native_tag_for_member(member: discord.Member, guild_id: int) -> str | None:
    primary_guild = getattr(member, "primary_guild", None)
    if (
        primary_guild
        and primary_guild.id == guild_id
        and primary_guild.tag
        and primary_guild.tag.strip()
    ):
        return primary_guild.tag.strip()
    return None


def is_tagged_member(
    member: discord.Member, guild_id: int, native_tag_names: set[str]
) -> bool:
    if member.bot:
        return False
    primary_guild = getattr(member, "primary_guild", None)
    has_native_tag = bool(
        primary_guild
        and primary_guild.identity_enabled is not False
        and (
            primary_guild.id == guild_id
            or (
                primary_guild.id is None
                and primary_guild.tag
                and primary_guild.tag.casefold() in native_tag_names
            )
        )
    )
    text_tags = {SERVER_TAG.casefold(), *native_tag_names}
    names = (
        member.nick or "",
        member.name,
        member.display_name,
        member.global_name or "",
    )
    has_name_tag = any(
        tag and tag in name.casefold()
        for name in names
        for tag in text_tags
    )
    return has_native_tag or has_name_tag


def build_tag_member_data(guild: discord.Guild, members: list[discord.Member]):
    tagged_lines = []
    untagged_lines = []
    native_tag_names = sorted({
        tag
        for member in members
        if (tag := get_native_tag_for_member(member, guild.id))
    }, key=str.casefold)
    native_tag_keys = {tag.casefold() for tag in native_tag_names}
    tagged_ids = set()

    for member in members:
        if member.bot:
            continue
        is_tagged = is_tagged_member(member, guild.id, native_tag_keys)
        primary_guild = getattr(member, "primary_guild", None)
        has_official_tag = bool(
            primary_guild
            and primary_guild.identity_enabled is not False
            and (
                primary_guild.id == guild.id
                or (
                    primary_guild.id is None
                    and primary_guild.tag
                    and primary_guild.tag.casefold() in native_tag_keys
                )
            )
        )
        names = (
            member.nick or "",
            member.name,
            member.display_name,
            member.global_name or "",
        )
        has_name_tag = any(
            tag and tag in name.casefold()
            for name in names
            for tag in {SERVER_TAG.casefold(), *native_tag_keys}
        )
        line = (
            f"{member} ({member.id}) — "
            f"공식 서버 태그: {'착용' if has_official_tag else '미착용'} / "
            f"닉네임 태그: {'포함' if has_name_tag else '미포함'}\n"
        )
        if is_tagged:
            tagged_lines.append(line)
            tagged_ids.add(member.id)
        else:
            untagged_lines.append(line)

    return tagged_lines, untagged_lines, native_tag_names, tagged_ids


async def fetch_tag_member_lists(guild: discord.Guild):
    members = [member async for member in guild.fetch_members(limit=None)]
    tagged, untagged, native_tag_names, _ = build_tag_member_data(guild, members)
    return tagged, untagged, native_tag_names


def get_tag_counter_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in tag_counter_locks:
        tag_counter_locks[guild_id] = asyncio.Lock()
    return tag_counter_locks[guild_id]


def get_tag_counter_channel_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in tag_counter_channel_locks:
        tag_counter_channel_locks[guild_id] = asyncio.Lock()
    return tag_counter_channel_locks[guild_id]


def clear_tag_counter_cache(guild_id: int):
    tagged_member_ids.pop(guild_id, None)
    native_tag_by_member.pop(guild_id, None)
    tag_counter_requested_names.pop(guild_id, None)
    task = tag_counter_update_tasks.pop(guild_id, None)
    if task and not task.done():
        task.cancel()


async def update_tag_counter_channel(guild: discord.Guild):
    channel_id = get_setting(guild.id, "tag_count_channel_id")
    if not channel_id:
        return
    async with get_tag_counter_channel_lock(guild.id):
        channel = guild.get_channel(int(channel_id))
        if not isinstance(channel, discord.VoiceChannel):
            logger.warning(
                "Tag count voice channel %s is missing in guild %s",
                channel_id,
                guild.id,
            )
            return
        count = len(tagged_member_ids.get(guild.id, set()))
        channel_name = f"{TAG_COUNT_CHANNEL_PREFIX}{count}"
        request_key = (channel.id, channel_name)
        if channel.name == channel_name:
            tag_counter_requested_names[guild.id] = request_key
            return
        if tag_counter_requested_names.get(guild.id) == request_key:
            return
        await channel.edit(
            name=channel_name,
            reason="Update live server-tag member count",
        )
        tag_counter_requested_names[guild.id] = request_key


async def refresh_tag_counter(guild: discord.Guild):
    async with get_tag_counter_lock(guild.id):
        members = [member async for member in guild.fetch_members(limit=None)]
        _, _, _, tagged_ids = build_tag_member_data(guild, members)
        tagged_member_ids[guild.id] = tagged_ids
        native_tag_by_member[guild.id] = {
            member.id: tag
            for member in members
            if (tag := get_native_tag_for_member(member, guild.id))
        }
    await update_tag_counter_channel(guild)


async def update_tag_counter_for_member(
    member: discord.Member, *, removed: bool = False
):
    guild = member.guild
    if not get_setting(guild.id, "tag_count_channel_id"):
        return
    if guild.id not in tagged_member_ids:
        await refresh_tag_counter(guild)
        return

    refresh_required = False
    async with get_tag_counter_lock(guild.id):
        member_tags = native_tag_by_member.setdefault(guild.id, {})
        previous_tag_names = {tag.casefold() for tag in member_tags.values()}
        tagged_ids = tagged_member_ids[guild.id]
        member_was_tagged = member.id in tagged_ids
        if removed:
            member_tags.pop(member.id, None)
            member_is_tagged = False
        else:
            tag = get_native_tag_for_member(member, guild.id)
            if tag:
                member_tags[member.id] = tag
            else:
                member_tags.pop(member.id, None)
            current_tag_names = {value.casefold() for value in member_tags.values()}
            if current_tag_names != previous_tag_names:
                refresh_required = True
            else:
                member_is_tagged = is_tagged_member(
                    member, guild.id, current_tag_names
                )
        if not refresh_required:
            if member_is_tagged:
                tagged_ids.add(member.id)
            else:
                tagged_ids.discard(member.id)
            changed = member_was_tagged != member_is_tagged
        else:
            changed = False

    if refresh_required:
        await refresh_tag_counter(guild)
    elif changed:
        schedule_tag_counter_channel_update(guild)


def schedule_tag_counter_channel_update(guild: discord.Guild):
    previous_task = tag_counter_update_tasks.get(guild.id)
    if previous_task and not previous_task.done():
        previous_task.cancel()
    tag_counter_update_tasks[guild.id] = asyncio.create_task(
        delayed_tag_counter_channel_update(guild)
    )


async def delayed_tag_counter_channel_update(guild: discord.Guild):
    try:
        await asyncio.sleep(2)
        await update_tag_counter_channel(guild)
    except asyncio.CancelledError:
        raise
    except discord.HTTPException:
        logger.exception("Could not update tag count channel in guild %s", guild.id)
    finally:
        current_task = asyncio.current_task()
        if tag_counter_update_tasks.get(guild.id) is current_task:
            tag_counter_update_tasks.pop(guild.id, None)


async def reconcile_tag_counters():
    await bot.wait_until_ready()
    while not bot.is_closed():
        for guild in bot.guilds:
            if not get_setting(guild.id, "tag_count_channel_id"):
                continue
            try:
                await refresh_tag_counter(guild)
            except discord.HTTPException:
                logger.exception(
                    "Could not reconcile tag counter for guild %s",
                    guild.id,
                )
        await asyncio.sleep(TAG_COUNTER_RECONCILE_INTERVAL)


@bot.tree.command(
    name="태그목록",
    description="서버 태그 착용자와 미착용자 전체 명단을 파일로 확인합니다.",
)
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(manage_nicknames=True)
@discord.app_commands.checks.has_permissions(manage_nicknames=True)
async def slash_tag_member_list(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    guild = interaction.guild
    if guild is None:
        await interaction.edit_original_response(
            content="이 명령어는 서버 안에서만 사용할 수 있습니다."
        )
        return
    try:
        tagged_lines, untagged_lines, native_tag_names = await fetch_tag_member_lists(guild)
    except discord.HTTPException:
        logger.exception("Could not fetch members for tag list in guild %s", guild.id)
        await interaction.edit_original_response(
            content="서버원 목록을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요."
        )
        return

    report_files = build_tag_report_files(tagged_lines, untagged_lines)
    embed = discord.Embed(
        title="서버 태그 명단",
        description=(
            f"공식 서버 태그: `{', '.join(native_tag_names)}`\n"
            f"닉네임 태그 문구: `{SERVER_TAG or '미설정'}`"
            if native_tag_names
            else f"공식 서버 태그: 장착된 태그에서 자동 확인 불가\n"
            f"닉네임 태그 문구: `{SERVER_TAG or '미설정'}`"
        ),
        color=0x5865F2,
    )
    embed.add_field(name="✅ 태그 완료", value=f"{len(tagged_lines)}명", inline=True)
    embed.add_field(name="❌ 태그 미착용", value=f"{len(untagged_lines)}명", inline=True)
    embed.set_footer(text="전체 명단은 첨부된 tag_check_result.txt 파일에서 확인할 수 있습니다.")
    try:
        await interaction.followup.send(
            embed=embed,
            files=report_files,
        )
    finally:
        for report_file in report_files:
            report_file.close()
@bot.tree.command(name="티켓패널생성", description="후원 및 문의 티켓 버튼 패널을 생성합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_ticket_panel(interaction: discord.Interaction):
    embed = build_ticket_panel_embed()
    await interaction.response.send_message(embed=embed, view=TicketSetupView())


@bot.tree.command(name="설화전광판설치", description="실시간 후원 대시보드와 티켓 버튼을 설치합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_setup_live_dashboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    embed = discord.Embed(
        title="🌌 설화 후원 실시간 라이브 전광판 🌌",
        description="후원 신청은 아래 버튼을 눌러 개인 티켓을 만들어 주세요.",
        color=0x4C00C7,
    )
    embed.add_field(name="💵 누적 총 후원금액", value="🏦 KRW 0원", inline=True)
    embed.add_field(name="🪙 유통 총 코인잔액", value="🪙 0 SNOW", inline=True)
    embed.add_field(
        name="📊 시즌 목표 달성도",
        value=make_premium_bar_graph(0, DEFAULT_SPONSOR_GOAL),
        inline=False,
    )
    channel = interaction.channel
    if channel is None:
        await interaction.edit_original_response(
            content="현재 채널을 확인할 수 없어 전광판을 설치하지 못했습니다."
        )
        return
    message = await channel.send(embed=embed, view=ServerMainDashboardView())
    save_dashboard(
        interaction.guild_id,
        interaction.channel_id,
        message.id,
        DEFAULT_SPONSOR_GOAL,
    )
    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(content="후원 전광판을 설치했습니다.")


@bot.tree.command(name="설화목표설정", description="후원 대시보드의 시즌 목표 금액을 설정합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_sponsor_goal(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if amount <= 0 or amount > 10_000_000_000:
        await interaction.edit_original_response(
            content="목표 금액은 1원 이상 100억 원 이하로 설정해 주세요."
        )
        return
    if get_dashboard(interaction.guild_id) is None:
        await interaction.edit_original_response(
            content="`/설화전광판설치` 명령어로 먼저 대시보드를 설치해 주세요."
        )
        return
    update_dashboard_goal(interaction.guild_id, amount)
    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(
        content=f"시즌 후원 목표를 **{amount:,}원**으로 설정했습니다."
    )


@bot.tree.command(
    name="설화누적설정",
    description="현재 누적 후원액을 설정하고 지정한 유저 기록에 차액을 반영합니다.",
)
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_sponsor_total(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: discord.app_commands.Range[int, 0, 10_000_000_000],
):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if get_dashboard(interaction.guild_id) is None:
        await interaction.edit_original_response(
            content="`/설화전광판설치` 명령어로 먼저 대시보드를 설치해 주세요."
        )
        return
    previous_total = get_guild_sponsor_stats(interaction.guild_id)[0]
    current_member_total = get_sponsor_total(interaction.guild_id, member.id)
    if previous_total == 0 and amount > 0:
        await interaction.edit_original_response(
            content=(
                "아직 후원 기록이 없습니다. 먼저 `/후원등록`으로 실제 후원을 등록한 뒤 "
                "누적 금액을 조정해 주세요."
            )
        )
        return
    try:
        previous_total, new_total, member_total = (
            set_guild_sponsor_total_by_adjusting_member(
                interaction.guild_id, member.id, amount
            )
        )
    except ValueError as error:
        if str(error) == "no_sponsor_records":
            await interaction.edit_original_response(
                content=(
                    "아직 후원 기록이 없습니다. 먼저 `/후원등록`으로 실제 후원을 등록한 뒤 "
                    "누적 금액을 조정해 주세요."
                )
            )
            return
        await interaction.edit_original_response(
            content=(
                f"총액을 `{amount:,}원`으로 낮추려면 {member.mention}님의 후원 기록에서 "
                f"`{previous_total - amount:,}원`을 차감해야 하지만, 현재 기록은 "
                f"`{current_member_total:,}원`입니다. "
                "더 큰 후원 기록을 가진 유저를 선택해 주세요."
            )
        )
        return

    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(
        content=(
            f"누적 후원액을 `{previous_total:,}원`에서 `{new_total:,}원`으로 설정했습니다.\n"
            f"{member.mention}님의 후원 기록을 `{member_total:,}원`으로 조정했습니다. "
            "코인 잔액도 후원 기록에 맞춰 조정했습니다."
        )
    )


@bot.tree.command(
    name="후원금액수정",
    description="유저의 누적 후원 기록을 수정하고 랭킹 및 대시보드를 갱신합니다.",
)
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(administrator=True)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_edit_sponsor_amount(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: discord.app_commands.Range[int, 0, 10_000_000_000],
):
    await interaction.response.defer(ephemeral=True, thinking=True)
    previous_amount, new_guild_total = set_member_sponsor_total(
        interaction.guild_id, member.id, amount
    )
    await update_live_dashboard(interaction.guild)
    await interaction.edit_original_response(
        content=(
            f"{member.mention}님의 누적 후원액을 `{previous_amount:,}원`에서 "
            f"`{amount:,}원`으로 수정했습니다. 대시보드 누적액은 "
            f"`{new_guild_total:,}원`입니다. 코인 잔액도 후원 기록에 맞춰 조정했습니다."
        )
    )


@bot.tree.command(name="후원등록", description="선택한 서버원의 후원 금액을 등록합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_register_sponsor(
    interaction: discord.Interaction,
    member: discord.Member,
    amount: int,
):
    if amount <= 0:
        await interaction.response.send_message("금액은 1원 이상이어야 합니다.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    total = add_sponsor_points(interaction.guild_id, member.id, amount)
    balance = get_coin_balance(interaction.guild_id, member.id)
    await update_live_dashboard(interaction.guild)
    embed = discord.Embed(
        title="후원 등록 완료",
        description=(
            f"{member.mention}님의 후원 `{amount:,}원`과 코인 "
            f"`{amount:,} SNOW`를 반영했습니다."
        ),
        color=0x2ECC71,
    )
    embed.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
    embed.add_field(name="코인 잔액", value=f"{balance:,} SNOW", inline=True)
    await interaction.edit_original_response(embed=embed)
    receipt = discord.Embed(title="후원 감사 안내", color=0xFFD700)
    receipt.add_field(name="이번 후원", value=f"{amount:,}원", inline=True)
    receipt.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
    try:
        await member.send(embed=receipt)
    except discord.Forbidden:
        logger.info("Could not DM sponsor receipt to user %s", member.id)


@bot.tree.command(name="인증패널배포", description="신규 유저 인증 버튼 패널을 생성합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_deploy_captcha_panel(interaction: discord.Interaction):
    embed = discord.Embed(
        title="서버 가입 인증",
        description="아래 버튼을 눌러 인증을 완료해 주세요.",
        color=0x2ECC71,
    )
    await interaction.response.send_message(embed=embed, view=CaptchaVerifyView())


@bot.tree.command(name="공지생성", description="제목과 내용으로 공지 임베드를 게시합니다.")
@discord.app_commands.guild_only()
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_create_announcement(
    interaction: discord.Interaction,
    title: str,
    content: str,
):
    if not title.strip() or not content.strip():
        await interaction.response.send_message("제목과 내용을 모두 입력해 주세요.", ephemeral=True)
        return
    embed = discord.Embed(
        title=title.strip()[:256],
        description=content.strip()[:4000],
        color=0x2ECC71,
    )
    embed.set_footer(text=f"작성자: {interaction.user.display_name}")
    await interaction.response.send_message(embed=embed)


settings_app_group = discord.app_commands.Group(
    name="설정",
    description="서버별 역할, 채널, 금칙어 설정을 관리합니다.",
)


@settings_app_group.command(name="보기", description="현재 서버의 봇 설정을 확인합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_show_settings(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await interaction.edit_original_response(embed=setting_summary(interaction.guild))


@settings_app_group.command(name="관리자역할", description="티켓 운영진 역할을 설정합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_admin_role(interaction: discord.Interaction, role: discord.Role):
    set_setting(interaction.guild_id, "admin_role_id", str(role.id))
    await interaction.response.send_message(
        f"티켓 운영진 역할을 {role.mention}로 설정했습니다.", ephemeral=True
    )


@settings_app_group.command(name="인증역할", description="인증 완료 시 지급할 역할을 설정합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_verified_role(interaction: discord.Interaction, role: discord.Role):
    set_setting(interaction.guild_id, "verified_role_id", str(role.id))
    await interaction.response.send_message(
        f"인증 완료 역할을 {role.mention}로 설정했습니다.", ephemeral=True
    )


@settings_app_group.command(name="대기역할", description="신규 멤버에게 부여할 인증 대기 역할을 설정합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_unverified_role(interaction: discord.Interaction, role: discord.Role):
    set_setting(interaction.guild_id, "unverified_role_id", str(role.id))
    await interaction.response.send_message(
        f"인증 대기 역할을 {role.mention}로 설정했습니다.", ephemeral=True
    )


@settings_app_group.command(
    name="보이스생성방",
    description="2인 숙소 음성방을 만들 생성 대기 채널을 설정합니다.",
)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_voice_channel(
    interaction: discord.Interaction,
    channel: discord.VoiceChannel,
):
    sleep_channel_id = get_setting(interaction.guild_id, "voice_sleep_create_channel_id")
    if sleep_channel_id == str(channel.id):
        await interaction.response.send_message(
            "2인 숙소와 1인 잠수방 생성 채널은 서로 다르게 설정해 주세요.",
            ephemeral=True,
        )
        return
    set_setting(interaction.guild_id, "voice_create_channel_id", str(channel.id))
    await interaction.response.send_message(
        f"{channel.mention} 입장 시 **{VOICE_ROOM_NAME}** 방을 생성하도록 설정했습니다. (최대 2명)",
        ephemeral=True,
    )


@settings_app_group.command(
    name="잠수방생성방",
    description="1인 잠수방을 만들 생성 대기 채널을 설정합니다.",
)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_sleep_voice_channel(
    interaction: discord.Interaction,
    channel: discord.VoiceChannel,
):
    room_channel_id = get_setting(interaction.guild_id, "voice_create_channel_id")
    if room_channel_id == str(channel.id):
        await interaction.response.send_message(
            "2인 숙소와 1인 잠수방 생성 채널은 서로 다르게 설정해 주세요.",
            ephemeral=True,
        )
        return
    set_setting(interaction.guild_id, "voice_sleep_create_channel_id", str(channel.id))
    await interaction.response.send_message(
        f"{channel.mention} 입장 시 **{VOICE_SLEEP_ROOM_NAME}** 방을 생성하도록 설정했습니다. (최대 1명)",
        ephemeral=True,
    )


@settings_app_group.command(name="로그채널", description="보안 로그를 보낼 텍스트 채널을 설정합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_log_channel(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
):
    set_setting(interaction.guild_id, "security_log_channel_id", str(channel.id))
    await interaction.response.send_message(
        f"보안 로그 채널을 {channel.mention}로 설정했습니다.", ephemeral=True
    )


@settings_app_group.command(
    name="태그현황채널",
    description="실시간 태그 인원 수를 표시할 음성 채널을 설정합니다.",
)
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_set_tag_count_channel(
    interaction: discord.Interaction,
    channel: discord.VoiceChannel,
):
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "이 설정은 서버 안에서만 사용할 수 있습니다.", ephemeral=True
        )
        return
    set_setting(interaction.guild_id, "tag_count_channel_id", str(channel.id))
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        await refresh_tag_counter(guild)
    except discord.HTTPException:
        logger.exception(
            "Could not initialize tag count channel for guild %s",
            interaction.guild_id,
        )
        await interaction.edit_original_response(
            content=(
                f"{channel.mention}을 표시 채널로 저장했지만 인원 수를 갱신하지 못했습니다. "
                "봇의 서버원 조회 및 채널 관리 권한을 확인해 주세요."
            )
        )
        return
    await interaction.edit_original_response(
        content=(
            f"{channel.mention}을 실시간 태그 인원 표시 채널로 설정했습니다. "
            f"채널 이름은 `{TAG_COUNT_CHANNEL_PREFIX}{len(tagged_member_ids.get(interaction.guild_id, set()))}` 형식으로 갱신됩니다."
        )
    )


@settings_app_group.command(name="금칙어추가", description="서버 금칙어를 추가합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_add_filter_word(interaction: discord.Interaction, word: str):
    normalized = word.strip().casefold()
    words = list(get_filter_words(interaction.guild_id))
    if not normalized or len(normalized) > 100:
        await interaction.response.send_message(
            "금칙어는 1~100자로 입력해 주세요.", ephemeral=True
        )
        return
    if normalized in words:
        await interaction.response.send_message("이미 등록된 금칙어입니다.", ephemeral=True)
        return
    if len(words) >= 100:
        await interaction.response.send_message(
            "금칙어는 최대 100개까지 등록할 수 있습니다.", ephemeral=True
        )
        return
    words.append(normalized)
    set_setting(
        interaction.guild_id,
        "filter_words",
        json.dumps(words, ensure_ascii=False),
    )
    await interaction.response.send_message(
        f"금칙어 `{normalized}`를 추가했습니다.", ephemeral=True
    )


@settings_app_group.command(name="금칙어삭제", description="서버 금칙어를 삭제합니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
async def slash_remove_filter_word(interaction: discord.Interaction, word: str):
    normalized = word.strip().casefold()
    words = list(get_filter_words(interaction.guild_id))
    if normalized not in words:
        await interaction.response.send_message(
            "등록된 금칙어에서 찾지 못했습니다.", ephemeral=True
        )
        return
    words.remove(normalized)
    set_setting(
        interaction.guild_id,
        "filter_words",
        json.dumps(words, ensure_ascii=False),
    )
    await interaction.response.send_message(
        f"금칙어 `{normalized}`를 삭제했습니다.", ephemeral=True
    )


@settings_app_group.command(name="초기화", description="선택한 설정을 기본값으로 되돌립니다.")
@discord.app_commands.checks.has_permissions(administrator=True)
@discord.app_commands.choices(
    setting=[
        discord.app_commands.Choice(name="운영진 역할", value="admin_role_id"),
        discord.app_commands.Choice(name="인증 역할", value="verified_role_id"),
        discord.app_commands.Choice(name="대기 역할", value="unverified_role_id"),
        discord.app_commands.Choice(name="2인 숙소 생성 채널", value="voice_create_channel_id"),
        discord.app_commands.Choice(name="1인 잠수방 생성 채널", value="voice_sleep_create_channel_id"),
        discord.app_commands.Choice(name="보안 로그 채널", value="security_log_channel_id"),
        discord.app_commands.Choice(name="태그 현황 채널", value="tag_count_channel_id"),
        discord.app_commands.Choice(name="금칙어", value="filter_words"),
    ]
)
async def slash_reset_setting(
    interaction: discord.Interaction,
    setting: str,
):
    reset_setting(interaction.guild_id, setting)
    if setting == "tag_count_channel_id":
        clear_tag_counter_cache(interaction.guild_id)
    await interaction.response.send_message(
        "선택한 설정을 기본값으로 되돌렸습니다.", ephemeral=True
    )


bot.tree.add_command(settings_app_group)


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: discord.app_commands.AppCommandError,
):
    if isinstance(error, discord.app_commands.MissingPermissions):
        response = "이 명령어는 서버 관리자만 사용할 수 있습니다."
    else:
        logger.error(
            "Application command failed",
            exc_info=(type(error), error, error.__traceback__),
        )
        response = "명령어 처리 중 오류가 발생했습니다. 운영자에게 문의해 주세요."
    if interaction.is_expired() or is_interaction_acknowledgement_error(error):
        logger.warning(
            "Skipping application command error response for completed or expired interaction %s",
            interaction.id,
        )
        return
    try:
        await send_interaction_message(
            interaction,
            content=response,
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception(
            "Could not send application command error response for interaction %s",
            interaction.id,
        )


@bot.event
async def on_ready():
    global tag_counter_reconcile_task
    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id if bot.user else "unknown")
    await bot.sync_guild_commands()
    for channel_id in get_temp_voice_room_ids():
        channel = bot.get_channel(channel_id)
        if channel is None:
            remove_temp_voice_room(channel_id)
        elif isinstance(channel, discord.VoiceChannel) and not channel.members:
            await channel.delete(reason="Clean up empty temporary voice room after restart")
            remove_temp_voice_room(channel_id)
    for guild in bot.guilds:
        await update_live_dashboard(guild)
        if get_setting(guild.id, "tag_count_channel_id"):
            try:
                await refresh_tag_counter(guild)
            except discord.HTTPException:
                logger.exception(
                    "Could not initialize tag counter for guild %s",
                    guild.id,
                )
    if tag_counter_reconcile_task is None or tag_counter_reconcile_task.done():
        tag_counter_reconcile_task = asyncio.create_task(reconcile_tag_counters())
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="서버 후원 및 커뮤니티 관리",
        )
    )


@bot.command(name="도움말")
async def help_command(ctx: commands.Context):
    embed = discord.Embed(title="봇 명령어 안내", color=0x5865F2)
    embed.description = (
        "서버원 명령어는 `!서버원명령어`, 관리자 명령어는 "
        "`!관리자명령어`를 입력해 확인하세요."
    )
    embed.add_field(
        name="서버원 명령어",
        value="`!후원랭킹`, `!코인잔액`, `!유저정보` 등",
        inline=False,
    )
    if ctx.guild and ctx.author.guild_permissions.administrator:
        embed.add_field(
            name="관리자 명령어",
            value="`!관리자명령어`에서 확인",
            inline=False,
        )
    await ctx.send(embed=embed)


@bot.command(name="서버원명령어")
async def member_help_command(ctx: commands.Context):
    embed = discord.Embed(title="서버원 명령어", color=0x5865F2)
    embed.description = (
        "`!후원랭킹` — 서버 후원 랭킹\n"
        "`!코인잔액 [@유저]` — 내 코인 잔액 또는 지정한 유저의 잔액\n"
        "`!유저정보 [@유저]` — 가입일, 경고, 후원 및 역할 정보\n"
        "`!도움말` — 명령어 안내"
    )
    await ctx.send(embed=embed)


@bot.command(name="관리자명령어")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def admin_help_command(ctx: commands.Context):
    embed = discord.Embed(title="관리자 명령어", color=0xE67E22)
    embed.add_field(
        name="패널 및 후원 관리",
        value=(
            "`!티켓패널생성` — 후원/문의 패널 생성\n"
            "`!설화전광판설치` — 실시간 후원 대시보드 설치\n"
            "`!설화목표설정 금액` — 시즌 목표 설정\n"
            "`!설화누적설정 @유저 금액` — 현재 누적액 설정 및 후원 기록 차액 조정\n"
            "`!후원등록 @유저 금액` — 후원 금액 수동 등록 (후원액과 같은 코인 지급)\n"
            "`!후원금액수정 @유저 금액` — 유저 후원액 기록 수정\n"
            "`!코인삭제 @유저 수량` — 코인과 같은 금액의 후원액 차감\n"
            "`!후원차감 @유저 금액` — 입력한 금액만큼 후원액과 코인 차감\n"
            "`!서버저장`, `!서버복구` — 서버 구조 백업 및 확인 후 복구\n"
            "`/태그목록` — 태그 완료/미착용 전체 명단 파일 제공\n"
            "티켓의 **후원 금액 승인** 버튼 — 후원 등록 및 후원액과 같은 코인 지급"
        ),
        inline=False,
    )
    embed.add_field(
        name="서버 설정 및 관리",
        value=(
            "`!인증패널배포` — 신규 유저 인증 패널\n"
            "`!설정` — 서버 설정 확인\n"
            "`!설정 관리자역할 @역할`, `!설정 인증역할 @역할`\n"
            "`!설정 대기역할 @역할`, `!설정 보이스생성방 #채널`\n"
            "`!설정 잠수방생성방 #채널`\n"
            "`!설정 태그현황채널 #음성채널` — 실시간 태그 인원 수 표시\n"
            "`!설정 로그채널 #채널`\n"
            "`!설정 금칙어추가 단어`, `!설정 금칙어삭제 단어`\n"
            "`!설정초기화 항목` — 설정을 기본값으로 복원\n"
            "`!공지생성 제목 | 내용` — 공지 임베드 게시"
        ),
        inline=False,
    )
    await ctx.send(embed=embed)


def setting_summary(guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(title=f"{guild.name} 서버 설정", color=0x5865F2)
    admin_role = get_role(guild, "관리자")
    verified_role = get_role(guild, VERIFIED_ROLE_NAME)
    unverified_role = get_role(guild, UNVERIFIED_ROLE_NAME)

    voice_id = get_setting(guild.id, "voice_create_channel_id")
    voice_channel = guild.get_channel(int(voice_id)) if voice_id else None
    sleep_voice_id = get_setting(guild.id, "voice_sleep_create_channel_id")
    sleep_voice_channel = guild.get_channel(int(sleep_voice_id)) if sleep_voice_id else None
    log_id = get_setting(guild.id, "security_log_channel_id")
    log_channel = guild.get_channel(int(log_id)) if log_id else None
    tag_count_id = get_setting(guild.id, "tag_count_channel_id")
    tag_count_channel = guild.get_channel(int(tag_count_id)) if tag_count_id else None
    if log_channel is None:
        log_channel = discord.utils.get(guild.text_channels, name=SECURITY_LOG_CHANNEL_NAME)

    filters = get_filter_words(guild.id)
    embed.add_field(name="운영진 역할", value=admin_role.mention if admin_role else "미지정", inline=True)
    embed.add_field(name="인증 완료 역할", value=verified_role.mention if verified_role else VERIFIED_ROLE_NAME, inline=True)
    embed.add_field(name="인증 대기 역할", value=unverified_role.mention if unverified_role else UNVERIFIED_ROLE_NAME, inline=True)
    embed.add_field(
        name="2인 숙소 생성 대기 채널",
        value=voice_channel.mention if isinstance(voice_channel, discord.VoiceChannel) else VOICE_CREATE_CHANNEL_NAME,
        inline=True,
    )
    embed.add_field(
        name="1인 잠수방 생성 대기 채널",
        value=(
            sleep_voice_channel.mention
            if isinstance(sleep_voice_channel, discord.VoiceChannel)
            else VOICE_SLEEP_CREATE_CHANNEL_NAME
        ),
        inline=True,
    )
    embed.add_field(
        name="보안 로그 채널",
        value=log_channel.mention if isinstance(log_channel, discord.TextChannel) else SECURITY_LOG_CHANNEL_NAME,
        inline=True,
    )
    embed.add_field(
        name="실시간 태그 현황 채널",
        value=(
            tag_count_channel.mention
            if isinstance(tag_count_channel, discord.VoiceChannel)
            else "미지정"
        ),
        inline=True,
    )
    embed.add_field(
        name="금칙어",
        value=", ".join(filters)[:1024] if filters else "없음",
        inline=False,
    )
    embed.set_footer(text="설정 변경은 관리자 전용이며 이 서버에만 적용됩니다.")
    return embed


@bot.group(name="설정", invoke_without_command=True)
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def settings_group(ctx: commands.Context):
    await ctx.send(embed=setting_summary(ctx.guild))


@settings_group.command(name="관리자역할")
@commands.has_guild_permissions(administrator=True)
async def set_admin_role(ctx: commands.Context, role: discord.Role):
    set_setting(ctx.guild.id, "admin_role_id", str(role.id))
    await ctx.reply(f"티켓 운영진 역할을 {role.mention}로 설정했습니다.", mention_author=False)


@settings_group.command(name="인증역할")
@commands.has_guild_permissions(administrator=True)
async def set_verified_role(ctx: commands.Context, role: discord.Role):
    set_setting(ctx.guild.id, "verified_role_id", str(role.id))
    await ctx.reply(f"인증 완료 시 부여할 역할을 {role.mention}로 설정했습니다.", mention_author=False)


@settings_group.command(name="대기역할")
@commands.has_guild_permissions(administrator=True)
async def set_unverified_role(ctx: commands.Context, role: discord.Role):
    set_setting(ctx.guild.id, "unverified_role_id", str(role.id))
    await ctx.reply(f"신규 멤버 대기 역할을 {role.mention}로 설정했습니다.", mention_author=False)


@settings_group.command(name="보이스생성방")
@commands.has_guild_permissions(administrator=True)
async def set_voice_create_channel(ctx: commands.Context, channel: discord.VoiceChannel):
    sleep_channel_id = get_setting(ctx.guild.id, "voice_sleep_create_channel_id")
    if sleep_channel_id == str(channel.id):
        await ctx.reply(
            "2인 숙소와 1인 잠수방 생성 채널은 서로 다르게 설정해 주세요.",
            mention_author=False,
        )
        return
    set_setting(ctx.guild.id, "voice_create_channel_id", str(channel.id))
    await ctx.reply(
        f"{channel.mention} 입장 시 **{VOICE_ROOM_NAME}** 방을 생성하도록 설정했습니다. (최대 2명)",
        mention_author=False,
    )


@settings_group.command(name="잠수방생성방")
@commands.has_guild_permissions(administrator=True)
async def set_sleep_voice_create_channel(
    ctx: commands.Context, channel: discord.VoiceChannel
):
    room_channel_id = get_setting(ctx.guild.id, "voice_create_channel_id")
    if room_channel_id == str(channel.id):
        await ctx.reply(
            "2인 숙소와 1인 잠수방 생성 채널은 서로 다르게 설정해 주세요.",
            mention_author=False,
        )
        return
    set_setting(ctx.guild.id, "voice_sleep_create_channel_id", str(channel.id))
    await ctx.reply(
        f"{channel.mention} 입장 시 **{VOICE_SLEEP_ROOM_NAME}** 방을 생성하도록 설정했습니다. (최대 1명)",
        mention_author=False,
    )


@settings_group.command(name="태그현황채널")
@commands.has_guild_permissions(administrator=True)
async def set_tag_count_channel(
    ctx: commands.Context, channel: discord.VoiceChannel
):
    set_setting(ctx.guild.id, "tag_count_channel_id", str(channel.id))
    try:
        await refresh_tag_counter(ctx.guild)
    except discord.HTTPException:
        logger.exception("Could not initialize tag counter for guild %s", ctx.guild.id)
        await ctx.reply(
            "채널 설정은 저장했지만 인원 수를 갱신하지 못했습니다. "
            "봇의 서버원 조회 및 채널 관리 권한을 확인해 주세요.",
            mention_author=False,
        )
        return
    await ctx.reply(
        f"{channel.mention}을 실시간 태그 인원 표시 채널로 설정했습니다.",
        mention_author=False,
    )


@settings_group.command(name="로그채널")
@commands.has_guild_permissions(administrator=True)
async def set_security_log_channel(ctx: commands.Context, channel: discord.TextChannel):
    set_setting(ctx.guild.id, "security_log_channel_id", str(channel.id))
    await ctx.reply(f"보안 로그 채널을 {channel.mention}로 설정했습니다.", mention_author=False)


@settings_group.command(name="금칙어추가")
@commands.has_guild_permissions(administrator=True)
async def add_filter_word(ctx: commands.Context, *, word: str):
    word = word.strip().casefold()
    if not word or len(word) > 100:
        await ctx.reply("금칙어는 1~100자로 입력해 주세요.", mention_author=False)
        return
    words = list(get_filter_words(ctx.guild.id))
    if word in words:
        await ctx.reply("이미 등록된 금칙어입니다.", mention_author=False)
        return
    if len(words) >= 100:
        await ctx.reply("금칙어는 최대 100개까지 등록할 수 있습니다.", mention_author=False)
        return
    words.append(word)
    set_setting(ctx.guild.id, "filter_words", json.dumps(words, ensure_ascii=False))
    await ctx.reply(f"금칙어 `{word}`를 추가했습니다.", mention_author=False)


@settings_group.command(name="금칙어삭제")
@commands.has_guild_permissions(administrator=True)
async def remove_filter_word(ctx: commands.Context, *, word: str):
    word = word.strip().casefold()
    words = list(get_filter_words(ctx.guild.id))
    if word not in words:
        await ctx.reply("등록된 금칙어에서 찾지 못했습니다.", mention_author=False)
        return
    words.remove(word)
    set_setting(ctx.guild.id, "filter_words", json.dumps(words, ensure_ascii=False))
    await ctx.reply(f"금칙어 `{word}`를 삭제했습니다.", mention_author=False)


@bot.command(name="설정초기화")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def reset_guild_setting(ctx: commands.Context, setting: str):
    aliases = {
        "관리자역할": "admin_role_id",
        "인증역할": "verified_role_id",
        "대기역할": "unverified_role_id",
        "보이스생성방": "voice_create_channel_id",
        "잠수방생성방": "voice_sleep_create_channel_id",
        "로그채널": "security_log_channel_id",
        "태그현황채널": "tag_count_channel_id",
        "금칙어": "filter_words",
    }
    key = aliases.get(setting.strip())
    if key is None:
        await ctx.reply(
            "초기화할 항목: 관리자역할, 인증역할, 대기역할, 보이스생성방, 잠수방생성방, 로그채널, 태그현황채널, 금칙어",
            mention_author=False,
        )
        return
    reset_setting(ctx.guild.id, key)
    if key == "tag_count_channel_id":
        clear_tag_counter_cache(ctx.guild.id)
    await ctx.reply(f"`{setting}` 설정을 기본값(.env)으로 되돌렸습니다.", mention_author=False)


@bot.command(name="티켓패널생성")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def send_ticket_panel(ctx: commands.Context):
    embed = build_ticket_panel_embed()
    await ctx.send(embed=embed, view=TicketSetupView())


@bot.command(name="후원등록")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def register_sponsor(ctx: commands.Context, member: discord.Member, amount: int):
    if amount <= 0:
        await ctx.reply("금액은 1원 이상이어야 합니다.", mention_author=False)
        return
    total = add_sponsor_points(ctx.guild.id, member.id, amount)
    balance = get_coin_balance(ctx.guild.id, member.id)
    embed = discord.Embed(
        title="후원 등록 완료",
        description=(
            f"{member.mention}님의 후원 `{amount:,}원`과 코인 "
            f"`{amount:,} SNOW`를 반영했습니다."
        ),
        color=0x2ECC71,
    )
    embed.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
    embed.add_field(name="코인 잔액", value=f"{balance:,} SNOW", inline=True)
    await ctx.send(embed=embed)
    await update_live_dashboard(ctx.guild)
    receipt = discord.Embed(title="후원 감사 안내", color=0xFFD700)
    receipt.add_field(name="이번 후원", value=f"{amount:,}원", inline=True)
    receipt.add_field(name="누적 후원", value=f"{total:,}원", inline=True)
    try:
        await member.send(embed=receipt)
    except discord.Forbidden:
        logger.info("Could not DM sponsor receipt to user %s", member.id)


@bot.command(name="후원랭킹")
@commands.guild_only()
async def show_sponsor_ranking(ctx: commands.Context):
    rows = get_sponsor_ranking(ctx.guild.id)
    embed = discord.Embed(title="후원 명예의 전당 Top 5", color=0xFFD700)
    if not rows:
        embed.description = "아직 등록된 후원 내역이 없습니다."
    else:
        medals = ("🥇", "🥈", "🥉", "🏅", "🏅")
        lines = []
        for rank, (user_id, amount) in enumerate(rows, start=1):
            lines.append(f"{medals[rank - 1]} **{rank}위** <@{user_id}> — `{amount:,}원`")
        embed.description = "\n".join(lines)
    await ctx.send(embed=embed)


@bot.command(name="코인잔액")
@commands.guild_only()
async def show_coin_balance(ctx: commands.Context, member: discord.Member = None):
    member = member or ctx.author
    if not isinstance(member, discord.Member):
        await ctx.reply("서버 멤버만 조회할 수 있습니다.", mention_author=False)
        return
    balance = get_coin_balance(ctx.guild.id, member.id)
    await ctx.send(f"{member.mention}님의 보유 코인: **{balance:,} SNOW**")


@bot.command(name="코인삭제")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def deduct_coins(ctx: commands.Context, member: discord.Member, amount: int):
    if amount <= 0 or amount > 1_000_000_000:
        await ctx.reply("삭제 수량은 1 이상 1,000,000,000 이하로 입력해 주세요.", mention_author=False)
        return
    try:
        balance, sponsor_total = deduct_member_coins_and_sponsorship(
            ctx.guild.id, member.id, amount
        )
    except ValueError as error:
        if str(error) == "insufficient_coins":
            message = f"{member.mention}님의 코인이 부족합니다. 현재 잔액은 `{get_coin_balance(ctx.guild.id, member.id):,} SNOW`입니다."
        else:
            message = (
                f"{member.mention}님의 누적 후원액이 차감할 금액보다 적어 처리하지 않았습니다. "
                f"현재 후원액은 `{get_sponsor_total(ctx.guild.id, member.id):,}원`입니다."
            )
        await ctx.reply(message, mention_author=False)
        return

    await update_live_dashboard(ctx.guild)
    await ctx.reply(
        f"{member.mention}님의 코인 `{amount:,} SNOW`와 후원액 `{amount:,}원`을 차감했습니다.\n"
        f"남은 코인: `{balance:,} SNOW` · 남은 누적 후원액: `{sponsor_total:,}원`",
        mention_author=False,
    )


@bot.command(name="후원차감")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def deduct_sponsorship(ctx: commands.Context, member: discord.Member, amount: int):
    if amount <= 0 or amount > 1_000_000_000:
        await ctx.reply("차감 금액은 1 이상 1,000,000,000 이하로 입력해 주세요.", mention_author=False)
        return
    try:
        balance, sponsor_total = deduct_member_coins_and_sponsorship(
            ctx.guild.id, member.id, amount
        )
    except ValueError as error:
        if str(error) == "insufficient_coins":
            message = (
                f"{member.mention}님의 코인이 부족합니다. "
                f"현재 잔액은 `{get_coin_balance(ctx.guild.id, member.id):,} SNOW`입니다."
            )
        else:
            message = (
                f"{member.mention}님의 누적 후원액이 차감할 금액보다 적어 처리하지 않았습니다. "
                f"현재 후원액은 `{get_sponsor_total(ctx.guild.id, member.id):,}원`입니다."
            )
        await ctx.reply(message, mention_author=False)
        return

    await update_live_dashboard(ctx.guild)
    await ctx.reply(
        f"{member.mention}님의 후원액 `{amount:,}원`과 코인 `{amount:,} SNOW`를 차감했습니다.\n"
        f"남은 후원액: `{sponsor_total:,}원` · 남은 코인: `{balance:,} SNOW`",
        mention_author=False,
    )


@bot.command(name="설화전광판설치")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def setup_live_dashboard(ctx: commands.Context):
    embed = discord.Embed(
        title="🌌 설화 후원 실시간 라이브 전광판 🌌",
        description="후원 신청은 아래 버튼을 눌러 개인 티켓을 만들어 주세요.",
        color=0x4C00C7,
    )
    embed.add_field(name="💵 누적 총 후원금액", value="🏦 KRW 0원", inline=True)
    embed.add_field(name="🪙 유통 총 코인잔액", value="🪙 0 SNOW", inline=True)
    embed.add_field(
        name="📊 시즌 목표 달성도",
        value=make_premium_bar_graph(0, DEFAULT_SPONSOR_GOAL),
        inline=False,
    )
    message = await ctx.send(embed=embed, view=ServerMainDashboardView())
    save_dashboard(ctx.guild.id, ctx.channel.id, message.id, DEFAULT_SPONSOR_GOAL)
    await update_live_dashboard(ctx.guild)


@bot.command(name="설화목표설정")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def set_sponsor_goal(ctx: commands.Context, amount: int):
    if amount <= 0 or amount > 10_000_000_000:
        await ctx.reply(
            "목표 금액은 1원 이상 100억 원 이하로 설정해 주세요.",
            mention_author=False,
        )
        return
    if get_dashboard(ctx.guild.id) is None:
        await ctx.reply(
            "`!설화전광판설치` 명령어로 먼저 대시보드를 설치해 주세요.",
            mention_author=False,
        )
        return
    update_dashboard_goal(ctx.guild.id, amount)
    await update_live_dashboard(ctx.guild)
    await ctx.reply(f"시즌 후원 목표를 **{amount:,}원**으로 설정했습니다.", mention_author=False)


@bot.command(name="설화누적설정")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def set_guild_sponsor_total(
    ctx: commands.Context, member: discord.Member, amount: int
):
    if amount < 0 or amount > 10_000_000_000:
        await ctx.reply(
            "누적 금액은 0원 이상 100억 원 이하로 입력해 주세요.",
            mention_author=False,
        )
        return
    if get_dashboard(ctx.guild.id) is None:
        await ctx.reply(
            "`!설화전광판설치` 명령어로 먼저 대시보드를 설치해 주세요.",
            mention_author=False,
        )
        return

    previous_total = get_guild_sponsor_stats(ctx.guild.id)[0]
    current_member_total = get_sponsor_total(ctx.guild.id, member.id)
    if previous_total == 0 and amount > 0:
        await ctx.reply(
            "아직 후원 기록이 없습니다. 먼저 `!후원등록`으로 실제 후원을 등록한 뒤 누적 금액을 조정해 주세요.",
            mention_author=False,
        )
        return
    try:
        _, new_total, member_total = set_guild_sponsor_total_by_adjusting_member(
            ctx.guild.id, member.id, amount
        )
    except ValueError as error:
        if str(error) == "no_sponsor_records":
            await ctx.reply(
                "아직 후원 기록이 없습니다. 먼저 `!후원등록`으로 실제 후원을 등록한 뒤 누적 금액을 조정해 주세요.",
                mention_author=False,
            )
            return
        await ctx.reply(
            f"총액을 `{amount:,}원`으로 낮추려면 {member.mention}님의 후원 기록에서 "
            f"`{previous_total - amount:,}원`을 차감해야 하지만, 현재 기록은 "
            f"`{current_member_total:,}원`입니다. 더 큰 후원 기록을 가진 유저를 선택해 주세요.",
            mention_author=False,
        )
        return

    await update_live_dashboard(ctx.guild)
    await ctx.reply(
        f"누적 후원액을 `{previous_total:,}원`에서 `{new_total:,}원`으로 설정했습니다.\n"
        f"{member.mention}님의 후원 기록을 `{member_total:,}원`으로 조정했습니다. "
        "코인 잔액도 후원 기록에 맞춰 조정했습니다.",
        mention_author=False,
    )


@bot.command(name="후원금액수정")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def edit_member_sponsor_amount(
    ctx: commands.Context, member: discord.Member, amount: int
):
    if amount < 0 or amount > 10_000_000_000:
        await ctx.reply(
            "후원 금액은 0원 이상 100억 원 이하로 입력해 주세요.",
            mention_author=False,
        )
        return

    previous_amount, new_guild_total = set_member_sponsor_total(
        ctx.guild.id, member.id, amount
    )
    await update_live_dashboard(ctx.guild)
    await ctx.reply(
        f"{member.mention}님의 누적 후원액을 `{previous_amount:,}원`에서 "
        f"`{amount:,}원`으로 수정했습니다. 대시보드 누적액은 "
        f"`{new_guild_total:,}원`입니다. 코인 잔액도 후원 기록에 맞춰 조정했습니다.",
        mention_author=False,
    )


@bot.command(name="인증패널배포")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def deploy_captcha_panel(ctx: commands.Context):
    embed = discord.Embed(
        title="서버 가입 인증",
        description="아래 버튼을 눌러 인증을 완료해 주세요.",
        color=0x2ECC71,
    )
    await ctx.send(embed=embed, view=CaptchaVerifyView())


async def expire_unverified_member(member: discord.Member):
    await asyncio.sleep(600)
    guild = member.guild
    current_member = guild.get_member(member.id)
    if current_member is None:
        return
    verified_role = get_role(guild, VERIFIED_ROLE_NAME)
    unverified_role = get_role(guild, UNVERIFIED_ROLE_NAME)
    if verified_role and verified_role in current_member.roles:
        return
    if unverified_role is None or unverified_role not in current_member.roles:
        return
    try:
        await current_member.kick(reason="10 minutes passed without verification")
    except discord.Forbidden:
        logger.error("Missing permission to remove unverified member %s", member.id)


@bot.event
async def on_member_join(member: discord.Member):
    unverified_role = get_role(member.guild, UNVERIFIED_ROLE_NAME)
    if unverified_role:
        try:
            await member.add_roles(unverified_role, reason="New member verification")
        except discord.Forbidden:
            logger.error("Could not assign unverified role to member %s", member.id)
    try:
        await member.send(
            f"{member.guild.name} 서버에 오신 것을 환영합니다. "
            "서버의 인증 채널에서 인증 버튼을 눌러 주세요. 인증하지 않으면 10분 후 서버에서 퇴장됩니다."
        )
    except discord.Forbidden:
        logger.info("Could not DM welcome message to member %s", member.id)
    if unverified_role:
        asyncio.create_task(expire_unverified_member(member))
    await update_tag_counter_safely(member)


async def update_tag_counter_safely(
    member: discord.Member, *, removed: bool = False
):
    try:
        await update_tag_counter_for_member(member, removed=removed)
    except discord.HTTPException:
        logger.exception(
            "Could not update tag counter after member change in guild %s",
            member.guild.id,
        )


@bot.event
async def on_member_remove(member: discord.Member):
    await update_tag_counter_safely(member, removed=True)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    await update_tag_counter_safely(after)


@bot.command(name="유저정보")
@commands.guild_only()
async def user_info(ctx: commands.Context, member: discord.Member = None):
    member = member or ctx.author
    if not isinstance(member, discord.Member):
        await ctx.reply("서버 멤버만 조회할 수 있습니다.", mention_author=False)
        return
    embed = discord.Embed(
        title=f"{member.display_name} 유저 정보",
        color=member.color,
    )
    if member.display_avatar:
        embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="계정 ID", value=str(member.id), inline=True)
    embed.add_field(
        name="서버 가입일",
        value=discord.utils.format_dt(member.joined_at, "D") if member.joined_at else "확인 불가",
        inline=True,
    )
    embed.add_field(
        name="누적 경고",
        value=f"{get_warning_count(ctx.guild.id, member.id)}회",
        inline=True,
    )
    embed.add_field(
        name="누적 후원",
        value=f"{get_sponsor_total(ctx.guild.id, member.id):,}원",
        inline=True,
    )
    role_mentions = [role.mention for role in member.roles if role != ctx.guild.default_role]
    embed.add_field(name="역할", value=", ".join(role_mentions) or "없음", inline=False)
    await ctx.send(embed=embed)


@bot.command(name="공지생성")
@commands.guild_only()
@commands.has_guild_permissions(administrator=True)
async def create_announcement(ctx: commands.Context, *, text: str):
    if "|" not in text:
        await ctx.reply(
            "사용법: `!공지생성 제목 | 내용`",
            mention_author=False,
        )
        return
    title, description = (part.strip() for part in text.split("|", 1))
    if not title or not description:
        await ctx.reply("제목과 내용을 모두 입력해 주세요.", mention_author=False)
        return
    embed = discord.Embed(title=title[:256], description=description[:4000], color=0x2ECC71)
    embed.set_footer(text=f"작성자: {ctx.author.display_name}")
    await ctx.send(embed=embed)


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
):
    if before.channel == after.channel:
        return

    room_config = None
    if after.channel:
        creation_channels = (
            (
                "voice_create_channel_id",
                VOICE_CREATE_CHANNEL_NAME,
                VOICE_ROOM_NAME,
                VOICE_ROOM_USER_LIMIT,
            ),
            (
                "voice_sleep_create_channel_id",
                VOICE_SLEEP_CREATE_CHANNEL_NAME,
                VOICE_SLEEP_ROOM_NAME,
                VOICE_SLEEP_ROOM_USER_LIMIT,
            ),
        )
        for setting_key, fallback_name, room_name, user_limit in creation_channels:
            configured_channel_id = get_setting(member.guild.id, setting_key)
            if configured_channel_id:
                matches_channel = after.channel.id == int(configured_channel_id)
            else:
                matches_channel = after.channel.name == fallback_name
            if matches_channel:
                room_config = (room_name, user_limit)
                break

    if after.channel and room_config:
        room_name, user_limit = room_config
        room = await member.guild.create_voice_channel(
            name=room_name,
            category=after.channel.category,
            user_limit=user_limit,
        )
        set_temp_voice_room(room.id, member.id)
        try:
            await member.move_to(room)
        except discord.HTTPException:
            await room.delete(reason="Could not move room owner")
            remove_temp_voice_room(room.id)
            raise

    if before.channel and before.channel.id in get_temp_voice_room_ids():
        if not before.channel.members:
            await before.channel.delete(reason="Temporary voice room is empty")
            remove_temp_voice_room(before.channel.id)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None:
        return

    content = message.content.casefold()
    repeat_count = track_repeated_message(
        message.guild.id,
        message.author.id,
        message.content,
        message.created_at,
    )
    if repeat_count >= REPEAT_MESSAGE_LIMIT:
        timeout_until = discord.utils.utcnow() + REPEAT_MESSAGE_TIMEOUT
        try:
            await message.author.timeout(
                timeout_until,
                reason=(
                    f"Sent the same message {REPEAT_MESSAGE_LIMIT} times "
                    f"within {REPEAT_MESSAGE_INTERVAL} seconds per message"
                ),
            )
        except discord.Forbidden:
            logger.exception(
                "Could not timeout repeated-message spammer %s in guild %s",
                message.author.id,
                message.guild.id,
            )
            try:
                await message.channel.send(
                    f"{message.author.mention}님의 연속 도배가 감지됐지만 "
                    "봇 권한 또는 역할 순서 문제로 타임아웃을 적용하지 못했습니다."
                )
            except discord.HTTPException:
                logger.exception(
                    "Could not report failed spam timeout in guild %s",
                    message.guild.id,
                )
            return
        except discord.HTTPException:
            logger.exception(
                "Discord rejected repeated-message timeout for user %s in guild %s",
                message.author.id,
                message.guild.id,
            )
            return

        logger.warning(
            "Timed out repeated-message spammer user=%s guild=%s channel=%s count=%s duration_hours=24",
            message.author.id,
            message.guild.id,
            message.channel.id,
            repeat_count,
        )
        try:
            await message.channel.send(
                f"{message.author.mention}님이 같은 메시지를 {REPEAT_MESSAGE_LIMIT}회 "
                f"연속 전송해 1일 타임아웃되었습니다."
            )
        except discord.HTTPException:
            logger.exception(
                "Could not announce repeated-message timeout in guild %s",
                message.guild.id,
            )
        return

    if any(phrase in content for phrase in DM_INVITE_PHRASES):
        try:
            await message.delete()
        except discord.Forbidden:
            logger.error("Missing permission to remove prohibited message %s", message.id)
            return
        alert = await message.channel.send(
            f"{message.author.mention}님, 서버에서 개인 DM 유도는 허용되지 않습니다."
        )
        await alert.delete(delay=4)
        return

    if any(word in content for word in get_filter_words(message.guild.id)):
        try:
            await message.delete()
        except discord.Forbidden:
            logger.error("Missing permission to remove filtered message %s", message.id)
            return
        count = add_warning(message.guild.id, message.author.id)
        embed = discord.Embed(
            title="제한된 표현이 감지되었습니다",
            description=f"{message.author.mention} 누적 경고: **{count}회**",
            color=0xE74C3C,
        )
        await message.channel.send(embed=embed, delete_after=7)
        try:
            await message.author.send(
                f"{message.guild.name} 서버에서 제한된 표현이 감지되었습니다. "
                f"현재 경고는 {count}회입니다."
            )
        except discord.Forbidden:
            logger.info("Could not DM warning to user %s", message.author.id)

        try:
            if count >= 5:
                await message.guild.ban(
                    message.author, reason="Reached 5 moderation warnings"
                )
            elif count >= 3:
                await message.author.timeout(
                    discord.utils.utcnow() + timedelta(hours=24),
                    reason="Reached 3 moderation warnings",
                )
        except discord.Forbidden:
            logger.error("Missing moderation permission for user %s", message.author.id)
        return

    if (
        message.mention_everyone
        and not message.author.guild_permissions.mention_everyone
    ):
        await send_log(
            message.guild,
            f"무단 전체 멘션 감지: {message.author.mention} — {message.channel.mention}",
        )
    await bot.process_commands(message)


@bot.event
async def on_message_delete(message: discord.Message):
    if (
        message.guild is None
        or message.author.bot
        or not message.mentions
        or not message.content
    ):
        return
    embed = discord.Embed(title="삭제된 멘션 기록", color=0xE67E22)
    embed.add_field(name="작성자", value=message.author.mention, inline=True)
    embed.add_field(
        name="멘션 대상",
        value=", ".join(user.mention for user in message.mentions)[:1024],
        inline=True,
    )
    embed.add_field(name="삭제된 내용", value=message.content[:1024], inline=False)
    await send_log(message.guild, embed=embed)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("이 명령어를 사용할 권한이 없습니다.", mention_author=False)
        return
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.reply("이 명령어는 서버 안에서만 사용할 수 있습니다.", mention_author=False)
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("필수 인수가 빠졌습니다. `!도움말`에서 사용법을 확인해 주세요.", mention_author=False)
        return
    if isinstance(error, commands.BadArgument):
        await ctx.reply("입력 형식이 올바르지 않습니다. 멘션과 금액을 확인해 주세요.", mention_author=False)
        return
    logger.exception("Command failed", exc_info=error)
    await ctx.reply("명령어 처리 중 오류가 발생했습니다. 운영자에게 문의해 주세요.", mention_author=False)


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN 환경변수가 비어 있습니다. .env 파일을 설정해 주세요.")
    bot.run(TOKEN)
