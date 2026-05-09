"""
Hyperliquid BTC Perp Watcher v2 (アグリゲート/合意シグナル/急変アラート版)

トップトレーダーのBTCポジション動向をDiscordに通知する。
GitHub Actions cronから5分間隔で実行されるが、通知は次の3種類のみ：

  1. 4時間アグリゲート (UTC 00/04/08/12/16/20 のバケットに最初に入った実行)
     → ロング/ショート集計、ロング優勢率
  2. 合意シグナル (1回の実行で同方向の新規エントリーが N 人以上)
     → 短期センチメント急変の早期検知
  3. 急変アラート (ロング優勢率が前回比 X% ポイント以上動いた)
     → 集計値の急変検知

state.json に前回スナップショット・前回ロング優勢率・最後にアグリゲートを送った
4時間バケットを保存し、ワークフローがコミットバックする。

環境変数:
    DISCORD_WEBHOOK_URL : Discordチャンネルで発行したWebhook URL
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ─────────────────────────────────────────
# 設定
# ─────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TOP_N = 20                  # Leaderboard上位何人を監視するか
MIN_BTC_SIZE = 0.1          # 無視するポジションサイズの下限（BTC）
STATE_FILE = "state.json"

# 通知ロジック
AGGREGATE_HOURS_UTC = (0, 4, 8, 12, 16, 20)   # 4時間アグリゲート対象（UTC）
CONSENSUS_THRESHOLD = 3                        # この人数以上が同方向に新規→合意シグナル
RATIO_CHANGE_THRESHOLD_PCT = 10.0              # ロング優勢率がこのpt以上動いたら急変アラート

# Hyperliquid API
HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

# Discord
DISCORD_MAX_LEN = 1900


# ─────────────────────────────────────────
# Discord通知
# ─────────────────────────────────────────
def send_discord(message: str) -> None:
    """Discord Webhookにメッセージを送信する。"""
    if not DISCORD_WEBHOOK_URL:
        print(f"[DISCORD（未送信・WEBHOOK未設定）] {message}")
        return

    chunks = [message[i:i + DISCORD_MAX_LEN] for i in range(0, len(message), DISCORD_MAX_LEN)] or [message]
    for chunk in chunks:
        try:
            res = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": chunk},
                timeout=10,
            )
            if res.status_code == 429:
                retry_after = float(res.json().get("retry_after", 1))
                time.sleep(retry_after + 0.5)
                requests.post(
                    DISCORD_WEBHOOK_URL,
                    json={"content": chunk},
                    timeout=10,
                )
            elif res.status_code >= 400:
                print(f"Discord送信エラー: status={res.status_code} body={res.text[:200]}")
        except Exception as e:
            print(f"Discord送信エラー: {e}")


# ─────────────────────────────────────────
# Hyperliquid API
# ─────────────────────────────────────────
def fetch_leaderboard() -> list:
    """フロントエンド統計エンドポイントからリーダーボードを取得。"""
    try:
        res = requests.get(HYPERLIQUID_LEADERBOARD_URL, timeout=20)
        if res.status_code != 200:
            print(f"Leaderboard HTTP {res.status_code}: {res.text[:300]}")
            return []
        data = res.json()
        entries = data.get("leaderboardRows", [])
        if not entries:
            print(f"leaderboardRowsが空。レスポンスキー: {list(data.keys())[:10] if isinstance(data, dict) else type(data).__name__}")
        return entries[:TOP_N]
    except Exception as e:
        print(f"Leaderboard取得エラー: {e}")
        return []


def fetch_positions(address: str) -> list:
    """指定アドレスのBTC perpポジションを取得。"""
    try:
        res = requests.post(
            HYPERLIQUID_API,
            json={"type": "clearinghouseState", "user": address},
            timeout=15,
        )
        data = res.json()
        positions = data.get("assetPositions", [])
        btc_positions = []
        for p in positions:
            pos = p.get("position", {})
            coin = pos.get("coin", "")
            size = float(pos.get("szi", 0))
            if coin == "BTC" and abs(size) >= MIN_BTC_SIZE:
                btc_positions.append({
                    "coin": coin,
                    "size": size,
                    "entry_price": float(pos.get("entryPx", 0)),
                    "unrealized_pnl": float(pos.get("unrealizedPnl", 0)),
                    "leverage": pos.get("leverage", {}).get("value", "?"),
                })
        return btc_positions
    except Exception as e:
        print(f"ポジション取得エラー ({address[:8]}...): {e}")
        return []


# ─────────────────────────────────────────
# 状態の永続化
# ─────────────────────────────────────────
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"state.json読み込み失敗 (空からやり直します): {e}")
        return {}


def save_state(state: dict) -> None:
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────
# 集計・差分検出
# ─────────────────────────────────────────
def compute_aggregate(positions_dict: dict, leaders_count: int) -> dict:
    """ポジション辞書から集計指標を計算する。"""
    flat_positions = [
        p for positions in positions_dict.values() for p in positions
    ]
    long_positions = [p for p in flat_positions if p["size"] > 0]
    short_positions = [p for p in flat_positions if p["size"] < 0]

    total_long = sum(p["size"] for p in long_positions)
    total_short = sum(abs(p["size"]) for p in short_positions)
    btc_holders = sum(1 for positions in positions_dict.values() if positions)

    if total_long + total_short > 0:
        long_ratio = total_long / (total_long + total_short) * 100
    else:
        long_ratio = None

    return {
        "leaders_count": leaders_count,
        "btc_holders": btc_holders,
        "total_long": total_long,
        "total_short": total_short,
        "long_count": len(long_positions),
        "short_count": len(short_positions),
        "long_ratio": long_ratio,
    }


def format_aggregate(agg: dict, label: str) -> str:
    """アグリゲート指標を整形する。"""
    if agg["long_ratio"] is None:
        return f"\n{label}\nBTCポジションなし"

    ratio = agg["long_ratio"]
    bias_emoji = "🟢" if ratio > 60 else ("🔴" if ratio < 40 else "⚖️")

    return (
        f"\n{label}\n"
        f"監視: {agg['leaders_count']}人 / BTC保有: {agg['btc_holders']}人\n"
        f"ロング: {agg['long_count']}人 ({agg['total_long']:.2f} BTC)\n"
        f"ショート: {agg['short_count']}人 ({agg['total_short']:.2f} BTC)\n"
        f"ロング優勢率: {bias_emoji} {ratio:.0f}%"
    )


def get_4h_bucket(dt: datetime) -> str:
    """UTC 4時間バケットキー。例: '2026-05-09T08'。"""
    bucket_hour = (dt.hour // 4) * 4
    return f"{dt.strftime('%Y-%m-%d')}T{bucket_hour:02d}"


def detect_new_entries(prev: dict, curr: dict) -> tuple:
    """前回→今回でBTCに新規エントリーした人数を (long, short) で返す。

    既にBTCを持っていた人がポジション継続している場合はカウントしない。
    既存BTCをクローズして反対方向に新規した場合もカウントする。
    """
    new_long = 0
    new_short = 0
    for addr, curr_list in curr.items():
        prev_btc = next(
            (p for p in prev.get(addr, []) if p["coin"] == "BTC"), None
        )
        curr_btc = next(
            (p for p in curr_list if p["coin"] == "BTC"), None
        )

        if curr_btc is None:
            continue

        if prev_btc is None:
            # 完全新規
            if curr_btc["size"] > 0:
                new_long += 1
            else:
                new_short += 1
        else:
            # 方向反転（ロング→ショート or ショート→ロング）も新規扱い
            prev_dir = 1 if prev_btc["size"] > 0 else -1
            curr_dir = 1 if curr_btc["size"] > 0 else -1
            if prev_dir != curr_dir:
                if curr_dir > 0:
                    new_long += 1
                else:
                    new_short += 1
    return new_long, new_short


# ─────────────────────────────────────────
# メイン（1回実行）
# ─────────────────────────────────────────
def main() -> int:
    now_utc = datetime.now(timezone.utc)
    print(f"🚀 [{now_utc.isoformat()}] BTC Watcher v2 起動")

    # 状態読み込み
    state = load_state()
    prev_positions = state.get("positions", {})
    prev_long_ratio = state.get("long_ratio")
    last_aggregate_bucket = state.get("last_aggregate_bucket")
    is_first_run = not prev_positions

    # Leaderboard取得
    leaders = fetch_leaderboard()
    if not leaders:
        print("Leaderboard取得失敗、終了します")
        return 1

    # トレーダー名（順位）を更新
    new_trader_names = {}
    for i, entry in enumerate(leaders):
        addr = entry.get("ethAddress", "")
        if addr:
            new_trader_names[addr] = f"#{i+1}位"

    # ポジション取得
    curr_positions = {}
    for entry in leaders:
        addr = entry.get("ethAddress", "")
        if addr:
            curr_positions[addr] = fetch_positions(addr)
            time.sleep(0.2)

    # 集計
    agg = compute_aggregate(curr_positions, len(leaders))
    current_long_ratio = agg["long_ratio"]
    current_bucket = get_4h_bucket(now_utc)

    notifications = []
    aggregate_sent = False

    # ─── 1. アグリゲート通知 ───
    if is_first_run:
        notifications.append(
            format_aggregate(agg, "🚀 BTC Watcher 起動 - 初回スキャン")
        )
        aggregate_sent = True
    elif current_bucket != last_aggregate_bucket and now_utc.hour in AGGREGATE_HOURS_UTC:
        # 4時間バケットの境界を跨いだ最初の実行
        label = f"📊 4時間アグリゲート ({now_utc.strftime('%Y-%m-%d %H:%M UTC')})"
        notifications.append(format_aggregate(agg, label))
        aggregate_sent = True

    # ─── 2. 合意シグナル（初回はスキップ） ───
    if not is_first_run:
        new_long, new_short = detect_new_entries(prev_positions, curr_positions)
        if new_long >= CONSENSUS_THRESHOLD:
            notifications.append(
                f"\n🟢 合意シグナル: ロング\n"
                f"{new_long}人のトップトレーダーがBTCロングに新規エントリー\n"
                f"（直近5分以内）"
            )
        if new_short >= CONSENSUS_THRESHOLD:
            notifications.append(
                f"\n🔴 合意シグナル: ショート\n"
                f"{new_short}人のトップトレーダーがBTCショートに新規エントリー\n"
                f"（直近5分以内）"
            )

    # ─── 3. ロング優勢率の急変アラート（初回はスキップ） ───
    if (
        not is_first_run
        and prev_long_ratio is not None
        and current_long_ratio is not None
        and abs(current_long_ratio - prev_long_ratio) >= RATIO_CHANGE_THRESHOLD_PCT
    ):
        diff = current_long_ratio - prev_long_ratio
        arrow = "📈" if diff > 0 else "📉"
        notifications.append(
            f"\n⚡ ロング優勢率の急変\n"
            f"{prev_long_ratio:.0f}% → {current_long_ratio:.0f}% "
            f"({arrow} {diff:+.0f}pt)\n"
            f"BTC保有: {agg['btc_holders']}人"
        )

    # 通知送信
    for msg in notifications:
        print(msg)
        send_discord(msg)

    if not notifications:
        print(f"[{now_utc.strftime('%H:%M:%S')}] 通知なし（変化なし・アグリゲートタイミング外）")

    # 状態保存
    new_state = {
        "positions": curr_positions,
        "trader_names": new_trader_names,
        "long_ratio": current_long_ratio,
        "last_aggregate_bucket": current_bucket if aggregate_sent else last_aggregate_bucket,
    }
    save_state(new_state)
    print(f"✅ state.json更新（aggregate_sent={aggregate_sent}, notifications={len(notifications)}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
