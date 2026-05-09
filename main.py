"""
Hyperliquid BTC Perp Watcher (GitHub Actions cron 版)

トップトレーダーのBTCポジション変化をDiscordに通知する。
GitHub Actionsから5〜10分間隔で1回実行される設計。
前回スナップショットは state.json に保存し、ワークフローが
コミットバックすることで次回実行時に引き継ぐ。

環境変数:
    DISCORD_WEBHOOK_URL : Discordチャンネルで発行したWebhook URL
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ────────────────────────────────────────
# 設定
# ────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TOP_N = 20                  # Leaderboard上位何人を監視するか
MIN_BTC_SIZE = 0.1          # 無視するポジションサイズの下限（BTC）
STATE_FILE = "state.json"

HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

# Discordのメッセージ最大長は2000文字
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
    try:
        res = requests.post(
            HYPERLIQUID_API,
            json={"type": "leaderboard"},
            timeout=15,
        )
        data = res.json()
        entries = data.get("leaderboardRows", [])
        return entries[:TOP_N]
    except Exception as e:
        print(f"Leaderboard取得エラー: {e}")
        return []


def fetch_positions(address: str) -> list:
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
    """state.jsonを読み込む。存在しなければ空のスケルトンを返す。"""
    if not os.path.exists(STATE_FILE):
        return {"positions": {}, "trader_names": {}, "last_run": None}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"state.json読み込み失敗 (空からやり直します): {e}")
        return {"positions": {}, "trader_names": {}, "last_run": None}


def save_state(state: dict) -> None:
    """state.jsonに書き込む。"""
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────
# 変化検出ロジック
# ─────────────────────────────────────────
def detect_changes(prev: dict, curr: dict, trader_names: dict) -> list:
    messages = []
    all_addresses = set(prev.keys()) | set(curr.keys())
    now_str = datetime.now().strftime("%H:%M")

    for addr in all_addresses:
        name = trader_names.get(addr, addr[:8] + "...")
        prev_pos = {p["coin"]: p for p in prev.get(addr, [])}
        curr_pos = {p["coin"]: p for p in curr.get(addr, [])}

        for coin in set(prev_pos.keys()) | set(curr_pos.keys()):
            p = prev_pos.get(coin)
            c = curr_pos.get(coin)

            if p is None and c is not None:
                direction = "🟢 LONG" if c["size"] > 0 else "🔴 SHORT"
                msg = (
                    f"\n🚨 新規エントリー検出\n"
                    f"トレーダー: {name}\n"
                    f"銘柄: {coin}\n"
                    f"方向: {direction}\n"
                    f"サイズ: {abs(c['size']):.4f} BTC\n"
                    f"エントリー価格: ${c['entry_price']:,.0f}\n"
                    f"レバレッジ: {c['leverage']}x\n"
                    f"時刻: {now_str}"
                )
                messages.append(msg)

            elif p is not None and c is None:
                direction = "LONG" if p["size"] > 0 else "SHORT"
                pnl = p["unrealized_pnl"]
                pnl_emoji = "✅" if pnl >= 0 else "❌"
                msg = (
                    f"\n💨 ポジションクローズ\n"
                    f"トレーダー: {name}\n"
                    f"銘柄: {coin} ({direction})\n"
                    f"サイズ: {abs(p['size']):.4f} BTC\n"
                    f"含み損益: {pnl_emoji} ${pnl:+,.0f}\n"
                    f"時刻: {now_str}"
                )
                messages.append(msg)

            elif p is not None and c is not None:
                size_diff = c["size"] - p["size"]
                if abs(p["size"]) > 0 and abs(size_diff / p["size"]) > 0.1:
                    action = "📈 増玉" if abs(c["size"]) > abs(p["size"]) else "📉 減玉"
                    msg = (
                        f"\n{action} 検出\n"
                        f"トレーダー: {name}\n"
                        f"銘柄: {coin}\n"
                        f"変化: {p['size']:+.4f} → {c['size']:+.4f} BTC\n"
                        f"現在エントリー価格: ${c['entry_price']:,.0f}\n"
                        f"時刻: {now_str}"
                    )
                    messages.append(msg)

    return messages


# ─────────────────────────────────────────
# メイン（1回実行）
# ─────────────────────────────────────────
def main() -> int:
    print(f"🚀 [{datetime.now(timezone.utc).isoformat()}] BTC Watcher 起動")

    # 状態読み込み
    state = load_state()
    prev_positions = state.get("positions", {})
    trader_names = state.get("trader_names", {})
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

    # 各トレーダーのBTCポジション取得
    curr_positions = {}
    for entry in leaders:
        addr = entry.get("ethAddress", "")
        if addr:
            curr_positions[addr] = fetch_positions(addr)
            time.sleep(0.2)

    # 通知
    if is_first_run:
        # 初回スキャン: サマリーを送信
        send_discord("\n🚀 Hyperliquid BTC Watcher 起動しました\nトップ20トレーダーのBTCポジションを監視中...")

        total_long = sum(
            p["size"] for positions in curr_positions.values()
            for p in positions if p["size"] > 0
        )
        total_short = sum(
            abs(p["size"]) for positions in curr_positions.values()
            for p in positions if p["size"] < 0
        )
        btc_holders = sum(1 for positions in curr_positions.values() if positions)

        if (total_long + total_short) > 0:
            ratio = total_long / (total_long + total_short) * 100
            summary = (
                f"\n📊 初回スキャン完了\n"
                f"監視トレーダー数: {len(leaders)}人\n"
                f"BTC保有者: {btc_holders}人\n"
                f"合計ロング: {total_long:.2f} BTC\n"
                f"合計ショート: {total_short:.2f} BTC\n"
                f"ロング優勢率: {ratio:.0f}%"
            )
        else:
            summary = "\n📊 初回スキャン完了\n合計ポジションなし"
        print(summary)
        send_discord(summary)
    else:
        # 通常実行: 差分のみ送信
        changes = detect_changes(prev_positions, curr_positions, new_trader_names)
        for msg in changes:
            print(msg)
            send_discord(msg)
        if not changes:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 変化なし")

    # 状態保存
    save_state({
        "positions": curr_positions,
        "trader_names": new_trader_names,
    })
    print("✅ state.jsonを更新しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
