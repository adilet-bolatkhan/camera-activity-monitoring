"""
Мониторинг активности камер.

Источник данных — те же эндпоинты, что в CAMERA_METRICS_BY_DOMAIN вашего
FastAPI-сервиса (http://<host>:8000/), отдающие СЫРОЙ Prometheus exposition
format (см. parse_cameras_metrics в вашем сервисе). Это НЕ JSON API
VictoriaMetrics (/api/v1/query) — просто текущий снимок метрик без истории.

Поэтому:
    1. Каждый запуск скрипта забирает текущий снимок (total/active камер)
       и парсит его так же, как ваш сервис (prometheus_client.parser).
    2. Снимок сохраняется в локальную SQLite (camera_history.sqlite3) —
       так со временем накапливается история для расчёта нормы.
    3. "Норма" = среднее количество активных камер за последние NORM_DAYS
       дней, взятое в районе того же времени суток (окно ±TIME_TOLERANCE_MIN
       минут), — то есть скользящее среднее с поправкой на дневной цикл.
    4. Если текущее значение активных камер упало относительно нормы на
       DROP_THRESHOLD (10%) и более — шлём алерт в Google Chat.

ВАЖНО: скрипт должен запускаться регулярно и часто (например, раз в
10-15 минут через cron), чтобы:
  а) вовремя ловить падения активности;
  б) накапливать историю в SQLite, из которой считается норма.
Первые NORM_DAYS дней после первого запуска норма будет "н/д" —
это ожидаемо, истории ещё не накопилось.

Если у вас ЕСТЬ отдельная настоящая VictoriaMetrics с длинным ретеншеном,
которая скрейпит эти же эндпоинты, — расчёт нормы лучше делать через её
query_range API (это надёжнее и не зависит от локального файла на сервере).
Скажите мне адрес и путь такого API, и я перепишу vm_active_norm под него.
"""

import os
import sys
import time
import sqlite3
import logging
import argparse
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor

import requests
from prometheus_client.parser import text_string_to_metric_families

# --- Логирование: в консоль и в файл ---
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("camera_monitor")
logger.setLevel(logging.INFO)
logger.propagate = False

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_file_handler = RotatingFileHandler(
    LOG_DIR / "camera_monitor.log", maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_file_handler.setFormatter(_fmt)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)
logger.addHandler(_console_handler)

# ============================================================
#                        КОНФИГУРАЦИЯ
# ============================================================

DROP_THRESHOLD = 0.2        # порог падения активности, при котором шлём алерт (10%)
TIME_TOLERANCE_MIN = 30       # окно совпадения "того же времени суток" (используется только для тренда в --test-send)
REQUEST_TIMEOUT = 15          # таймаут запроса к metrics-эндпоинту, сек

CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAeQLcWDk/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=EJ-H1oYJPsAB6euB3-bdT5v4XbsosCxPOvF9zYnXQW4"

# Норма задаётся ВРУЧНУЮ — доля активных камер от общего числа камер
# (0.0-1.0) по окружению. Итоговая норма (в штуках) = доля * текущий total.
# Доля, а не абсолютное число — чтобы норма сама подстраивалась при
# росте/сокращении парка камер.
# Если для окружения норма не задана — алерты по нему не проверяются
# (нет базы для сравнения), в отчёте будет "н/д".
#   "Атырау": 0.74  — ожидаем стабильно ~74% камер активными (45к из 61к)
ENVIRONMENT_NORMS = {
    "ЕСВМ Алматы": 0.74,
    "ЕСВМ Атырау": 0.81,
    "КСБ Астана": 0.98,
    "ЕСВМ Мангыстау": 0.72,
    "ЕСВМ Шымкент": 0.78
}

# Те же адреса, что в CAMERA_METRICS_BY_DOMAIN вашего сервиса
ENVIRONMENTS = [
    {"name": "ЕСВМ Алматы",   "metrics_url": "http://100.100.101.12:8000/"},
    {"name": "ЕСВМ Атырау",    "metrics_url": "http://100.100.122.17:8000/"},
    {"name": "КСБ Астана",    "metrics_url": "http://100.100.170.11:8000/"},
    {"name": "ЕСВМ Мангыстау", "metrics_url": "http://100.100.175.14:8000/"},
    {"name": "ЕСВМ Шымкент",   "metrics_url": "http://100.100.181.18:8000/"},
]

DB_PATH = Path(__file__).parent / "camera_history.sqlite3"

# Слать recovery-уведомление ("✅ восстановилось"), когда сущность
# переходит из ALERT обратно в OK. Если не нужно - поставьте False.
ENABLE_RECOVERY_NOTIFICATIONS = True


# ============================================================
#                 ХРАНИЛИЩЕ ИСТОРИИ (SQLite)
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            environment TEXT NOT NULL,
            ts INTEGER NOT NULL,      -- unix timestamp запроса
            total INTEGER NOT NULL,
            active INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_env_ts ON snapshots(environment, ts)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_state (
            entity TEXT PRIMARY KEY,
            status TEXT NOT NULL,     -- 'ok' | 'alert'
            updated_ts INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def save_snapshot(environment: str, ts: float, total: int, active: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO snapshots (environment, ts, total, active) VALUES (?, ?, ?, ?)",
        (environment, int(ts), total, active),
    )
    conn.commit()
    conn.close()


def get_last_alert_status(entity: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("SELECT status FROM alert_state WHERE entity = ?", (entity,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_alert_status(entity: str, status: str, ts: float):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO alert_state (entity, status, updated_ts) VALUES (?, ?, ?)
        ON CONFLICT(entity) DO UPDATE SET status=excluded.status, updated_ts=excluded.updated_ts
        """,
        (entity, status, int(ts)),
    )
    conn.commit()
    conn.close()


def process_alert_states(results: list, now_ts: float):
    """
    Сравнивает текущий статус (alert/ok) каждого окружения с последним
    сохранённым и обновляет состояние. Возвращает (new_alerts, recovered):
      - new_alerts  — сущности, у которых ТОЛЬКО ЧТО начался алерт
                       (был ok/неизвестно -> стал alert). Только они уходят в чат.
      - recovered   — сущности, которые ТОЛЬКО ЧТО вышли из алерта
                       (был alert -> стал ok).
    Если статус не изменился (alert->alert или ok->ok) — сущность НЕ
    попадает ни в один из списков, повторный алерт не шлётся.
    Окружения с ошибкой получения данных (entry["Статус"]) не трогают
    состояние алертов вообще.
    """
    new_alerts, recovered = [], []
    for entry in results:
        if entry.get("Статус"):
            continue

        name = entry["Проект"]
        is_alert_now = bool(entry.get("alert"))
        current_status = "alert" if is_alert_now else "ok"
        last_status = get_last_alert_status(name)

        if is_alert_now and last_status != "alert":
            new_alerts.append(entry)
        elif not is_alert_now and last_status == "alert":
            recovered.append(entry)

        set_alert_status(name, current_status, now_ts)

    return new_alerts, recovered


def get_norm(environment: str, total: int):
    """
    Норма задаётся ВРУЧНУЮ через ENVIRONMENT_NORMS (доля от total).
    Если для окружения норма не задана — возвращает None: сравнивать
    не с чем, алерт по такому окружению не проверяется.
    """
    share = ENVIRONMENT_NORMS.get(environment)
    if share is None:
        return None
    return share * total


def get_history_trend(environment: str, now_ts: float, days: int = 7,
                       tolerance_min: int = TIME_TOLERANCE_MIN):
    """
    Возвращает список (день_смещение, дата, active) за последние `days`
    дней, включая сегодня (offset=0) — по одному ближайшему снапшоту
    на день. Используется для тестовой отправки/визуализации тренда.
    """
    conn = sqlite3.connect(DB_PATH)
    trend = []
    for day_offset in range(days, -1, -1):  # от старых к новым, включая сегодня
        target_ts = now_ts - day_offset * 86400
        low = target_ts - tolerance_min * 60
        high = target_ts + tolerance_min * 60
        cur = conn.execute(
            """
            SELECT active, ts FROM snapshots
            WHERE environment = ? AND ts BETWEEN ? AND ?
            ORDER BY ABS(ts - ?) ASC
            LIMIT 1
            """,
            (environment, low, high, target_ts),
        )
        row = cur.fetchone()
        date_label = datetime.fromtimestamp(target_ts).strftime("%d.%m")
        if row:
            trend.append((day_offset, date_label, row[0]))
        else:
            trend.append((day_offset, date_label, None))
    conn.close()
    return trend


def generate_trend_message(results: list, now_ts: float, days: int = 7) -> str:
    """Тестовое сообщение: текущий снимок + тренд активных камер за N дней."""
    message = f"🧪 Тестовая сводка — активные камеры за последние {days} дней:\n"
    for entry in results:
        name = entry["Проект"]
        message += f"\nПроект: {name}\n"
        if entry.get("Статус"):
            message += f"Статус: {entry['Статус']}\n"
            continue

        trend = get_history_trend(name, now_ts, days=days)
        points = []
        for day_offset, date_label, active in trend:
            label = "сегодня" if day_offset == 0 else date_label
            points.append(f"{label}: {active if active is not None else 'н/д'}")
        message += "  " + " | ".join(points) + "\n"
        message += f"Сейчас активных: {entry['Активные камеры']} из {entry['Общее количество камер']}\n"
        message += f"Норма: {entry['Норма активных камер']}\n"
    return message


# ============================================================
#                 ПОЛУЧЕНИЕ И ПАРСИНГ МЕТРИК
# ============================================================

def parse_cameras_metrics(metrics_text: str) -> list[dict]:
    """Аналог parse_cameras_metrics из вашего сервиса — берём только нужные лейблы."""
    cameras = []
    for family in text_string_to_metric_families(metrics_text):
        if family.name != "cameras":
            continue
        for sample in family.samples:
            if sample.name != "cameras":
                continue
            cameras.append(dict(sample.labels))
    return cameras


def fetch_cameras(metrics_url: str) -> list[dict]:
    resp = requests.get(metrics_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return parse_cameras_metrics(resp.text)


# ============================================================
#                 ОБРАБОТКА ОДНОГО ОКРУЖЕНИЯ
# ============================================================

def process_environment(env: dict) -> dict:
    name = env["name"]
    metrics_url = env["metrics_url"]
    now_ts = time.time()

    entry = {"Проект": name}

    try:
        cameras = fetch_cameras(metrics_url)
    except Exception as e:
        logger.error(f"[{name}] Ошибка получения метрик с {metrics_url}: {e}")
        entry["Статус"] = f"⚠️ Нет ответа от {metrics_url}"
        return entry

    total = len(cameras)
    active = sum(1 for c in cameras if c.get("is_active") == "True")
    archive = sum(1 for c in cameras if c.get("archive_recording_enabled") == "True")

    logger.info(f"[{name}] OK: получено {total} камер, активных {active}, архивных {archive}")

    # Сохраняем снапшот — используется только для тренда в --test-send,
    # на расчёт нормы и алерты больше не влияет
    save_snapshot(name, now_ts, total, active)

    norm_active = get_norm(name, total)

    entry.update({
        "Общее количество камер": total,
        "Активные камеры": active,
        "Архивные камеры": archive,
        "Норма активных камер": round(norm_active, 1) if norm_active else "н/д",
    })

    if norm_active and norm_active > 0:
        drop_ratio = (norm_active - active) / norm_active
        entry["Падение от нормы, %"] = round(drop_ratio * 100, 1)

        if drop_ratio >= DROP_THRESHOLD:
            entry["alert"] = (
                f"🔴 {name}: активных камер {active} из нормы ~{norm_active:.0f} "
                f"(падение {drop_ratio * 100:.1f}%)"
            )
    else:
        entry["Падение от нормы, %"] = "н/д"
        entry["Примечание"] = f"Норма не задана вручную для «{name}» в ENVIRONMENT_NORMS — алерт не проверяется"
        logger.info(f"[{name}] Норма не задана вручную — алерт по этому окружению не проверяется")

    return entry


# ============================================================
#                    ФОРМИРОВАНИЕ СООБЩЕНИЙ
# ============================================================

def generate_report_message(results: list) -> str:
    message = "Отчет по активности камер:\n"
    for entry in results:
        message += f"\nПроект: {entry['Проект']}\n"
        if entry.get("Статус"):
            message += f"Статус: {entry['Статус']}\n"
            continue
        message += f"Общее количество камер: {entry['Общее количество камер']}\n"
        message += f"Активные камеры: {entry['Активные камеры']}\n"
        message += f"Архивные камеры: {entry['Архивные камеры']}\n"
        message += f"Норма активных камер: {entry['Норма активных камер']}\n"
        message += f"Отклонение от нормы: {entry['Падение от нормы, %']}%\n"
        if entry.get("Примечание"):
            message += f"Примечание: {entry['Примечание']}\n"
    return message


def generate_alert_message(results: list):
    alerts = [entry["alert"] for entry in results if entry.get("alert")]
    if not alerts:
        return None
    message = "⚠️ Обнаружено падение активности камер:\n\n"
    message += "\n".join(alerts)
    return message


def generate_recovery_message(recovered: list):
    if not recovered:
        return None
    lines = []
    for entry in recovered:
        lines.append(
            f"✅ {entry['Проект']}: активность восстановлена "
            f"({entry['Активные камеры']} активных, норма ~{entry['Норма активных камер']})"
        )
    return "Активность восстановилась:\n\n" + "\n".join(lines)


# ============================================================
#                      ОТПРАВКА В CHAT
# ============================================================

def send_to_chat(message: str):
    headers = {"Content-Type": "application/json"}
    payload = {"text": message}
    try:
        response = requests.post(CHAT_WEBHOOK_URL, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        logger.info("Сообщение успешно отправлено в Google Chat.")
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")


# ============================================================
#                            MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Мониторинг активности камер")
    parser.add_argument(
        "--test-send", action="store_true",
        help="Собрать текущий снимок + тренд за 7 дней и отправить в чат "
             "независимо от того, сработал алерт или нет (для проверки вебхука и парсинга).",
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Отправить полный отчёт (как в --test-send, но без тренда) вместо проверки алертов.",
    )
    args = parser.parse_args()

    start_time = time.time()
    init_db()

    mode = "test-send" if args.test_send else ("report" if args.report else "alert-check")
    logger.info(f"=== Запуск скрипта, режим={mode}, окружений={len(ENVIRONMENTS)} ===")

    with ThreadPoolExecutor(max_workers=len(ENVIRONMENTS)) as executor:
        results = list(executor.map(process_environment, ENVIRONMENTS))

    ok_envs = [r["Проект"] for r in results if not r.get("Статус")]
    failed_envs = [r["Проект"] for r in results if r.get("Статус")]

    now_ts = time.time()

    if args.test_send:
        message = generate_trend_message(results, now_ts, days=7)
        send_to_chat(message)
        logger.info("Тестовое сообщение отправлено.")
    elif args.report:
        message = generate_report_message(results)
        send_to_chat(message)
    else:
        new_alerts, recovered = process_alert_states(results, now_ts)

        alert_message = generate_alert_message(new_alerts)
        if alert_message:
            send_to_chat(alert_message)
            logger.info(f"Алерт отправлен: новых просадок — {len(new_alerts)}.")
        else:
            logger.info("Новых просадок нет (либо активность в норме, либо проблема уже зафиксирована ранее).")

        if ENABLE_RECOVERY_NOTIFICATIONS:
            recovery_message = generate_recovery_message(recovered)
            if recovery_message:
                send_to_chat(recovery_message)
                logger.info(f"Отправлено уведомление о восстановлении: {len(recovered)}.")

    duration = time.time() - start_time

    status = "УСПЕШНО" if not failed_envs else f"ЧАСТИЧНО ({len(failed_envs)} из {len(ENVIRONMENTS)} с ошибкой)"
    logger.info(
        f"=== Запуск завершён: {status}. "
        f"OK: {', '.join(ok_envs) if ok_envs else '-'}. "
        f"Ошибки: {', '.join(failed_envs) if failed_envs else '-'}. "
        f"Длительность: {duration:.2f} сек. ==="
    )


if __name__ == "__main__":
    main()
