"""
Каждый запуск скрипта забирает текущий снимок (total/active камер)
и парсит его так же, как ваш сервис (prometheus_client.parser).
"Норма" задаётся ВРУЧНУЮ в ENVIRONMENT_NORMS — доля активных камер
от общего числа камер по окружению (например, 0.74). Итоговая
норма в штуках = доля * текущий total.
Если текущее значение активных камер упало относительно нормы на
DROP_THRESHOLD (10%) и более — шлём алерт в Google Chat.
Повторно алерт по одному и тому же окружению НЕ шлётся, пока
проблема не устранится и не возникнет снова — статус хранится в
таблице alert_state в SQLite (camera_history.sqlite3), с гистерезисом
через DROP_THRESHOLD/RECOVERY_THRESHOLD (см. ниже).

ВАЖНО: для окружения, для которого не задана норма в ENVIRONMENT_NORMS,
алерты не проверяются вообще (нет базы для сравнения) — впишите долю
вручную для каждого нужного окружения.
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

# Порог для ВОССТАНОВЛЕНИЯ — должен быть меньше DROP_THRESHOLD. Это
# гистерезис: если норма ~1000, а активных то 899, то 901, то 899 -
# без зазора между "заалертить" и "восстановить" статус будет дёргаться
# туда-сюда на паре камер. Поэтому выйти из алерта можно только когда
# падение опустится НИЖЕ RECOVERY_THRESHOLD (а не просто ниже DROP_THRESHOLD).
# Пока падение между RECOVERY_THRESHOLD и DROP_THRESHOLD — статус не
# меняется (остаётся тем, каким был).

RECOVERY_THRESHOLD = 0.05    # восстановление засчитывается при падении <=5%
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
# "ЕСВМ Алматы": 0.74  — ожидаем стабильно ~74% камер активными (45к из 61к)
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

# Слать recovery-уведомление, когда сущность переходит из ALERT обратно в OK. Если не нужно - то написать False.
ENABLE_RECOVERY_NOTIFICATIONS = True


# ============================================================
#                 ХРАНИЛИЩЕ ИСТОРИИ (SQLite)
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_state (
            entity TEXT PRIMARY KEY,
            status TEXT NOT NULL,     -- 'ok' | 'alert'
            updated_ts INTEGER NOT NULL
        )
    """)
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
    Сравнивает текущее падение активности каждого окружения с последним
    сохранённым статусом и обновляет состояние. Используется гистерезис
    (см. DROP_THRESHOLD/RECOVERY_THRESHOLD выше): войти в alert можно
    только при падении >= DROP_THRESHOLD, а выйти обратно в ok — только
    при падении <= RECOVERY_THRESHOLD. Пока значение "болтается" между
    этими порогами, статус не меняется — это защищает от дребезга
    алерт/восстановление на паре камер туда-сюда.

    Возвращает (new_alerts, recovered):
      - new_alerts  — сущности, у которых ТОЛЬКО ЧТО начался алерт
                       (был ok/неизвестно -> стал alert). Только они уходят в чат.
      - recovered   — сущности, которые ТОЛЬКО ЧТО вышли из алерта
                       (был alert -> стал ok).
    Если статус не изменился (alert->alert или ok->ok) — сущность НЕ
    попадает ни в один из списков, повторный алерт не шлётся.
    Окружения с ошибкой получения данных (entry["Статус"]) или без
    заданной нормы (entry["_drop_ratio"] is None) не трогают состояние.
    """
    new_alerts, recovered = [], []
    for entry in results:
        if entry.get("Статус"):
            continue

        drop_ratio = entry.get("_drop_ratio")
        if drop_ratio is None:
            continue  # нет нормы - не с чем сравнивать, состояние не трогаем

        name = entry["Проект"]
        last_status = get_last_alert_status(name)

        if last_status == "alert":
            # Уже в алерте - выходим только при уверенном восстановлении
            current_status = "alert" if drop_ratio > RECOVERY_THRESHOLD else "ok"
        else:
            # Были в ok (или это первый запуск) - входим в алерт по обычному порогу
            current_status = "alert" if drop_ratio >= DROP_THRESHOLD else "ok"

        if current_status == "alert" and last_status != "alert":
            new_alerts.append(entry)
        elif current_status == "ok" and last_status == "alert":
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
        entry["_drop_ratio"] = drop_ratio  # для гистерезиса в process_alert_states

        if drop_ratio >= DROP_THRESHOLD:
            entry["alert"] = (
                f"🔴 {name}: активных камер {active} из нормы ~{norm_active:.0f} "
                f"(падение {drop_ratio * 100:.1f}%)"
            )
    else:
        entry["Падение от нормы, %"] = "н/д"
        entry["_drop_ratio"] = None
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

def send_to_chat(message: str, silent: bool = False):
    """
    silent=True — режим dry-run: сообщение не отправляется, только
    логируется (первые ~200 символов) как "было бы отправлено". Состояние
    алертов (alert_state) при этом всё равно обновляется как обычно —
    иначе после выхода из тихого режима гистерезис/антиспам собьётся.
    """
    if silent:
        preview = message[:200].replace("\n", " ")
        logger.info(f"[SILENT] Сообщение НЕ отправлено (dry-run): {preview}...")
        return

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
        "--report", action="store_true",
        help="Отправить полный отчёт по всем окружениям в чат вместо проверки алертов "
             "(удобно для разовой проверки вебхука и парсинга).",
    )
    parser.add_argument(
        "--silent", action="store_true",
        help="Тихий режим (dry-run): все проверки и запись состояния алертов "
             "выполняются как обычно, но сообщения в чат НЕ отправляются — "
             "только логируются. Полезно для отладки без спама в чат.",
    )
    args = parser.parse_args()

    start_time = time.time()
    init_db()

    mode = "report" if args.report else "alert-check"
    logger.info(f"=== Запуск скрипта, режим={mode}, silent={args.silent}, окружений={len(ENVIRONMENTS)} ===")

    with ThreadPoolExecutor(max_workers=len(ENVIRONMENTS)) as executor:
        results = list(executor.map(process_environment, ENVIRONMENTS))

    ok_envs = [r["Проект"] for r in results if not r.get("Статус")]
    failed_envs = [r["Проект"] for r in results if r.get("Статус")]

    now_ts = time.time()

    if args.report:
        message = generate_report_message(results)
        send_to_chat(message, silent=args.silent)
    else:
        new_alerts, recovered = process_alert_states(results, now_ts)

        alert_message = generate_alert_message(new_alerts)
        if alert_message:
            send_to_chat(alert_message, silent=args.silent)
            logger.info(f"Алерт {'(silent) ' if args.silent else ''}отправлен: новых просадок — {len(new_alerts)}.")
        else:
            logger.info("Новых просадок нет (либо активность в норме, либо проблема уже зафиксирована ранее).")

        if ENABLE_RECOVERY_NOTIFICATIONS:
            recovery_message = generate_recovery_message(recovered)
            if recovery_message:
                send_to_chat(recovery_message, silent=args.silent)
                logger.info(f"Отправлено {'(silent) ' if args.silent else ''}уведомление о восстановлении: {len(recovered)}.")

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
