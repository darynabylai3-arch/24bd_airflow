-- База airflow создаётся самим контейнером postgres (POSTGRES_DB).
-- Здесь создаём отдельную базу для данных проекта.
CREATE DATABASE telegram_data;

\connect telegram_data

CREATE TABLE IF NOT EXISTS telegram_messages (
    chat_id          BIGINT       NOT NULL,
    message_id       BIGINT       NOT NULL,
    sender_id        BIGINT,
    text             TEXT,
    sent_at          TIMESTAMPTZ  NOT NULL,
    reply_to_msg_id  BIGINT,
    edited_at        TIMESTAMPTZ,
    collected_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_sent_at ON telegram_messages (chat_id, sent_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender  ON telegram_messages (chat_id, sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_reply   ON telegram_messages (chat_id, reply_to_msg_id);

-- Для инкрементальной загрузки: последний обработанный message_id по каждому чату
CREATE TABLE IF NOT EXISTS collector_state (
    chat_key         TEXT         PRIMARY KEY,
    last_message_id  BIGINT       NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
