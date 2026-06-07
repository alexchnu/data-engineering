# Лабораторна робота №5
## «Побудова Data Lakehouse»

---

## 1. Мета роботи

Розширення попередньої лабораторної роботи шляхом додавання шару зберігання даних на основі **Apache Iceberg**, **Apache Polaris**, **Trino** та **MinIO** для побудови повноцінного data lakehouse.

---

## 2. Завдання

1. Додати нові сервіси до `docker-compose.yml`: Apache Polaris, Trino, MinIO, MinIO Client.
2. Налаштувати **Trino** для роботи з каталогом Iceberg через Polaris.
3. Зареєструвати каталог у **Apache Polaris** (REST-каталог Iceberg), що використовує MinIO як S3-сумісне сховище.
4. Налаштувати **RBAC** (керування доступом): ролі та права для роботи з каталогом.
5. Через **Trino** створити Iceberg-таблицю, наповнити даними та виконати SQL-запит.
6. Зафіксувати всі зміни у **Git-репозиторії** та надіслати посилання в Moodle.

---

## 3. Обладнання та матеріали

- Персональний комп'ютер (Linux, x86-64)
- Docker 29.x / Docker Compose
- Apache Polaris (apache/polaris:latest)
- Trino (trinodb/trino:latest)
- MinIO (minio/minio:latest)
- Apache Iceberg (відкритий табличний формат)
- curl, jq (для налаштування Polaris)

---

## 4. Практична частина

### 4.1. Архітектура Data Lakehouse

```
┌──────────────────────────────────────────────────────┐
│                   Клієнт / CLI                        │
└────────────────────────┬─────────────────────────────┘
                         │ SQL
                    ┌────▼────┐
                    │  Trino  │  ← розподілений SQL-рушій
                    └────┬────┘
          metadata │          │ дані (Parquet)
                   ▼          ▼
           ┌─────────┐   ┌───────┐
           │ Polaris │   │ MinIO │  ← S3-сумісне сховище
           │ (REST   │   │       │
           │ catalog)│   └───────┘
           └─────────┘
```

| Компонент | Роль |
|---|---|
| Apache Iceberg | Відкритий табличний формат (ACID, schema evolution, time travel) |
| Apache Polaris | REST-каталог Iceberg — центральний реєстр метаданих |
| Trino | SQL-рушій — читає/пише Iceberg-таблиці через коннектор |
| MinIO | S3-сумісне об'єктне сховище (локальна альтернатива AWS S3) |

---

### 4.2. Структура проєкту

```
lab5/
├── docker-compose.yml          # Polaris, Trino, MinIO, MinIO Client
├── setup-polaris.sh            # Автоматизація налаштування каталогу і RBAC
├── trino/
│   └── catalog/
│       └── iceberg.properties  # Конфігурація Iceberg-коннектора Trino
└── minio_data/                 # Локальний том MinIO (не включається в git)
```

---

### 4.3. Конфігурація сервісів

**Сервіси та порти:**

| Сервіс | Образ | Порт |
|---|---|---|
| polaris | apache/polaris:latest | **8181** (API), 8182 |
| trino | trinodb/trino:latest | **8090** |
| minio | minio/minio:latest | **9000** (API), **9001** (UI) |
| minio-client | minio/mc:latest | — (ініціалізація bucket) |

**Змінні оточення (Polaris):**

```yaml
AWS_ACCESS_KEY_ID: admin
AWS_SECRET_ACCESS_KEY: password
AWS_ENDPOINT_URL_S3: http://minio:9000
POLARIS_BOOTSTRAP_CREDENTIALS: default-realm,root,secret
```

---

### 4.4. Налаштування Trino (`iceberg.properties`)

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog/
iceberg.rest-catalog.warehouse=polariscatalog
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:secret
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=dummy-region
```

---

### 4.5. Налаштування Polaris (RBAC)

Скрипт `setup-polaris.sh` автоматизує наступні кроки:

1. Отримання OAuth2-токена від Polaris.
2. Створення каталогу `polariscatalog` (тип `INTERNAL`, сховище — `s3://warehouse` у MinIO).
3. Надання права `CATALOG_MANAGE_CONTENT` ролі `catalog_admin`.
4. Створення principal role `data_engineer`.
5. Призначення `catalog_admin` → `data_engineer` → `root`.

```bash
./setup-polaris.sh
```

---

### 4.6. Запуск та перевірка

**Запуск стека:**

```bash
cd lab5
docker compose up
```

**Налаштування Polaris:**

```bash
./setup-polaris.sh
```

**Підключення до Trino:**

```bash
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg
# (inside the container, port 8080 is still the internal port)
```

**Створення та наповнення таблиці:**

```sql
CREATE SCHEMA db;
USE db;

CREATE TABLE trips (
  trip_id      VARCHAR,
  start_time   VARCHAR,
  tripduration DOUBLE,
  from_station VARCHAR,
  to_station   VARCHAR
);

INSERT INTO trips VALUES
  ('25223640', '2019-10-01 00:01:39', 940.0,
   'Sheffield Ave & Kingsbury St', 'Leavitt St & Armitage Ave'),
  ('25223641', '2019-10-01 00:02:16', 258.0,
   'Throop (Loomis) St & Taylor St', 'Morgan St & Polk St');

SELECT * FROM trips;
```

**Розташування даних у MinIO** (http://localhost:9001, admin/password):

```
warehouse/
  db.db/
    trips/
      metadata/   ← JSON-метадані Iceberg (snapshots, schema)
      data/        ← Parquet-файли з даними
```

---

## 5. Висновки

В ході виконання лабораторної роботи було:

- Розгорнуто повноцінний **Data Lakehouse стек**: Apache Polaris + Trino + MinIO за допомогою Docker Compose.
- Налаштовано **Apache Polaris** як REST-каталог Iceberg з підтримкою OAuth2 та RBAC (ролі `catalog_admin`, `data_engineer`).
- Налаштовано **Trino** для роботи з Iceberg-таблицями через Polaris-каталог та S3-сумісне сховище MinIO.
- Автоматизовано процес ініціалізації каталогу та прав доступу (скрипт `setup-polaris.sh`).
- Створено Iceberg-таблицю через Trino SQL, виконано `INSERT` та `SELECT` — дані збережено у форматі **Parquet** у MinIO.
- Перевірено розташування файлів метаданих і даних у MinIO Console (http://localhost:9001).
