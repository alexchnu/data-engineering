# Лабораторна робота №3
## «Producer/Consumer з Apache Kafka»

---

## 1. Мета роботи

Ознайомлення з можливостями Apache Kafka. Налаштування Kafka-кластера за допомогою Docker. Створення producer та consumer, що взаємодіють з Kafka-кластером.

---

## 2. Завдання

1. Створити Python-проєкт, що виконує роль **producer**.
2. Producer зчитує дані з **CSV-файлу** і для кожного рядка формує повідомлення/подію для надсилання до Kafka-кластера.
3. Kafka-кластер повинен містити: Zookeeper, Broker1, Broker2, Kafka UI.
4. Kafka-кластер повинен мати **два топіки** (Topic1, Topic2). Кожне повідомлення публікується в обидва топіки одночасно.
5. За допомогою **Kafka UI** перевірити надходження повідомлень.
6. Producer та Kafka-кластер запускаються через **Docker**.
7. Зафіксувати всі зміни у **Git-репозиторії** та надіслати посилання в Moodle.

---

## 3. Обладнання та матеріали

- Персональний комп'ютер (Linux, x86-64)
- Docker 29.x / Docker Compose
- Python 3.12 (всередині образу)
- kafka-python 2.0.2
- confluentinc/cp-zookeeper:7.6.1
- confluentinc/cp-kafka:7.6.1
- provectuslabs/kafka-ui:latest
- Набір даних: Divvy Bike Trips Q4 2019 (`Divvy_Trips_2019_Q4.csv`, ~704 тис. записів)

---

## 4. Практична частина

### 4.1. Вибір технологій

| Компонент | Вибір |
|---|---|
| Мова producer/consumer | Python 3.12 |
| Kafka-клієнт | kafka-python 2.0.2 |
| Серіалізація | JSON (UTF-8) |
| Кластер | Confluent Platform (cp-zookeeper + cp-kafka) |
| Моніторинг | Kafka UI (provectuslabs) |

---

### 4.2. Структура проєкту

```
lab3/
├── docker-compose.yml      # Zookeeper, Broker1, Broker2, Kafka UI, Producer
└── producer/
    ├── Dockerfile
    ├── requirements.txt
    ├── producer.py          # читає CSV, публікує в обидва топіки
    └── consumer.py          # зчитує повідомлення з trips-topic1
```

---

### 4.3. Архітектура Kafka-кластера

```
Zookeeper
   │
   ├── Broker1 (29092)
   └── Broker2 (29093)
          │
    ┌─────┴──────┐
    │            │
trips-topic1  trips-topic2
(3 partitions, RF=2)
```

| Сервіс | Образ | Порт (хост) |
|---|---|---|
| zookeeper | cp-zookeeper:7.6.1 | — |
| broker1 | cp-kafka:7.6.1 | 9092 |
| broker2 | cp-kafka:7.6.1 | 9093 |
| kafka-ui | provectuslabs/kafka-ui | **8088** |
| producer | (збирається локально) | — |

---

### 4.4. Набір даних

Файл: `Divvy_Trips_2019_Q4.csv` (~704 тис. рядків, 93 МБ).

Схема: `trip_id, start_time, end_time, bikeid, tripduration, from_station_id, from_station_name, to_station_id, to_station_name, usertype, gender, birthyear`

CSV монтується у контейнер тільки для читання — до репозиторію не включається.

---

### 4.5. Producer

`producer.py` читає CSV через `csv.DictReader`, для кожного рядка формує JSON-повідомлення і надсилає **одночасно** в `trips-topic1` та `trips-topic2`.

```python
for topic in TOPICS:
    producer.send(topic, key=row["trip_id"], value=msg)
```

Ключ повідомлення — `trip_id`. Кожні 500 повідомлень виконується `producer.flush()`.

Змінні оточення:

| Змінна | За замовчуванням | Опис |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `broker1:29092,broker2:29093` | Bootstrap-сервери |
| `TOPICS` | `trips-topic1,trips-topic2` | Топіки для публікації |
| `CSV_PATH` | `/data/Divvy_Trips_2019_Q4.csv` | Шлях до файлу всередині контейнера |
| `MAX_ROWS` | `5000` | Ліміт записів (0 = усі 704 тис.) |

---

### 4.6. Consumer

`consumer.py` підключається до `trips-topic1` та виводить у консоль `partition`, `offset`, `trip_id` та дату поїздки.

```bash
docker compose run --rm \
  -e CONSUME_TOPIC=trips-topic1 \
  producer python consumer.py
```

---

### 4.7. Запуск та перевірка

**Збірка та запуск:**

```bash
cd lab3
docker compose up --build
```

**Перевірка через Kafka UI:**

Відкрити http://localhost:8088 → вкладка **Topics** → `trips-topic1` та `trips-topic2` — повинні відображатись повідомлення з JSON-подіями поїздок.

**Приклад повідомлення в топіку:**

```json
{
  "trip_id": "25223640",
  "start_time": "2019-10-01 00:01:39",
  "tripduration": 940.0,
  "from_station_name": "Sheffield Ave & Kingsbury St",
  "to_station_name": "Leavitt St & Armitage Ave",
  "usertype": "Subscriber"
}
```

---

## 5. Висновки

В ході виконання лабораторної роботи було:

- Розгорнуто **Kafka-кластер** з двома брокерами та Zookeeper за допомогою Docker Compose.
- Розроблено **Python producer**, який зчитує дані з CSV-файлу (704 тис. записів) та публікує JSON-повідомлення **одночасно** в два топіки (`trips-topic1`, `trips-topic2`).
- Реалізовано **consumer**, що зчитує та виводить повідомлення з топіку.
- Перевірено надходження повідомлень через **Kafka UI** (http://localhost:8088).
- Усі сервіси контейнеризовано та запускаються єдиною командою `docker compose up`.
