# Лабораторна робота №4
## «Потокова обробка даних з Kafka Streams»

---

## 1. Мета роботи

Розробка Java-застосунку з використанням **Quarkus + Kafka Streams** для виконання агрегацій у реальному часі над даними про поїздки, що надходять з Kafka-топіку.

---

## 2. Завдання

1. Створити **Java-проєкт** (Maven + Quarkus).
2. Додати залежності **quarkus-kafka-streams**.
3. Підписатися на один з топіків з лабораторної роботи №3 та обчислити наступне (агрегації за **датою поїздки**):
   - а. Яка **середня тривалість** поїздки за кожен день?
   - б. **Скільки поїздок** здійснено за кожен день?
   - в. Яка **найпопулярніша початкова станція** за кожен день?
   - г. Які **топ-3 станції** зустрічаються найчастіше за кожен день (враховуючи як початкові, так і кінцеві)?
4. Результат записується у топік `trips-aggregated`.
5. Зафіксувати всі зміни у **Git-репозиторії** та надіслати посилання в Moodle.

---

## 3. Обладнання та матеріали

- Персональний комп'ютер (Linux, x86-64)
- Docker 29.x / Docker Compose
- Java 21 (всередині образу)
- Apache Maven 3.9 (всередині образу збірки)
- Quarkus 3.34.1 з розширенням `quarkus-kafka-streams`
- quay.io/strimzi/kafka:latest-kafka-4.1.0 (KRaft, без Zookeeper)
- provectuslabs/kafka-ui:latest
- Python 3.12 + kafka-python-ng (producer для посіву даних)
- Набір даних: Divvy Bike Trips Q4 2019

---

## 4. Практична частина

### 4.1. Вибір технологій

| Компонент | Вибір |
|---|---|
| Мова | Java 21 |
| Фреймворк | Quarkus 3.34.1 |
| Збірка | Maven 3.9 (quarkus-maven-plugin) |
| Потокова обробка | quarkus-kafka-streams |
| Серіалізація | JSON (ObjectMapperSerde від Quarkus) |
| Брокер | Strimzi KRaft (без Zookeeper) |
| Контейнеризація | Multi-stage Docker (Maven builder → UBI JRE runtime) |

---

### 4.2. Структура проєкту

```
lab4/
├── docker-compose.yml              # Kafka (KRaft) + Producer + Streams App + Kafka UI
├── producer/                       # Python producer (посів даних)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py
│   └── consumer.py
└── streams-app/
    ├── Dockerfile                  # Multi-stage: Maven → UBI JRE
    ├── pom.xml
    └── src/main/java/ua/edu/chnu/lab4/
        ├── TripEvent.java          # POJO вхідного повідомлення
        ├── DayAggregation.java     # Стан агрегації (avg, count, popular, top3)
        └── TopologyProducer.java   # CDI-бін з @Produces Topology
```

---

### 4.3. Архітектура Quarkus Kafka Streams

Quarkus-застосунок реєструє `Topology` через CDI (`@Produces`). Quarkus сам запускає `KafkaStreams`, керує підключенням і health checks.

```java
@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        // ...
        return builder.build();
    }
}
```

---

### 4.4. Схема обробки

```
trips-topic1
     │
     ▼
KStream<String, TripEvent>   (ObjectMapperSerde)
     │
     └── groupBy(start_time[0:10])
              │
              └── aggregate(DayAggregation)
                       │ count, avgDuration,
                       │ popularStartStation, top3Stations
                       ▼
               trips-aggregated   (JSON per day key)
```

---

### 4.5. Налаштування (`application.properties`)

```properties
quarkus.kafka-streams.bootstrap-servers=localhost:9092
quarkus.kafka-streams.topics=trips-topic1

kafka-streams.auto.offset.reset=earliest
kafka-streams.commit.interval.ms=1000
```

У Docker Compose перевизначається через env:
```yaml
QUARKUS_KAFKA_STREAMS_BOOTSTRAP_SERVERS: kafka:9092
```

---

### 4.6. Запуск та перевірка

**Запуск:**

```bash
cd lab4
docker compose up --build
```

Producer посіває 10 000 записів у `trips-topic1`. Streams-застосунок обробляє з `auto.offset.reset=earliest`.

**Перевірка через Kafka UI** (http://localhost:8089):

Топік `trips-aggregated` — повідомлення з ключем `"2019-10-01"` і JSON-значенням:

```json
{
  "count": 415,
  "avgDuration": 742.50,
  "popularStartStation": "Streeter Dr & Grand Ave",
  "top3Stations": ["Streeter Dr & Grand Ave", "Lake Shore Dr & Monroe St", "Millennium Park"]
}
```

---

## 5. Висновки

В ході виконання лабораторної роботи було:

- Розроблено **Quarkus-застосунок** (Java 21, Maven) з розширенням `quarkus-kafka-streams` для потокової обробки даних про велосипедні поїздки.
- Реалізовано **4 агрегації** за датою: середня тривалість, кількість поїздок, найпопулярніша початкова станція та топ-3 станцій (start+end) — у єдиному стані `DayAggregation`.
- Використано **CDI-підхід** (`@ApplicationScoped` + `@Produces Topology`) замість `main()`-методу, як у прикладі від кафедри.
- Застосовано **Strimzi KRaft** брокер (без Zookeeper) та `ObjectMapperSerde` від Quarkus.
- Перевірено надходження агрегованих результатів через **Kafka UI** (http://localhost:8089).
