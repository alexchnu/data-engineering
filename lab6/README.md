# Лабораторна робота №6
## «Оркестрація ETL-пайплайну з Apache Airflow»

---

## 1. Мета роботи

Побудова простого ETL (Extract → Transform → Load) пайплайну за допомогою **Apache Airflow**, керованого через **Astro CLI**.

---

## 2. Завдання

1. Встановити консольний інструмент **Astro CLI**.
2. За допомогою Astro CLI ініціалізувати **інфраструктуру Airflow**.
3. У директорії `/dags` створити **Python-скрипт** з описом ETL-пайплайну.
4. Налаштувати **DAG** з розкладом запуску **щогодини**.
5. Задача **Extract**: згенерувати тестові дані у **JSON-форматі** (з вкладеною структурою).
6. Задача **Transform**: **вирівняти** вкладеність JSON-структури (flatten).
7. Задача **Load**: створити **DataFrame** з перетворених даних та вивести у консоль.
8. **Протестувати** пайплайн.
9. Зафіксувати всі зміни у **Git-репозиторії** та надіслати посилання в Moodle.

---

## 3. Обладнання та матеріали

- Персональний комп'ютер (Linux, x86-64)
- Docker 29.x / Docker Compose
- Astro CLI (остання версія)
- Apache Airflow 3.x (всередині Astro-середовища)
- Python 3.13
- pandas (для побудови DataFrame у задачі Load)

---

## 4. Практична частина

### 4.1. Вибір технологій

| Компонент | Вибір |
|---|---|
| Оркестратор | Apache Airflow 3.x |
| Управління середовищем | Astro CLI |
| API-стиль DAG | TaskFlow API (`@task` декоратори) |
| Передача даних між задачами | XCom (автоматично через TaskFlow) |
| Обробка даних | Python + pandas |

---

### 4.2. Структура проєкту

```
lab6/
├── dags/
│   └── etl_pipeline.py     # DAG з трьома TaskFlow-задачами
├── requirements.txt         # pandas
└── README.md

# Генерується командою `astro dev init`:
├── Dockerfile
├── airflow_settings.yaml
├── packages.txt
└── tests/
```

---

### 4.3. Схема ETL-пайплайну

```
┌─────────┐    вкладений JSON    ┌───────────┐   плаский JSON   ┌──────┐
│ extract │ ──────────────────► │ transform │ ───────────────► │ load │
│ (@task) │                     │  (@task)  │                  │(@task│
└─────────┘                     └───────────┘                  └──────┘
                                                                    │
                                                              pandas DataFrame
                                                              (вивід у лог)
```

| Задача | Опис |
|---|---|
| `extract` | Повертає список JSON-записів з вкладеними полями `station` та `rider` |
| `transform` | Вирівнює вкладеність: `station.from.name` → `from_station`, `rider.gender` → `gender` тощо |
| `load` | Будує `pandas.DataFrame` з пласких записів та виводить у лог задачі |

---

### 4.4. Реалізація DAG (`etl_pipeline.py`)

```python
@dag(schedule="@hourly", start_date=datetime(2026, 5, 22), catchup=False, max_active_runs=1)
def etl_pipeline():

    @task()
    def extract() -> list[dict]:
        return [{"trip_id": "25223640", "duration": 940,
                 "station": {"from": {"id": 20, "name": "Sheffield Ave..."},
                             "to":   {"id": 309, "name": "Leavitt St..."}},
                 "rider": {"usertype": "Subscriber", "gender": "Male"}}, ...]

    @task()
    def transform(raw: list[dict]) -> list[dict]:
        return [{"trip_id": r["trip_id"],
                 "from_station": r["station"]["from"]["name"],
                 "to_station":   r["station"]["to"]["name"],
                 "gender":       r["rider"]["gender"], ...}
                for r in raw]

    @task()
    def load(flat: list[dict]) -> None:
        df = pd.DataFrame(flat)
        print(df.to_string(index=False))

    load(transform(extract()))

etl_pipeline()
```

Дані між задачами передаються через **XCom** автоматично (механізм TaskFlow API).

---

### 4.5. Встановлення Astro CLI та запуск

**Встановлення Astro CLI:**

```bash
# Linux/macOS
curl -sSL https://install.astronomer.io | sudo bash -s
```

**Ініціалізація проєкту:**

```bash
cd lab6
astro dev init
```

Команда генерує `Dockerfile`, `airflow_settings.yaml`, `packages.txt`, `tests/`. Файли `dags/etl_pipeline.py` та `requirements.txt` вже присутні.

**Запуск Airflow:**

```bash
astro dev start
```

**Перезапуск після змін у DAG:**

```bash
astro dev restart
```

---

### 4.6. Перевірка роботи

1. Відкрити **Airflow UI**: http://lab6.localhost:6563 (admin / admin).
2. Знайти DAG **`etl_pipeline`** (може бути призупинено — увімкнути перемикач).
3. Натиснути **Trigger DAG** → ручний запуск.
4. Клікнути на запущений DAG Run → задача **`load`** → вкладка **Logs**.

**Очікуваний вивід у логах:**

```
[2026-05-26 21:00:07] INFO - 
[2026-05-26 21:00:07] INFO - === Loaded DataFrame ===
[2026-05-26 21:00:07] INFO - gender  trip_id   usertype  birthyear          start_time                to_station                   from_station  to_station_id  from_station_id  duration_seconds
[2026-05-26 21:00:07] INFO -   Male 25223640 Subscriber       1987 2019-10-01 00:01:39 Leavitt St & Armitage Ave   Sheffield Ave & Kingsbury St            309               20               940
[2026-05-26 21:00:07] INFO -   Male 25223641 Subscriber       1998 2019-10-01 00:02:16       Morgan St & Polk St Throop (Loomis) St & Taylor St            241               19               258
[2026-05-26 21:00:07] INFO - Female 25223642   Customer       1991 2019-10-01 00:04:32    Wabash Ave & Grand Ave      Milwaukee Ave & Grand Ave            199               84               850
[2026-05-26 21:00:07] INFO - 
[2026-05-26 21:00:07] INFO - Rows: 3, Columns: ['gender', 'trip_id', 'usertype', 'birthyear', 'start_time', 'to_station', 'from_station', 'to_station_id', 'from_station_id', 'duration_seconds']
```

---

## 5. Висновки

В ході виконання лабораторної роботи було:

- Встановлено **Astro CLI** та ініціалізовано Airflow-середовище командою `astro dev init`.
- Розроблено **ETL DAG** (`etl_pipeline.py`) з використанням **TaskFlow API** (`@task` декоратори).
- Реалізовано три задачі: **Extract** (генерація вкладеного JSON), **Transform** (вирівнювання структури), **Load** (побудова `pandas.DataFrame` та вивід у лог).
- DAG налаштований на **щогодинний** запуск (`schedule="@hourly"`), `max_active_runs=1`.
- Передача даних між задачами реалізована через **XCom** (автоматично завдяки TaskFlow API).
- Пайплайн протестовано вручну через Airflow UI (http://lab6.localhost:6563) — усі три задачі виконались успішно.
