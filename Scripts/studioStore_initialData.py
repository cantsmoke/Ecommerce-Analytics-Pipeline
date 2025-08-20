import random
import math
import faker
import psycopg2

fake = faker.Faker('ru_RU')
faker.Faker.seed(42)
random.seed(42)

DB_NAME = "xxxxx"
DB_USER = "xxxxx"
DB_PASSWORD = "xxxxx"
DB_HOST = "xxxxx"
DB_PORT = "xxxx"

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def generate_customers(n=50000):
    cities = [
        ("Москва", 0.35),
        ("Санкт-Петербург", 0.25),
        ("Новосибирск", 0.16),
        ("Екатеринбург", 0.14),
        ("Казань", 0.10)
    ]

    city_names = [c[0] for c in cities]
    weights = [c[1] for c in cities]

    customers = []
    for _ in range(n):
        city = random.choices(city_names, weights=weights, k=1)[0]
        customers.append((
            fake.name(),
            fake.unique.email(),
            fake.phone_number(),
            city
        ))
    return customers

brands = [
    "Yamaha", "Roland", "Behringer", "Focusrite", "PreSonus",
    "Tascam", "Steinberg", "Universal Audio", "AKAI", "M-Audio",
    "Shure", "Sennheiser", "Audio-Technica", "Neumann", "KRK",
    "Rode", "Zoom", "Arturia", "Native Instruments", "Mackie",
    "Antelope Audio", "SSL", "Audient", "Apogee", "Alesis",
    "Genelec", "EVE Audio", "Avantone", "IK Multimedia", "Blue",
    "Electro-Voice", "AKG", "JBL", "Warm Audio", "Black Lion Audio",
    "Beyerdynamic", "Focal", "Aston", "sE Electronics", "Earthworks",
    "Radial", "BOSS", "Digidesign", "Lexicon", "Moog", "Eventide",
    "Rupert Neve Designs", "Sound Devices", "HEDD Audio", "Cranborne Audio"
]

categories = [
    "Аудиоинтерфейсы", "Микрофоны", "Студийные мониторы",
    "Наушники", "Микшерные пульты", "MIDI-контроллеры",
    "Преампы", "Компрессоры", "Аналоговые синтезаторы",
    "Цифровые рекордеры", "Кабели", "Аксессуары",
    "Стойки и крепления"
]

def generate_products(n=2000, category_ids=None, brand_ids=None):
    category_weights = {
        "Аудиоинтерфейсы": 0.1, "Микрофоны": 0.1, "Студийные мониторы": 0.15,
        "Наушники": 0.2, "Микшерные пульты": 0.05, "MIDI-контроллеры": 0.1,
        "Преампы": 0.05, "Компрессоры": 0.05, "Аналоговые синтезаторы": 0.05,
        "Цифровые рекордеры": 0.04, "Кабели": 0.05, "Аксессуары": 0.04, "Стойки и крепления": 0.02
    }
    brand_weights = {b: random.uniform(0.5, 1.5) for b in brands}

    category_probs = [category_weights[c] for c in categories]
    brand_probs = [brand_weights[b] for b in brands]
    brand_probs = [p / sum(brand_probs) for p in brand_probs]

    products = []
    for _ in range(n):
        category_name = random.choices(categories, weights=category_probs, k=1)[0]
        brand_name = random.choices(brands, weights=brand_probs, k=1)[0]

        category_id = category_ids[category_name]
        brand_id = brand_ids[brand_name]

        name = f"{brand_name} {category_name} {fake.word().capitalize()}"
        
        min_price = 1000
        max_price = 150000

        mu = math.log(8000)
        sigma = 1.0

        raw_price = random.lognormvariate(mu, sigma)
        price = min(max(round(raw_price, 2), min_price), max_price)

        products.append((name, category_id, brand_id, price))
    return products

def insert_data():
    conn = get_conn()
    cur = conn.cursor()

    cur.executemany("INSERT INTO brands (name) VALUES (%s) ON CONFLICT DO NOTHING", [(b,) for b in brands])
    cur.executemany("INSERT INTO categories (name) VALUES (%s) ON CONFLICT DO NOTHING", [(c,) for c in categories])

    cur.execute("SELECT id, name FROM brands")
    brand_ids = {name: _id for _id, name in cur.fetchall()}

    cur.execute("SELECT id, name FROM categories")
    category_ids = {name: _id for _id, name in cur.fetchall()}

    products = generate_products(2000, category_ids, brand_ids)
    cur.executemany(
        "INSERT INTO products (name, category_id, brand_id, price) VALUES (%s, %s, %s, %s)",
        products
    )

    customers = generate_customers()
    cur.executemany(
        "INSERT INTO customers (full_name, email, phone, city) VALUES (%s, %s, %s, %s)",
        customers
    )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Данные успешно загружены в базу.")

if __name__ == "__main__":
    insert_data()
