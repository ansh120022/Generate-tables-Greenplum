import random
import sqlalchemy_greenplum
from sqlalchemy.orm import sessionmaker
from faker import Faker
import faker_ids
from random import randint, uniform
from datetime import date
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Numeric, Boolean, LargeBinary, create_engine, MetaData, Table, insert


def create_table(
        name: str,
        schema: str = "test_wrk",
        distributed_by: str = 'id',
        rows_num: int = 100) -> str:
    """Создание таблицы."""
    # engine - пул соединений к БД
    engine = create_engine('greenplum://user:password@example.server.com/example_database')
    metadata = MetaData(schema=schema)
    clients = Table(
        name,
        metadata,
        Column('id', Integer, autoincrement=True, comment="Идентификатор"),
        Column('name', String(32), comment="Имя"),
        Column('surname', String(50), comment="Фамилия"),
        Column('phone', String(255), comment="Телефон"),
        Column('birthday', DateTime(timezone=False), comment="timestamp without time zone"),
        Column('valid_from_dttm', DateTime(timezone=True), comment="timestamp with time zone"),
        Column('kids', Integer, comment="Количество детей"),
        Column('category', String(32), comment="Категория клиента"),
        Column('account_balance', Numeric(), comment="Остаток на счёте"),
        Column('email', String(255), comment="электронный адрес"),
        Column('big_int', BigInteger, comment="bigint"),
        Column('is_employee', Boolean, comment="Флаг, является клиент сотрудником или нет"),
        Column('processed_dttm', DateTime(timezone=False), default=date.today(), comment="дата добавления записи"),
        Column('password', LargeBinary, comment="Хэш пароля"),
        greenplum_storage_params='APPENDONLY=TRUE, OIDS=FALSE,ORIENTATION=COLUMN',
        greenplum_distributed_by=distributed_by
    )
    metadata.create_all(engine)
    # создание новой сессии, для выполнения действий
    Session = sessionmaker(bind=engine)
    session = Session()
    fake = Faker('ru_RU')
    values_dict = {}
    for i in range(1, rows_num):
        values_dict["id"] = i
        values_dict["name"] = fake.first_name(),
        values_dict["surname"] = fake.last_name()
        values_dict["phone"] = fake.phone_number(),
        values_dict["birthday"] = fake.date_time(tzinfo=None),
        values_dict["valid_from_dttm"] = fake.iso8601(),
        values_dict["kids"] = randint(1, 5),
        values_dict["category"] = random.choice(["AAA", "ABC", "ABB"]),
        values_dict["account_balance"] = uniform(1.5, 1.9),
        values_dict["email"] = fake.email(),
        values_dict["big_int"] = randint(-9223372036854775808, 9223372036854775807),
        values_dict["is_employee"] = randint(0, 1)
        values_dict["processed_dttm"] = date.today(),
        values_dict["password"] = fake.sha256(raw_output=True)
        a = insert(clients, values=values_dict)
        i += 1
        session.execute(a)
    session.commit()
    session.close()
    print(f"created table {schema}.{name}")