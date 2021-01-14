"""Примеры использования."""
from create_table import create_table


create_table(name="clients")

create_table(schema="test_dds", name="clients", distributed_by="birthday", rows_num=5)