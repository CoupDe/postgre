import json
from math import ceil

import psycopg2
from numpy import around
from psycopg2 import sql
import processing as proc


def info_base():
    curs.execute(
        "SELECT table_name FROM information_schema.tables  WHERE table_schema='public';")  # Запрос на существующие таблицы
    table_name = curs.fetchone()
    curs.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", table_name)
    column_name = curs.fetchall()
    print(column_name)
    curs.execute("SELECT version();")
    version = curs.fetchone()
    curs.execute("SELECT id_estimate, local_num, total_price  FROM estimatestb")
    datadb = curs.fetchall()
    print(
        f"Вы подключены к - {version} \n	-> {access_db.split()[:2][0], access_db.split()[:2][1]} Таблица - {table_name}")
    return column_name, table_name, datadb


"""Подключение к БД"""
access_db = "dbname='estimatesdb' user='postgres' host='localhost' password='C0l0ssok' port='5432'"
try:
    psql_conn = psycopg2.connect(access_db)  # Создание подключения к бд
    curs = psql_conn.cursor()
    column_name, table_name, datadb = info_base()
    print("Добавить данные? ")
    # s = proc.get_data() duplicates = [] if input().lower() == "y" else print("ок")
    duplicates=[]
    data_json = proc.get_data()

    for dt in data_json:

        if (dt.id_estimate, dt.local_num, dt.total_price) in datadb:
            duplicates.append(dt.local_num)
            continue
        dd = list(dt.__dict__.values())
        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({val})").format(
            table=sql.Identifier(table_name[0]),
            columns=sql.SQL(', ').join(map(lambda x: sql.Identifier(x[0]), column_name[:-1])),
            val=sql.SQL(', ').join(sql.Placeholder() * len(dd))
        )
        values = sql.SQL(', ').join(map(sql.Literal, dd))
        dd[9] = json.dumps(dd[9])
        print(sql.SQL(', ').join(map(lambda x: sql.Identifier(x[0]), column_name[:-1])).as_string(psql_conn))
        print(dd)
        curs.execute(query, dd)

except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
    psql_conn.commit()
finally:
    if psql_conn:
        if duplicates: print("Найдены следующие дубликаты смет:", (' ').join(duplicates))
        psql_conn.commit()
        curs.close()
        psql_conn.close()
        print("Соединение с PostgreSQL закрыто")
