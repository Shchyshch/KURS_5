import requests
from typing import Any


def get_hh_data(url: str) -> dict:
    """Получает данные о вакансиях с указанного сайта"""
    response = requests.get(url)
    return response.json()


def insert_data_into_db(data: dict, db: Any) -> None:
    """Сохраняет данные о работодателях и вакансиях в таблицы БД"""
    with db.conn.cursor() as cur:
        employer_name = data['items'][0]['employer']['name']
        cur.execute(
            """
            INSERT INTO employers (employer_name)
            VALUES (%s)
            RETURNING employer_id
            """,
            (employer_name,)
        )
        employer_id = cur.fetchone()[0]
        for vacancy in data['items']:
            if vacancy['salary'] is not None:
                if vacancy['salary']['from'] is not None:
                    if vacancy['salary']['currency'] == 'RUR':
                        cur.execute(
                            """
                            INSERT INTO vacancies (employer_id, title, salary, vacancy_url)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (employer_id, vacancy['name'], vacancy['salary']['from'], vacancy['alternate_url'])
                        )
    db.conn.commit()
