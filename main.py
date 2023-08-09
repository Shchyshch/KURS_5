from config import config
from dbmanager import DBManager
from funcs import get_hh_data, insert_data_into_db


def main():
    params = config()
    db = DBManager(dbname='postgres', **params)
    db.drop_tables()
    db.create_tables()

    employer_ids = [
        78638,
        1740,
        3529,
        1367924,
        99993,
        10147101,
        135317,
        63387,
        1488652,
        1868342
    ]

    for employer_id in employer_ids:
        url = f'https://api.hh.ru/vacancies?employer_id={employer_id}&per_page=50'
        data = get_hh_data(url)
        insert_data_into_db(data, db)

    print(db.get_companies_and_vacancies_count())
    print(db.get_all_vacancies())
    print(db.get_avg_salary())
    print(db.get_vacancies_with_higher_salary())
    print(db.get_vacancies_with_keyword('Python'))


if __name__ == '__main__':
    main()
