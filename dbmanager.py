import psycopg2


class DBManager:
    """Класс для работы с таблицами Postgres"""
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    def create_tables(self) -> None:
        """Создаёт таблицы в БД"""
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE employers (
            employer_id SERIAL PRIMARY KEY,
            employer_name VARCHAR NOT NULL
            );
            
            CREATE TABLE vacancies (
            vacancy_id SERIAL PRIMARY KEY,
            employer_id INT REFERENCES employers(employer_id),
            title VARCHAR NOT NULL,
            salary INTEGER,
            vacancy_url TEXT
            )
            """)
        self.conn.commit()

    def drop_tables(self) -> None:
        """Удаляет таблицы в БД, если они там есть"""
        with self.conn.cursor() as cur:
            cur.execute("""
            DROP TABLE IF EXISTS employers CASCADE;
            DROP TABLE IF EXISTS vacancies CASCADE
            """)
        self.conn.commit()

    def get_companies_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT employer_name, COUNT(*)
            FROM employers
            JOIN vacancies
            USING(employer_id)
            GROUP BY employer_id
            """)
            rows = cur.fetchall()
        return rows

    def get_all_vacancies(self) -> list:
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT employer_name, title, salary, vacancy_url
            FROM employers
            JOIN vacancies
            USING(employer_id)
            """)
            rows = cur.fetchall()
        return rows

    def get_avg_salary(self) -> list:
        """Получает среднюю зарплату по вакансиям"""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT AVG(salary) FROM vacancies
            """)
            rows = cur.fetchall()
        return rows

    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых зарплата выше средней"""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT employer_name, title, salary, vacancy_url
            FROM employers
            JOIN vacancies
            USING(employer_id)
            WHERE salary > (SELECT AVG(salary) FROM vacancies)
            """)
            rows = cur.fetchall()
        return rows

    def get_vacancies_with_keyword(self, keyword: str) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT employer_name, title, salary, vacancy_url
            FROM employers
            JOIN vacancies
            USING(employer_id)
            WHERE title LIKE (%s)
            """, (f'%{keyword}%',))
            rows = cur.fetchall()
        return rows
