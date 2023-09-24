import csv
import sqlite3
from typing import Union, List

from sqlalchemy import create_engine, select, Table, Column, Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = 'companies'
    ticker: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    sector: Mapped[str]


class Financial(Base):
    __tablename__ = 'financial'
    ticker: Mapped[str] = mapped_column(primary_key=True)
    ebitda: Mapped[float]
    sales: Mapped[float]
    net_profit: Mapped[float]
    market_price: Mapped[float]
    net_debt: Mapped[float]
    assets: Mapped[float]
    equity: Mapped[float]
    cash_equivalents: Mapped[float]
    liabilities: Mapped[float]


engine: Engine = create_engine('sqlite:///investor.db')
Base.metadata.create_all(engine)


def insert_one(record: Union[Company, Financial]):
    with Session(engine) as session:
        session.add(record)
        session.commit()


def insert_many(records: List[Union[Company, Financial]]):
    with Session(engine) as session:
        session.add_all(records)
        session.commit()


def read_data():
    with open('test/companies.csv') as companies_file:
        with open('test/financial.csv') as financial_file:
            with sqlite3.connect('investor.db') as connection:
                cursor = connection.cursor()
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS companies(
                    ticker TEXT PRIMARY KEY,
                    name TEXT,
                    sector TEXT
                )
                ''')
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS financial(
                ticker TEXT PRIMARY KEY,
                ebitda REAL,
                sales REAL,
                net_profit REAL,
                market_price REAL,
                net_debt REAL,
                assets REAL,
                equity REAL,
                cash_equivalents REAL,
                liabilities REAL
                )
                ''')
                cursor.execute('''
                DELETE FROM
                    companies
                ''')
                cursor.execute('''
                DELETE FROM
                    financial
                ''')
                file_reader = csv.DictReader(companies_file, delimiter=',')
                companies_title = ['ticker', 'name', 'sector']
                for line in file_reader:
                    values = ','.join(
                        ['\'' + line[title] + '\'' if line[title] else 'NULL' for title in companies_title])
                    cursor.execute(f'''
                    INSERT INTO 
                        companies
                    VALUES (
                        {values}
                    )
                    ''')
                file_reader = csv.DictReader(financial_file, delimiter=',')
                financial_title = ['ticker', 'ebitda', 'sales', 'net_profit',
                                   'market_price', 'net_debt', 'assets', 'equity',
                                   'cash_equivalents', 'liabilities']
                for line in file_reader:
                    values = ','.join(
                        ['\'' + line[title] + '\'' if line[title] else 'NULL' for title in financial_title])
                    cursor.execute(f'''
                    INSERT INTO 
                        financial
                    VALUES (
                        {values}
                    )
                    ''')
                connection.commit()
                # print('Database created successfully!')


def ask_for_an_option() -> str:
    return input('Enter an option:')


def main_menu():
    print('''MAIN MENU
0 Exit
1 CRUD operations
2 Show top ten companies by criteria''')
    flag: bool = False
    try:
        option: int = int(ask_for_an_option())
    except ValueError:
        flag = True
    else:
        if option == 0:
            print('Have a nice day!')
        elif option == 1:
            crud_menu()
        elif option == 2:
            top_ten_menu()
        else:
            flag = True
    finally:
        if flag:
            print('Invalid option!')
            main_menu()


def create_a_company():
    company: Company = Company(
        ticker=input("Enter ticker (in the format 'MOON'):"),
        name=input("Enter company (in the format 'Moon Corp'):"),
        sector=input("Enter industries (in the format 'Technology'):"),
    )
    financial: Financial = Financial(
        ticker=company.ticker,
        ebitda=input("Enter ebitda (in the format '987654321'):"),
        sales=input("Enter sales (in the format '987654321'):"),
        net_profit=input("Enter net profit (in the format '987654321'):"),
        market_price=input("Enter market price (in the format '987654321'):"),
        net_debt=input("Enter net debt (in the format '987654321'):"),
        assets=input("Enter assets (in the format '987654321'):"),
        equity=input("Enter equity (in the format '987654321'):"),
        cash_equivalents=input("Enter cash equivalents (in the format '987654321'):"),
        liabilities=input("Enter liabilities (in the format '987654321'):"),
    )
    insert_many([company, financial])
    print("Company created successfully!")


def read_a_company():
    name: str = input("Enter company name:")
    with Session(engine) as session:
        records = session.query(Company).filter(Company.name.like(f'%{name}%'))
        if not records.count():
            print('Company not found!')
            return
        for i in range(records.count()):
            print(i, records.all()[i].name)
        number: int = int(input('Enter company number:'))
        company = records.all()[number]
        print(company.ticker, company.name)
        financial = session.query(Financial).filter(Financial.ticker == company.ticker).one()
        tmp = round(financial.market_price / financial.net_profit, 2) \
            if financial.market_price and financial.net_profit else None
        print(f'P/E = {tmp}')
        tmp = round(financial.market_price / financial.sales, 2) \
            if financial.market_price and financial.sales else None
        print(f'P/S = {tmp}')
        tmp = round(financial.market_price / financial.assets, 2) \
            if financial.market_price and financial.assets else None
        print(f'P/B = {tmp}')
        tmp = round(financial.net_debt / financial.ebitda, 2) \
            if financial.net_debt and financial.ebitda else None
        print(f'ND/EBITDA = {tmp}')
        tmp = round(financial.net_profit / financial.equity, 2) \
            if financial.net_profit and financial.equity else None
        print(f'ROE = {tmp}')
        tmp = round(financial.net_profit / financial.assets, 2) \
            if financial.net_profit and financial.assets else None
        print(f'ROA = {tmp}')
        tmp = round(financial.liabilities / financial.assets, 2) \
            if financial.liabilities and financial.assets else None
        print(f'L/A = {tmp}')


def update_a_company():
    name: str = input('Enter company name:')
    with Session(engine) as session:
        records = session.query(Company).filter(Company.name.like(f'%{name}%'))
        if not records.count():
            print('Company not found!')
            return
        for i in range(records.count()):
            print(i, records.all()[i].name)
        number: int = int(input('Enter company number:'))
        company = records.all()[number]
        session.query(Financial).filter(Financial.ticker == company.ticker).update({
            Financial.ebitda: int(input("Enter ebitda (in the format '987654321'):")),
            Financial.sales: int(input("Enter sales (in the format '987654321'):")),
            Financial.net_profit: int(input("Enter net profit (in the format '987654321'):")),
            Financial.market_price: int(input("Enter market price (in the format '987654321'):")),
            Financial.net_debt: int(input("Enter net debt (in the format '987654321'):")),
            Financial.assets: int(input("Enter assets (in the format '987654321'):")),
            Financial.equity: int(input("Enter equity (in the format '987654321'):")),
            Financial.cash_equivalents: int(input("Enter cash equivalents (in the format '987654321'):")),
            Financial.liabilities: int(input("Enter liabilities (in the format '987654321'):")),
        })
        session.commit()
        print('Company updated successfully!')


def delete_a_company():
    name: str = input("Enter company name:")
    with Session(engine) as session:
        records = session.query(Company).filter(Company.name.like(f'%{name}%'))
        if not records.count():
            print("Company not found!")
            return
        for i in range(records.count()):
            print(i, records.all()[i].name)
        number: int = int(input("Enter company number:"))
        ticker: str = records.all()[number].ticker
        session.query(Company).filter(Company.ticker == ticker).delete()
        session.query(Financial).filter(Financial.ticker == ticker).delete()
        session.commit()
        print("Company deleted successfully!")


def list_all_companies():
    with Session(engine) as session:
        print('COMPANY LIST')
        companies = session.query(Company).order_by(Company.ticker).all()
        for company in companies:
            print(company.ticker, company.name, company.sector)


def crud_menu():
    print('''CRUD MENU
0 Back
1 Create a company
2 Read a company
3 Update a company
4 Delete a company
5 List all companies''')
    flag: bool = False
    try:
        option: int = int(ask_for_an_option())
    except ValueError:
        flag = True
    else:
        if 0 <= option <= 5:
            if option == 1:
                create_a_company()
            elif option == 2:
                read_a_company()
            elif option == 3:
                update_a_company()
            elif option == 4:
                delete_a_company()
            elif option == 5:
                list_all_companies()
            main_menu()
        else:
            flag = True
    finally:
        if flag:
            print('Invalid option!')
            main_menu()


def top_ten_by_nd_ebitda():
    print('TICKER ND/EBITDA')
    with Session(engine) as session:
        financials = session.query(Financial) \
            .order_by((Financial.net_debt/Financial.ebitda).desc()) \
            .limit(10).all()
        for i in range(10):
            print(financials[i].ticker,
                  round(financials[i].net_debt / financials[i].ebitda, 2))


def top_ten_by_roe():
    print('TICKER ROE')
    with Session(engine) as session:
        financials = session.query(Financial) \
            .order_by((Financial.net_profit / Financial.equity).desc()) \
            .limit(10).all()
        for i in range(10):
            print(financials[i].ticker,
                  round(financials[i].net_profit / financials[i].equity, 2))


def top_ten_by_roa():
    print('TICKER ROA')
    with Session(engine) as session:
        financials = session.query(Financial) \
            .order_by((Financial.net_profit / Financial.assets).desc()) \
            .limit(10).all()
        for i in range(10):
            print(financials[i].ticker,
                  round(financials[i].net_profit / financials[i].assets, 2))


def top_ten_menu():
    print('''TOP TEN MENU
0 Back
1 List by ND/EBITDA
2 List by ROE
3 List by ROA''')
    flag: bool = False
    try:
        option: int = int(ask_for_an_option())
    except ValueError:
        flag = True
    else:
        if 0 <= option <= 3:
            if option == 1:
                top_ten_by_nd_ebitda()
            elif option == 2:
                top_ten_by_roe()
            elif option == 3:
                top_ten_by_roa()
            main_menu()
        else:
            flag = True
    finally:
        if flag:
            print('Invalid option!')
            main_menu()


def main():
    print('Welcome to the Investor Program!')
    main_menu()


if __name__ == '__main__':
    read_data()
    main()
