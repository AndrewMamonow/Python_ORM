import os
import json
import sqlalchemy
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from prettytable import PrettyTable
from dotenv import load_dotenv
import datetime
from model import Base
from model import Publisher
from model import Book
from model import Shop
from model import Stock
from model import Sale

def session_create(database_url):
# Функция создания сессии и создания базы данных   
    engine = sqlalchemy.create_engine(database_url)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
    
def json_load(session, file_json):
# Функция загрузки данных в базу    
    with open(file_json, 'r') as fd:
        data = json.load(fd)
        for record in data:
            model = {
                'publisher': Publisher,
                'shop': Shop,
                'book': Book,
                'stock': Stock,
                'sale': Sale,
            }[record.get('model')]
            session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()

def get_all(session):
    # Функция выборки всех данных из базы
    query = session.query(Publisher.id, Publisher.name)
    avtors = [avtor for avtor in query]
    table_title = ["id", "Автор"]
    return table_print(table_title, avtors)

def getshops(session, avtor):
# Функция выборки данных из базы по фильтру    
    query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
                        .select_from(Publisher) \
                        .join(Book).join(Stock).join(Shop).join(Sale)
    if avtor.isdigit():                    
        query = query.filter(Publisher.id == avtor).all()
    else:
        query = query.filter(Publisher.name == avtor).all()                           
    books=[]
    for sales in query:
        (book, shop, price, date_sale) = sales
        books.append((book, shop, price, date_sale))
        print (f"{book: <40} | {shop: <10} | {price: <8} | {date_sale.strftime('%d-%m-%Y')}") 
    return books

def table_print(table_title:list, books:list):
# Функция формирования таблицы для вывода   
    Pretty_table = PrettyTable()
    Pretty_table.field_names = table_title
    for book in books:
        Pretty_table.add_row(book)
    return Pretty_table


if __name__ == '__main__':
# Программа
    load_dotenv()  
    session = session_create(os.getenv('DATABASE_URL'))
    json_load(session, os.getenv('FILE_JSON'))

    print(f'Все публицисты:')
    print(get_all(session))
    avtor = input('Введите id или имя публициста.')
    if avtor !='':
        books = getshops(session, avtor)
        if len(books) == 0:
            print('Продаж нет или публицист указан не верно.')
    else:
        print('Публицист не выбран.')
    session.close_all