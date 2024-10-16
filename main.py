import os
import json
import sqlalchemy
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from prettytable import PrettyTable
from dotenv import load_dotenv
from model import Base
from model import Publisher
from model import Book
from model import Shop
from model import Stock
from model import Sale


def create_tables(engine):
# Функция удаления и создания базы данных   
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

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
    database_url = os.getenv('DATABASE_URL')
    engine = sqlalchemy.create_engine(database_url)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    with open('fixtures/tests_data.json', 'r') as fd:
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

    query = session.query(Publisher.id, Publisher.name)
    avtors = [avtor for avtor in query]
    table_title = ["id", "Автор"]
    print(f'Издатели в базе данных:')
    print(table_print(table_title, avtors))

    avtor = input('Введите id издателя.')
    if avtor !='' and [tup for tup in avtors if int(avtor) in tup]:
        query = session.query(Publisher.id, Book.title, Shop.name, Sale.price, Sale.date_sale) \
                        .filter(Publisher.id == avtor) \
                        .join(Book).join(Stock).join(Shop).join(Sale).all()
        books = [sales[1:] for sales in query]
        table_title = ["Книга", "Магазин", "Стоимость", "Дата"]    
        print(f'Книги издателя {avtors[int(avtor)-1][1]}:')
        print(table_print(table_title, books))
    else:
        print('Издатель не выбран.')
    Session.close_all