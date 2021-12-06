import timeit
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine, Integer, String, \
    Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker, relationship
from sqlalchemy.sql.functions import count
from sqlalchemy.sql.sqltypes import Float, Text

Base = declarative_base()

engine = create_engine("postgresql+psycopg2://postgres:1337@localhost/library_db")
Base.metadata.bind = engine
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
engine.connect()

class Author(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    fullname = Column(String(50))
    country = Column(String(50))
    books = relationship('Book')
    __table_args__ = {'extend_existing': True}

class Book(Base):
    __tablename__ = 'books'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    title = Column(String(50))
    rating = Column(Float)
    author_id = Column(Integer, ForeignKey('authors.id'))
    abonement_id = Column(Integer, ForeignKey('abonements.id'))
    __table_args__ = {'extend_existing': True}

class Reader(Base):
    __tablename__ = 'readers'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    username = Column(Text)
    number_of_read = Column(Integer)
    abonement_id = Column(Integer, ForeignKey('abonements.id'))
    __table_args__ = {'extend_existing': True}

class Abonement(Base):
    __tablename__ = 'abonements'
    id = Column(Integer, primary_key=True, unique=True, nullable=False)
    price = Column(Float)
    expiring_time = Column(Integer)
    __table_args__ = {'extend_existing': True}



class authors:

    def __init__(self):  
        self.id = 0  
        self.fullname = ""
        self.country = ""

    def create(self, id, fullname, country):
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            session = Session()
            session.add(Author(
                id = id,
                fullname = fullname,
                country = country
            ))
            session.commit()
            print("Entity successfully inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def update(self, id, fullname, country):
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            t = session.query(Author).get(id)
            t.fullname = fullname
            t.country = country
            session.add(t)
            session.commit()
            print("Entity successfully updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def delete(self, id):
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            t = session.query(Author).get(id)
            session.delete(t)
            session.commit()
            print("Entity successfully deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")


class books:

    def __init__(self):  
        self.id = 0  
        self.title = ""
        self.rating = 0
        self.author_id = -1  
        self.abonement_id = -1 


    def create(self, id, title, rating, author_id, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            session = Session()
            session.add(Book(
                id = id,
                title = title,
                rating = rating,
                author_id = author_id,
                abonement_id = abonement_id
            ))
            session.commit()
            print("Entity successfully inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")
    
    def update(self, id, title, rating, author_id, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            t = session.query(Book).get(id)
            t.title = title
            t.rating = rating
            t.author_id = author_id
            t.abonement_id = abonement_id
            session.add(t)
            session.commit()
            print("Entity successfully updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            t = session.query(Book).get(id)
            session.delete(t)
            session.commit()
            print("Entity successfully deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")
    

class readers:

    def __init__(self):  
        self.id = 0  
        self.username = ""
        self.number_of_read = 0
        self.abonement_id = -1 
    

    def create(self, id, username, number_of_read, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            session = Session()
            session.add(Reader(
                id = id,
                username = username,
                number_of_read = number_of_read,
                abonement_id = abonement_id
            ))
            session.commit()
            print("Entity successfully inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def update(self, id, username, number_of_read, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            t = session.query(Reader).get(id)
            t.username = username
            t.number_of_read = number_of_read
            t.abonement_id = abonement_id
            session.add(t)
            session.commit()
            print("Entity successfully updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            t = session.query(Reader).get(id)
            session.delete(t)
            session.commit()
            print("Entity successfully deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

     
class abonements:

    def __init__(self):  
        self.id = 0  
        self.price = 0  
        self.expiring_time = 0 


    def create(self, id, price, expiring_time):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            session = Session()
            session.add(Abonement(
                id = id,
                price = price,
                expiring_time = expiring_time
            ))
            session.commit()
            print("Entity successfully inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def update(self, id, price, expiring_time):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            t = session.query(Abonement).get(id)
            t.price = price
            t.expiring_time = expiring_time
            session.add(t)
            session.commit()
            print("Entity successfully updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            t = session.query(Abonement).get(id)
            session.delete(t)
            session.commit()
            print("Entity successfully deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            print("End of operation")

class Index:

    def testFunc(self):
        start = timeit.timeit()
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            selecr_query = """CREATE INDEX ON authors USING BTREE(id); 
                            CREATE INDEX book_name ON books USING gin (to_tsvector('english', code)); 
                            SELECT authors.fullname, authors.country, books.title from authors, books 
                            WHERE authors.id = 122495 AND books.author_id = 122495;"""
            cursor.execute(selecr_query)
            connection.commit()
            print("Result", cursor.fetchall())

        except (Exception, Error) as error:
            print("Error with PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                end = timeit.timeit()
                print("Time for operation " + str(end - start))

class Trigger:

    def create(self):
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            query = """DROP TABLE IF EXISTS book_logs;
                        CREATE TABLE book_logs(id integer NOT NULL, old_title text, new_title text, author_id integer);
                        CREATE OR REPLACE FUNCTION log_book() RETURNS trigger AS $BODY$
                        BEGIN
                            IF NEW.title IS NULL THEN
                                RAISE EXCEPTION 'Name cannot be null';
                            END IF;
                            IF NEW.author_id IS NULL THEN
                                RAISE EXCEPTION 'Book cannot have null author_id';
                            END IF;
                            INSERT INTO book_logs VALUES(OLD.id, OLD.title, NEW.title, NEW.author_id);
                            RETURN NEW;
                        END;
                    $BODY$ LANGUAGE plpgsql;
                    DROP TRIGGER IF EXISTS book_subj ON subjects;
                    CREATE TRIGGER book_subj BEFORE UPDATE OR DELETE ON books
                        FOR EACH ROW EXECUTE PROCEDURE book_subj();"""
            cursor.execute(query)
            connection.commit()
            print("Result", cursor.fetchall())

        except (Exception, Error) as error:
            print("Error with PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close() 