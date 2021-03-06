import psycopg2
from timeit import default_timer as timer
from psycopg2 import Error


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
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            insert_query = """ INSERT INTO authors (id, fullname, country) VALUES (%s, %s, %s)"""
            item_tuple = (id, fullname, country)
            cursor.execute(insert_query, item_tuple)
            connection.commit()
            print("Entity inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def read(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            select_query = """SELECT * from authors WHERE id = %s"""
            item_tuple = (id,)
            cursor.execute(select_query, item_tuple)
            connection.commit()
            print("Result", cursor.fetchall())
            
        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def update(self, id, fullname, country):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            update_query = """Update authors 
                                SET fullname = (%s),
                                    country = (%s) 
                                WHERE id = %s"""
            item_tuple = (fullname, country, id)
            cursor.execute(update_query, item_tuple)
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            cursor.execute("Delete from authors WHERE id = %s", [id])
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def generate(self, genNum):
        
        if (genNum < 0):
            print('Error with input!')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()

            generate_query = """INSERT INTO authors (fullname, country)
                                    SELECT md5(random()::text), md5(random()::text)
                                    FROM generate_series(1, %s)"""
            item_tuple = genNum,
            cursor.execute(generate_query, item_tuple)
            connection.commit()
            
            count = cursor.rowcount
            print(count, "Entity generated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def searchAuthorsBooks(self, fullname, title, rating):
        
        sttime = timer()

        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            select_query = """ SELECT fullname, title, rating  
                               FROM authors, books
                               WHERE rating > %s AND title LIKE %s AND fullname LIKE %s AND books.author_id = authors.id
                               ORDER BY rating DESC"""
            item_tuple = (rating, '%' + title + '%', '%' + fullname + '%')
            cursor.execute(select_query, item_tuple)
            connection.commit()
            print("Result", cursor.fetchall())
            
        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
            error()
        finally:
            if connection:
                cursor.close()
                connection.close()

                endtime = timer()

                print("Search operation take " + str((endtime - sttime) * 1000) + " ms")




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
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            insert_query = """ INSERT INTO books (id, title, rating, author_id, abonement_id) VALUES (%s, %s, %s, %s, %s)"""
            item_tuple = (id, title, rating, author_id, abonement_id)
            cursor.execute(insert_query, item_tuple)
            connection.commit()
            print("Entity inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def read(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            select_query = """SELECT * from books WHERE id = %s"""
            item_tuple = (id,)
            cursor.execute(select_query, item_tuple)
            connection.commit()
            print("Result", cursor.fetchall())
            
        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def update(self, id, title, rating, author_id, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            update_query = """Update books 
                              SET title = (%s),
                                  rating = (%s), 
                                  author_id = (%s),
                                  abonement_id = (%s)
                               WHERE id = (%s)"""
            item_tuple = (title, rating, author_id, abonement_id, id)
            cursor.execute(update_query, item_tuple)
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            cursor.execute("Delete from books WHERE id = %s", [id])
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def generate(self, genNum):
        
        if (genNum < 0):
            print('Error with input!')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()

            generate_query = """INSERT INTO books (title, rating, author_id, abonement_id)
                                SELECT md5(random()::text), (trunc(random()*10)::int), (floor(random()*((SELECT id FROM authors ORDER BY id DESC LIMIT 1) - (SELECT id FROM authors ORDER BY id LIMIT 1) + 1) + (SELECT id FROM authors ORDER BY id LIMIT 1))::int), (trunc(random()*(SELECT id FROM abonements ORDER BY id DESC LIMIT 1))::int)
                                FROM generate_series(1, %s)"""
            item_tuple = genNum,
            cursor.execute(generate_query, item_tuple)
            connection.commit()
            
            count = cursor.rowcount
            print(count, "Entity generated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    
       
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
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            insert_query = """ INSERT INTO readers (id, username, number_of_read, abonement_id) VALUES (%s, %s, %s, %s)"""
            item_tuple = (id, username, number_of_read, abonement_id)
            cursor.execute(insert_query, item_tuple)
            connection.commit()
            print("Entity inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def read(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            select_query = """SELECT * from readers WHERE id = %s"""
            item_tuple = (id,)
            cursor.execute(select_query, item_tuple)
            connection.commit()
            print("Result", cursor.fetchall())
            
        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def update(self, id, username, number_of_read, abonement_id):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            update_query = """Update readers 
                              SET username = (%s),
                                  number_of_read = (%s),
                                  abonement_id = (%s)
                                  WHERE id = (%s)"""
            item_tuple = (username, number_of_read, abonement_id, id)
            cursor.execute(update_query, item_tuple)
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            cursor.execute("Delete from readers WHERE id = %s", [id])
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def generate(self, genNum):
        
        if (genNum < 0):
            print('Error with input!')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()

            generate_query = """INSERT INTO readers (username, number_of_read, abonement_id)
                                    SELECT md5(random()::text), (trunc(random() * 100)::int), (trunc(random()*(SELECT id FROM abonements ORDER BY id DESC LIMIT 1))::int)
                                    FROM generate_series(1, %s)"""
            item_tuple = genNum,
            cursor.execute(generate_query, item_tuple)
            connection.commit()
            
            count = cursor.rowcount
            print(count, "Entity generated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

     
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
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            insert_query = """ INSERT INTO abonements (id, price, expiring_time) VALUES (%s, %s, %s)"""
            item_tuple = (id, price, expiring_time)
            cursor.execute(insert_query, item_tuple)
            connection.commit()
            print("Entity inserted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def read(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            select_query = """SELECT * from abonements WHERE id = %s"""
            item_tuple = (id,)
            cursor.execute(select_query, item_tuple)
            connection.commit()
            print("Result", cursor.fetchall())
            
        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def update(self, id, price, expiring_time):
        
        if (id < 1):
            print('Invalid id entered')
            return  
        try:
            connection = psycopg2.connect(user="postgres",
                                        password="1337",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="library_db")
            cursor = connection.cursor()
            update_query = """Update abonements 
                              SET price = (%s),
                                  expiring_time = (%s)
                              WHERE id = (%s)"""
            item_tuple = (price, expiring_time, id)
            cursor.execute(update_query, item_tuple)
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity updated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def delete(self, id):
        
        if (id < 1):
            print('Invalid id entered')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()
            cursor.execute("Delete from abonements WHERE id = %s", [id])
            connection.commit()
            count = cursor.rowcount
            print(count, "Entity deleted")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()

    def generate(self, genNum):
        
        if (genNum < 0):
            print('Error with input!')
            return 
        try:
            connection = psycopg2.connect(user="postgres",
                                            password="1337",
                                            host="127.0.0.1",
                                            port="5432",
                                            database="library_db")
            cursor = connection.cursor()

            generate_query = """INSERT INTO abonements (price, expiring_time)
                                    SELECT (trunc(random()*1000)::int), (trunc(random()*300)::int)
                                    FROM generate_series(1, %s)"""
            item_tuple = genNum,
            cursor.execute(generate_query, item_tuple)
            connection.commit()
            
            count = cursor.rowcount
            print(count, "Entity generated")

        except (Exception, Error) as error:
            print("Error occured in PostgreSQL: ", error)
        finally:
            if connection:
                cursor.close()
                connection.close()