from model import authors
from model import books
from model import readers
from model import abonements
from view import view

v = view()
au = authors()
b = books()
r = readers()
ab = abonements()

command = view.readCommand()

if (command == 'create'):

    table = view.readTable()

    if (table == 'authors'):
        id = view.getInt()
        fullname = view.getVal()
        country = view.getVal()
        au.create(id, fullname, country)
    elif (table == 'books'):
        id = view.getInt()
        title = view.getVal()
        rating = view.getInt()
        author_id = view.getInt()
        abonement_id = view.getInt()
        b.create(id, title, rating, author_id, abonement_id)
    elif (table == 'readers'):
        id = view.getInt()
        username = view.getVal()
        number_of_read = view.getInt()
        abonement_id = view.getInt()
        r.create(id, username, number_of_read, abonement_id)
    elif (table == 'abonements'):
        id = view.getInt()
        price = view.getInt()
        expiring_time = view.getInt()
        ab.create(id, price, expiring_time)
    else:
        print('Table with that name does not exist')

elif (command == 'update'):

    table = view.readTable()

    if (table == 'authors'):
        id = view.getInt()
        name = view.getVal()
        surname = view.getVal()
        au.update(id, name, surname)
    elif (table == 'books'):
        id = view.getInt()
        title = view.getVal()
        rating = view.getInt()
        author_id = view.getInt()
        abonement_id = view.getInt()
        b.update(id, title, rating, author_id, abonement_id)
    elif (table == 'readers'):
        id = view.getInt()
        surname = view.getVal()
        number_of_read = view.getInt()
        abonement_id = view.getInt()
        r.update(id, surname, number_of_read, abonement_id)
    elif (table == 'abonements'):
        id = view.getInt()
        price = view.getInt()
        expiring_time = view.getInt()
        ab.update(id, price, expiring_time)
    else:
        print('Table with that name does not exist')

elif (command == 'delete'):
    
    table = view.readTable()

    if (table == 'authors'):
        id = view.getInt()
        au.delete(id)
    elif (table == 'books'):
        id = view.getInt()
        b.delete(id)
    elif (table == 'readers'):
        id = view.getInt()
        r.delete(id)
    elif (table == 'abonements'):
        id = view.getInt()
        ab.delete(id)
    else:
        print('Table with that name does not exist')

else:
    print('Incorrect command')
