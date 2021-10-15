class view:

    def __init__(self):  
        print('======================')
        print('== Starting program ==')
        print('======================')

    def readCommand():
        com = input("Enter command (create, delete, update, random): ")
        return com

    def readTable():        
        tab = input("Enter table name: ")
        if (tab == 'authors' or tab == 'books' or tab == 'abonements' or tab == 'readers'):
            return tab
        return ''
    
    def getVal():
        val = input("Enter value: ")
        return val

    def getInt():
        val = input("Enter integer value: ")
        try:
            int(val)
        except ValueError:
            print(val + " is invalid")
            return 0
        return int(val)