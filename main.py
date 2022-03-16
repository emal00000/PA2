import datetime
import mysql.connector
import csv
#####       Connecting to mysql using mysql.connector     #####
cnx = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="root",
  buffered = True
)
#####       The database that will be checked if exists     #####
DB_NAME = "replacement_parts_store"

my_cursor = cnx.cursor()  # The my_cursor that we will use to execute data from mysql

def create_database(my_cursor, DB_NAME):  # To create database if not exist
  my_cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))# Listing the data using the command SHOW DATABASES with my_cursor
  print("Creating the database...")

def create_customerTable(my_cursor):# Here we create a table for the file  with all of its attributes. we use if not exist to make sure no dublicats occurs. AUTO_INCREMENT so it will increase automatic
    my_cursor.execute("CREATE TABLE IF NOT EXISTS customer ("\
                    "  customerID INT(4) NOT NULL AUTO_INCREMENT," \
                    "  name varchar(250) NOT NULL," \
                    "  address varchar(250) NOT NULL," \
                    "  postalzip nvarchar(20) NOT NULL," \
                    "  region varchar(50)," \
                    "  PRIMARY KEY (customerID))")
    print("Creating the Customer table...")

def insert_customer(my_cursor): # we insert the values of the file
  with open('customer.csv', 'r') as file: # Here we open the file and just read it
      reader_customer = csv.DictReader(file, delimiter=',')    # Here we want to delimit every column with a comma
      for line in reader_customer: # we read each line from the file and inserting the data, the {} used as place holders
        my_cursor.execute("INSERT INTO customer (name,address,postalzip,region) " \
                          "VALUES('{}', '{}', '{}', '{}')"\
                          .format(
                          line['name'], 
                          line['address'], 
                          line['postalzip'], 
                          line['region']))
  cnx.commit() # we commit our data, "save it" in the database
  print("The customer data has been inserted") #Confirm the has been done
  


def create_categoryTable(my_cursor): 
  my_cursor.execute("CREATE TABLE IF NOT EXISTS category ("\
                    "  CategoryID int(20) NOT NULL," \
                    "  CategoryName varchar(250) NOT NULL," \
                    "  PRIMARY KEY (CategoryID))")
  print("Creating the category table...") #Confirm the has been done
  

def insert_category(my_cursor): # function to insert data into category table
  with open('category.csv', 'r') as file: # Here we open the file and just read it
      reader_customer = csv.DictReader(file, delimiter=',')    # Here we want to delimit every column with a comma
      for line in reader_customer: # we read each line from the file
        my_cursor.execute("INSERT INTO category (CategoryID,CategoryName) " \
                          "VALUES('{}', '{}')"\
                          .format(
                          line['CategoryID'], 
                          line['CategoryName']))
  cnx.commit() # we commit our data, "save it" in the database
  print("The categories has been inserterd")



def create_productTable(my_cursor): # Have already commented above :)
  my_cursor.execute("CREATE TABLE IF NOT EXISTS products ("\
                    "  ProductID INT(4) NOT NULL AUTO_INCREMENT," \
                    "  ProductName NVARCHAR(1000) NOT NULL," \
                    "  CategoryID NVARCHAR(50) NOT NULL,"\
                    "  Quantity INT(100) NOT NULL,"\
                    "  Price INT(50),"\
                    "  PRIMARY KEY (ProductID))")
  print("Creating the products table...")


def insert_product(my_cursor): # Function that will insert the products values
  with open('products.csv', 'r') as file: # Here we open the file and just read it
      reader_products = csv.DictReader(file, delimiter=',')    # Here we want to delimit every column with a comma
      for line in reader_products: # we read each line from the file
        my_cursor.execute("INSERT INTO products (ProductName,CategoryID,Quantity,Price) " \
                          "VALUES('{}', '{}', '{}', '{}')"\
                          .format(
                          line['ProductName'], 
                          line['CategoryID'], 
                          line['Quantity'],
                          line['Price']))
        # we are using the "insert into planets and (atribute1,atribute2, .....) Values("%s")" to insert the values into our table.
  cnx.commit() # we commit our data, "save it" in the database
  print("The products data has been inserted")


def create_orderTable(my_cursor): # order table creating function
  my_cursor.execute("CREATE TABLE IF NOT EXISTS orders ("\
                    "  OrderID INT(4) NOT NULL AUTO_INCREMENT," \
                    "  CustomerID NVARCHAR(4),"\
                    "  OrderDate NVARCHAR(25),"\
                    "  PRIMARY KEY (OrderID))")
  print("Creating the orders table...")


def insert_order(my_cursor):
  with open('order.csv', 'r') as file: # Here we open the file and just read it
      reader_order = csv.DictReader(file, delimiter=',')    # Here we want to delimit every column with a comma
      for line in reader_order: # we read each line from the file
        my_cursor.execute("INSERT INTO orders (CustomerID,OrderDate) " \
                          "VALUES('{}', '{}')"\
                          .format(
                          line['CustomerID'], 
                          line['OrderDate']))
  cnx.commit() # we commit our data, "save it" in the database
  print("The orderdata data has been inserted")



def check_tables_exist(my_cursor): # The function that will check if every table exist in database
  my_cursor.execute("SHOW TABLES LIKE 'customer'") # We use this query "SHOW TABLES LIKE" to look if table exist
  if my_cursor.fetchone() == None: # if its None we create by using the function we made to create table
    create_customerTable(my_cursor)

  my_cursor.execute("SHOW TABLES LIKE 'category'")
  if my_cursor.fetchone() == None:
    create_categoryTable(my_cursor)

  my_cursor.execute("SHOW TABLES LIKE 'products'")
  if my_cursor.fetchone() == None:
    create_productTable(my_cursor)

  my_cursor.execute("SHOW TABLES LIKE 'orders'")
  if my_cursor.fetchone() == None:
    create_orderTable(my_cursor)


def list_category(my_cursor): # This function used to list all the categories/brand that we have in the store
  my_cursor.execute("SELECT * FROM replacement_parts_store.category") # we are selecting all attributes from the category
  print("|{:<5} |{:<15} |".format("CategoryID","CategoryName"))  # som printing to make it look nice
  print("+{:<10} +{:<15} +".format(10*"-",15*"-"))                
  for (CategoryID,CategoryName) in my_cursor: # We list all the values and print them out
    print("|{:<10} |{:<15} |".format(CategoryID,CategoryName))



def list_product(select_category,my_cursor): # This function used to list all products from the selected category from the user
  my_cursor.execute("DROP VIEW IF EXISTS {}_replacement_parts".format(select_category)) # We drop the view if it exist because I couldnt use if not exist
  # the next query a littel bit long becuase sorry, I get error if I divide it to several rows,  we are selecting the diiferent attributes that I want in the VIEW and i wanted to make a join of category and prodcts to list all the producs that have the selected category 
  my_cursor.execute("CREATE VIEW {}_replacement_parts AS SELECT products.ProductID, category.CategoryName, products.ProductName, products.Quantity, products.Price FROM products INNER JOIN category ON category.CategoryID = products.CategoryID WHERE CategoryName = '{}'".format(select_category,select_category))
  my_cursor.execute("SELECT * FROM {}_replacement_parts".format(select_category)) #
  print("|{:<5} |{:<15} |{:<75} |{:<30}| {:<10}|".format("ProductID","CategoryName","ProductName","Quantity","Price"))  
  print("+{:<9} +{:<15} +{:<75} +{:<30}+ {:<10}+".format(5*"-",15*"-",75*"-",30*"-",10*"-"))                
  for (ProductID,CategoryName,ProductName,Quantity,Price) in my_cursor:
      print("|{:<9} |{:<15} |{:<75} |{:<30}| {:<10}|".format(ProductID,CategoryName,ProductName,Quantity,Price))



def select_update_product(my_cursor,chose_product,chose_quantity): # this funtion is to update the quantity of the parts that has been orderd
  my_cursor.execute("UPDATE products\n" 
                    "SET products.Quantity = products.Quantity - {}\n"
                    "WHERE products.ProductID = {}".format(chose_quantity,chose_product))# the first {} used to hold the quantit that are wanted, second to hold the product id that will be updated
  cnx.commit() # We commit to save the value



def add_new_customer(cust_name,my_cursor): # This function is used to add new customer that are not registred
  my_cursor.execute("SELECT * FROM customer WHERE name = '{}'".format(cust_name)) # We look if the customer exist by using this query in the customer table
  if my_cursor.fetchone() == None: # if the value returned is none we ask about the information and then add them 
    cust_addr = input("Enter your address ")
    cust_post = input("Enter your postalzip ")
    cust_region = input("Enter you region ")
    my_cursor.execute("INSERT INTO customer (name,address,postalzip,region)\n" 
                      "VALUES('{}','{}','{}','{}')".format(cust_name,cust_addr,cust_post,cust_region))# we insert all the values into the customer table and savit by commit
    cnx.commit()


def add_order(cust_name,my_cursor): # add order for the customer
  my_cursor.execute("SELECT customerID FROM customer WHERE customer.name = '{}'".format(cust_name)) # we want to use the Customer Id in the orders information 
  cust_id = str(my_cursor.fetchone()).replace("(","").replace(")","").replace(",","") # We replace all unawanted signs so it look nice and dont make probelm 
  time = datetime.datetime.now() # using the time to add it in the orders
  form_time = time.strftime("%Y%m%d") # strftime used to format the date
  my_cursor.execute("INSERT INTO replacement_parts_store.orders (CustomerID,OrderDate)\n"
                    "VALUES ('{}', {})".format(cust_id,form_time)) # we insert the values into the orders table
  cnx.commit() # save the updating
  print("Order has been added, Welcome to our store Again")

  

def expensive_product(my_cursor): #functoin to list the expensive product in the store
  my_cursor.execute("SELECT ProductName,Price FROM products WHERE Price = (SELECT MAX(Price) FROM products)") # this quere are used to select the ProductName,Price from the product table with the "limit" just max price 
  print("|{:<60} |{:<20} |".format("ProductName","Price"))  # making it look nicer
  print("+{:<60} +{:<20} +".format(60*"-",20*"-"))                
  for (ProductName,Price) in my_cursor: # list it and print it out
      print("|{:<60} |{:<20} |".format(ProductName,Price))
  print(85*"-")



def products_to_be_soldout(my_cursor): # The function will list the products that will be sold out
  my_cursor.execute("SELECT ProductName,Price, Quantity FROM products WHERE Quantity < 20")
  print("|{:<60} |{:<20} |{:<20}|".format("ProductName","Price","Quantity"))  
  print("+{:<60} +{:<20} +{:<20} +".format(60*"-",20*"-",20*"-"))                
  for (ProductName,Price,Quantity) in my_cursor:
      print("|{:<60} |{:<20} |{:<20} |".format(ProductName,Price,Quantity))
  print(100*"-")


def main_menu(): #Main menu function 
  while True:
      print(30*"-")# Make the menu nice
      print("1. Make an order")
      print("2. Show most expensiv product")
      print("3. Show product to be sold out")
      print("Q. Quit")
      print(30*"-")
      selected_menu = input("Select a menu : ") # let the customer choose the menu
      print("\n")
      if selected_menu == "1":
        list_category(my_cursor) #we the list category to list all categoris
        select_category = input("Select the category ") # let the user select the category 
        list_product(select_category,my_cursor) # list all products from the category chosen before
        chose_product = input("\nEnter the productID of the product ")# Chose the product id from the table
        chose_quantity = int(input("Enter the quantity "))# choose how many of the products
        select_update_product(my_cursor,chose_product,chose_quantity)# update the product quantity values
        cust_name = input("Enter your full name ")
        add_new_customer(cust_name,my_cursor) # this till be if used if the customer name not exist
        add_order(cust_name,my_cursor) # this will add the order into the database
        input("Enter any key to go to main menu...") # to go to main menu
      elif selected_menu == "2":
        expensive_product(my_cursor) #we use the function above to show to expensive product
        input("Enter any key to go to main menu...")
      elif selected_menu == "3": 
        products_to_be_soldout(my_cursor) # to show the products that are less then 20
        input("Enter any key to go to main menu...")
      elif selected_menu == "Q":
        quit()  #it will close the program
      else:
        print("invaild input, try again...")


db_found = False
my_cursor.execute("SHOW DATABASES") 
for database in my_cursor: # checking all the databases
  if database == (DB_NAME,):# looking if the database exist
    db_found = True # We turn the boolean value to true so we dont continue, otherwise it will go the if statement 
    my_cursor.execute("USE {}".format(DB_NAME)) # we using the syntac "use" to be able to make changes in the database. The %s is lika a place holde,then we define the what it will include
    check_tables_exist(my_cursor) # function we made  above
    print("You are using the database")
    main_menu() #the menu that will always appear with the options     
   

if db_found == False: # if database does not exist, we create the database with tables showing data according to our files.
  create_database(my_cursor,DB_NAME) #We use the function to create the database
  my_cursor.execute("USE {}".format(DB_NAME)) # we use this query to be in the database
  create_customerTable(my_cursor)
  insert_customer(my_cursor)
  create_categoryTable(my_cursor)
  insert_category(my_cursor)
  create_productTable(my_cursor)
  insert_product(my_cursor)
  create_orderTable(my_cursor)
  insert_order(my_cursor)
  main_menu()#the menu that will always appear with the options     
