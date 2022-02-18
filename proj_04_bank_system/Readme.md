# Bank System Case Study


This project is to implment a simpler version of a banking system, tthrough which we can create customers, bank accounts, preform account transactions(deposit and withdrawal), process credit card applicaitons and create credit cards.


### Business requirements

Here are the system functionalities that the system is targetting. More details of the functions and their business logic can be found here: 
[Bank system function specification](https://github.com/huangc11/Springboard/blob/main/proj_04_bank_system/doc/OOP%20Project%20banking%20system%20function%20specification.pdf)

#### Module and funciton  

#### 1.Customer management: 
- Create or retrieve a customer 
#### 2.Account management: 
- Create checking or savings accounts
- Assign an account to a customer (for sharing account)
#### 3.Transaction management: 
- Perform deposits to or withdrawals from bank accounts.
####  4.Credit card manangment:
- Approval credit card applicaiton
- Generte new credit cards
####  5. Others
- Create a employee


### Design 

#### 1. Data storage

   We will use relational database to store the data. 

#### 2. Classes

   The system will be implemented using  Object Oriented Methodology.  We will have 3 layers of classes:

1. Bottom level class that talks to database
-  We have a bottom level class class,  Database, which directly talks to  MySQL database. 
-  It stores our configuration parameters of the database,  provides methods of initializing database connections, providing db sessions,  and saving a new/updated record or to the database. 
-  All other classes can import this class and call its methods to talk to database


2. Classes of  baisc entities
- These are classes which represent the very basic entities, such as customer,  account, employee, credit card, account transactions and etc. They provide methods of creation, saving to or retrieving  from database for individual entity of a single type.  
- In most cases it won't talk to database directly but sometimes will do, when certain specific functions are not provided by class 'Database'.


3.  Classes of  application modules
- These are highest  level classes that interact with different bottom level entities and provides systems functions the user need.  
- They provide interfaces for human-computer interaction so system can accept commands and input from the users, and send feedback/users to the users.
- They will call methods provided by classes of the other two types, and never talk directly to database


More details can be found: [Bank System Class specification](https://github.com/huangc11/Springboard/blob/main/proj_04_bank_system/doc/OOP%20Project%20bank%20system%20class%20specification.pdf)



### Implementation
	
- Database: MySQL
	
- Programming language: python 3
	
- SQLAlchemy ORM is used for applicaiton interacting with database


## Installation and set up


### Prerequisites

1. python 3 needs to installed  

2. Mysql server needs to be installed and a database credtional need to be set up. 

3. python pytest module is installed (if you want do test). Otherwise, run the following command to install it:
pip install pytest


### Installing and set up


#### Step 1. Download source code
Create a work folder on your compulter and 

1. download all the files in following github folder to it
https://github.com/huangc11/Springboard/tree/main/proj_04_bank_system

2. Make sure subfolder 'logs' is created under the root folder


#### Step 2. Install MySQL Python connector using pip
Run following  pip command to install the pyhton module:
- pip install pymysql
- pip install sqlalchemy
- pip install pytest


#### Step 3: Create database and table in mysql database

 1) At dos prompt,  launch mysql shell to connect to mysql server using following command (assume you have set up a credential,  'dbuser1' as username and a password):
 
     mysql -u dbuser1 -p
 
 2) After  connecting to mysql serer, run follow command to create database if it is not existing:
 
     create database bank;
  
 3) Type command 'use bank' to connect to the database.
 
 4) Locate file 'sql_creat_table.sql' in the root folder.  In msql shell, run all the DDL statements in this file to create all needed tables.
 
 5) Please make sure table 'sequence' is not empty.  If it is empy, run following SQL statement to insert one record:
     insert into sequence (note) values ('');
     commit;
 
#### Step 4: Config database connection for applications
1) At python script root folder, locate file 'database.py' and modify it by setting the following 4 variables to the proper values:
-  __db_username = 'xxx'
-  __db_password = 'xxx'
-  __db_host ='localhost'
-  __db_port = '3306'
  
2) (Optional) You can  test database connection using pytest(If you have pytest module installed). Navigate to the root folder, launch Windows 10 Dos prompt and run the following command:
pytest  test_database.py -v


#### Step 5: Run python script

At Windows 10 Dos prompt, navigate to the work folder created at step 1, typing one following command to launch the relevant application module:
python  manager_account.py
python manager_creditcard.py
python  manager_customer.py
python manager_transaction.py

 
 
## Test

#### Unit test

- We provide baunch of scripts to do unit testing.  The testing scripts will be placed in the root folder.

- Note:  Python pytest module needs to be installed. 

- Run test (on Windows) :

1) At  dos prompt, navigate to the root folder

2) To run a single test script, run one the following commands at doc prompt ( assume test_xxxx is the file name)
  pytest test_xxxx.py 
  pytest test_xxxx.py -v
  
  
3) To run all test scripts, run one the following command at dos prompt
  pytest   
  pytest -v
 
 
Please note that, a lot of test scripts require that sample data has been set up in database.  So you may need to set up sample data or change the test scripts before the run.


#### Integration test

We have designed test cases, set up sample data, and performed integration tests.  Please find the information in the reference section. 

## Authors

* **Chun Huang** - *Initial work* 

## References

Bank system integration test case and sample data set up: [here](https://github.com/huangc11/Springboard/blob/main/proj_04_bank_system/doc/OOP%20Project%20integration%20test%20case%20and%20sample%20data.pdf)

Bank system integration test case running results: [here](https://github.com/huangc11/Springboard/blob/main/proj_04_bank_system/doc/OOP%20project%20integration%20test%20case%20running%20result.pdf)

