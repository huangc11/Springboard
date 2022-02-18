
CREATE TABLE  sequence
(
   id INT NOT NULL AUTO_INCREMENT,
   note VARCHAR(30),
   PRIMARY KEY (id)
);

insert into sequence (note) values ('');
commit;


CREATE TABLE  bank(
   bank_id INT NOT NULL AUTO_INCREMENT,
   bank_no VARCHAR(10) NOT NULL ,
   name  VARCHAR(60) NOT NULL,
  PRIMARY KEY (bank_id)
)
;

CREATE TABLE  customer(
   id INT NOT NULL AUTO_INCREMENT,
   name  VARCHAR(60) NOT NULL,
   address VARCHAR(100),
  PRIMARY KEY (id)
)
;


CREATE TABLE  bankaccount(
   account_id INT NOT NULL AUTO_INCREMENT,
   account_no INT NOT NULL UNIQUE ,
   account_type VARCHAR(20),
   balance INT,
   intrst_rate FLOAT,
   cr_date  DATE,
   PRIMARY KEY (account_id)
   )
;

CREATE TABLE  acc_transaction(
   id INT NOT NULL AUTO_INCREMENT,
   account_no INT NOT NULL ,
   type VARCHAR(20),
   amount FLOAT,
   cr_date  DATE,
   PRIMARY KEY (id)
   )

-- Bridge table for customer and account
CREATE TABLE  ass_customer_account
(
   id INT NOT NULL AUTO_INCREMENT,
   customer_id INT NOT NULL,
   account_id INT NOT NULL ,
   cr_datetime  DATETIME,
   PRIMARY KEY (id)
)
;

CREATE TABLE  employee
(
   id INT NOT NULL AUTO_INCREMENT,
   name VARCHAR(60),
   login VARCHAR(20),
   password VARCHAR(20),
   PRIMARY KEY (id)
)
;

create table creditcard
(
   id INT NOT NULL AUTO_INCREMENT,
   card_no INT,
   bank_name VARCHAR(60),
   exp_date DATE,
   customer_id INT,
   cardholder_name VARCHAR(60),
   credit_limit DECIMAL,
   employee_id INT,
   cr_date  DATE,
   PRIMARY KEY (id)
)
;

create table card_application
(
   id INT NOT NULL AUTO_INCREMENT,
   customer_id INT,
   status VARCHAR(30),
   note VARCHAR(100),
   apply_date DATE,
   apprv_date  DATE,
   employee_id INT,
   card_id  INT,
   PRIMARY KEY (id)
)
;