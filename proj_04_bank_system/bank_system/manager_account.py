import datetime as dt
from utility import Utility as ut
from database import Database

from customer import Customer
from bankaccount import BankAccount, SavingsAccount, CheckingAccount
from customer_account import CustomerAccount



class AccountManager:
    logger = ut.setup_logger('log_account', 'logs\log_account_manager.log')

    @classmethod
    def log_show_info(cls, message):
        cls.logger.info(message)
        ut.print_info(message)

    @classmethod
    def log_show_error(cls, message):
        cls.logger.info(ut.msg_fmt_error(message))
        ut.print_error(message)

    @classmethod
    def log_show_success(cls, message):
        cls.logger.info(ut.msg_fmt_success(message))
        ut.print_success(message)

    @classmethod
    def create_account(cls, p_customer_id, p_account_type, p_balance=0, p_intrs_rate=0):
        """create account
          Args:
            p_customer_id(int): customer id
            p_balance(float): balance of the account
            p_account_type(str):   account type, ['c' (for checking), 's' (for savings)]

          Returns:
                  Account id, if success
                  None, if failure
        """

        #    if p_account_type = 's':       if intrs_rate=None:i

        cls.log_show_info("Requested to create account:  customer(id={}), account_type='{}', balance={}, intr_rate={}"
                          .format(p_customer_id,
                                  'checking' if p_account_type=='c' else 'savings',
                                  p_balance,
                                  p_intrs_rate)
        )

        #check if customer existing
        o_customer = Customer.get_by_id(p_customer_id)
        if o_customer ==None:
            msg='account creation failed -- customer with id={} not found.'.format(p_customer_id)
            cls.log_show_error(msg)
            #ut.print_error('Account creation failed -- customer with id={} not found'.format(p_customer_id))
            return None


       # create an account object
        if p_account_type =='s':
            new_account = SavingsAccount.create_account(p_balance,  p_intrs_rate)
        elif  p_account_type =='c':
            new_account= CheckingAccount.create_account(p_balance)
        else:
            msg='account creation failed, account type "{} unknown"'.format(p_account_type)
            cls.log_show_error(msg)
            return None


        if new_account==None:
            msg='Account creation failed -- due to database failure'
            cls.log_show_error(msg)
            return None

        # assign the acocunt to the customer, i.e. create a customer-account associatioon
       # o_cust_acc= CustomerAccount()
        new_cust_acc = CustomerAccount.create_association(o_customer.id,  new_account.account_id)

        if new_cust_acc == None: #if fail
            cls.log_show_error('Account creation failed -- customer-account assignment failed')
            return None


        cls.log_show_success('Account (id ={}) successfully created and assgined to customer (id={}).'
                    .format(new_account.account_id, o_customer.id))
        cls.log_show_info('\t {}'.format(new_account))
        cls.log_show_info('\t {}'.format(o_customer))
        return new_account

    @classmethod
    def assign_customer_account(cls,customer_id, account_id):

            if  Customer.get_by_id(customer_id)==None:
                cls.log_show_error("Customer-account connection: ({}, {}) failed-- customer_id not found".
                            format(customer_id, account_id))
                return None

            if  BankAccount.get_by_account_id(account_id)==None:
                cls.log_show_error("Customer-account connection: ({}, {}) failed-- account_id not found".
                            format(customer_id, account_id))
                return None

            record = CustomerAccount(customer_id, account_id).insert_to_db()
            print(record)

            # db creaton succeeds
            if record!=None:
                cls.log_show_success("Customer-account connection: ({}, {}) created.".format(customer_id,account_id))
                #ut.print_success("Customer-account connection: ({}, {}) created.".format(customer_id, account_id))
            else:
                cls.log_show_error("Customer-account connection: ({}, {}) failed.".format(customer_id, account_id))


    def disp_account_by_customer_id(p_id):
        try:
            session = Database.get_session()
            recs = session.query(BankAccount).join(CustomerAccount,BankAccount.account_id == CustomerAccount.account_id)\
                .filter(CustomerAccount.customer_id == p_id).all()

            print(len(recs))
            print(recs)
            for rec in recs:
                print(rec)

        except Exception as e:
            print(e)

    @staticmethod
    def menu():
        user_input = input("************ Account Manager **********\n"
                       "'s'-- create a savings account or 'c'-- create a checking account or  'a-- assign an account to a customer\n"
                       "'q' to quit. Enter your selection:  ")
        while user_input != 'q':
            if user_input == 's':
                try:
                    customer_id= int(input("Please enter customer id: "))
                    balance= float(input("Please enter account balance: "))
                    intrst_rate= float(input("Please enter interest_rate(default=0.01):") or 0.01)

                    try:
                        AccountManager. create_account(customer_id, 's', balance, intrst_rate)
                    except Exception as e:
                        ut.print_error(e)
                        ut.log_exeption(e)
                except:
                    AccountManager.log_show_error('Input error. Please input again')
            elif user_input == 'c':
                    try:
                        customer_id = int(input("Please enter customer id: "))
                        balance = float(input("Please enter account balance: "))

                        try:
                            AccountManager.create_account(customer_id, 'c', balance)
                        except Exception as e:
                            ut.print_error(e)

                    except:
                        AccountManager.log_show_error('Input error. Please input again')


            else:
                ut.print_error(("Invalid option. Please choose again."))

            user_input = input("************ Account Manager **********\n"
                               "'s'-- create a savings account or 'c'-- create a checking account or  'a-- assign an account to a customer\n"
                               "'q' to quit. Enter your selection:  ")

if __name__ =='__main__':
    AccountManager.menu()

