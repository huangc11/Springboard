
from utility import glb_logger
from utility import Utility as ut


import datetime as dt

from database import Database
from bankaccount import BankAccount
from account_transaction import AccountTransaction

class TransactionManager:
    logger = ut.setup_logger('transaction_mngmt', 'logs\log_transaction_manager.log')

    @classmethod
    def log_show_info(cls, message):
        cls.logger.info(message)
        ut.print_info(message)

    @classmethod
    def log_show_error(cls, message):
        cls.logger.info(message)
        ut.print_error(message)

    @classmethod
    def log_show_success(cls, message):
        cls.logger.info('!!!!!!!! '+ message)
        ut.print_success(message)

    @classmethod
    def log_show_request(cls, message):
        cls.logger.info('******** '+ message)
        ut.print_info('******** '+ message)


    @classmethod
    def withdraw_from_account(cls,p_account_no , p_amount):

        cls.log_show_request("Requested to withdraw from account: account_no={}, amount={}.".format(p_account_no, p_amount))

        o_account = BankAccount.get_by_account_no(p_account_no)

        #if account not found
        if o_account == None:
            cls.log_show_error("Withdrawal transaction failed -- account not found.")

        # Withdraw from the account: get the actual amount withdrawn and new balance
        amount_withdraw = o_account.withdraw(p_amount)
        #Create the transasction record

        new_trans =AccountTransaction.create_trans(p_account_no, 'w', amount_withdraw[0] )

        #if new transsation  in database failed
        if new_trans == None:
            cls.log_show_info('Withdraw transaction failed -- can not create transaction record')
            return None

        #new transsation in database succeed
        cls.log_show_success('Withdraw transaction succeeded')
        if  amount_withdraw[0]<p_amount:
            cls.log_show_info("%%% Warning: Account balance is less than desired amount. Actual amount withdrawn is {}".format(amount_withdraw[0]))

        cls.log_show_info("Transaction id: {};  amount withdrawn: {} account balance: {} ".\
                                 format(new_trans.id,  amount_withdraw[0], amount_withdraw[1]))
        print(new_trans.account_no)


    @classmethod
    def deposit_to_account(cls, p_account_no, p_amount):
        cls.log_show_request("Requested to deposit to account: account_no={}, amount={}."
                             .format(p_account_no, p_amount))

        o_account = BankAccount.get_by_account_no(p_account_no)

        # if account not found
        if o_account == None:
            cls.log_show_error("Deposit transaction failed -- account not found.")
            return None

        deposit_result = o_account.deposit(p_amount)

        new_trans =AccountTransaction.create_trans(p_account_no, 'd', p_amount )
        #p_account_no, p_trans_type, p_amount

        if new_trans == None:
            cls.log_show_error('Deposit transaction failed -- can not create transaction record')
            return None

            # new transsation in database succeed
        cls.log_show_success('Deposit transaction succeeded')
        cls.log_show_info("Transaction id: {}; amount deposited: {}; account balance: {} ". \
                     format(new_trans.id, new_trans.amount, deposit_result[1]))

        return new_trans

    @staticmethod
    def menu():
        user_input = input("************ Transaction Manager **********\n"
                       "'d'-- deposit to account or 'w'-- withdraw account \n"
                       "'q' to quit. Enter your selection:  ")
        while user_input != 'q':
            if user_input == 'd':
                try:
                    account_no = int(input("Please enter account_no: "))
                    amount= float(input("Please enter deposit amount: "))

                    try:
                        TransactionManager.deposit_to_account(account_no, amount)
                    except Exception as e:
                        ut.print_error(e)
                        ut.log_exeption(e)
                except:
                    TransactionManager.log_show_error('Input error. Please input again')
            elif user_input == 'w':
                try:
                    account_no = int(input("Please enter account_no: "))
                    amount = float(input("Please enter withdrawal amount: "))

                    try:
                        TransactionManager.withdraw_from_account(account_no, amount)
                    except Exception as e:
                        ut.print_error(e)
                        ut.log_exeption(e)
                except:
                    TransactionManager.log_show_error('Input error. Please input again')

            else:
                ut.print_error(("Invalid option. Please choose again."))

            user_input = input("************ Transaction Manager **********\n"
                               "'d'-- deposit to account or 'w'-- withdraw account \n"
                               "'q' to quit. Enter your selection:  ")

if __name__ == '__main__':
    ##TransactionManager.deposit_to_account(20701,400)
    #TransactionManager.withdraw_from_account(20701, 1300)
    TransactionManager.menu()

