
from customer import Customer
from utility  import Utility as ut



class CustomerManager:
    logger = ut.setup_logger('log_customer', 'logs\log_customer_manager.log')

    @classmethod
    def create_customer(cls, name, addr):
        # name = input("Please enter the new customer's name: ")
        #addr = input("Please enter the new customer's address: ")

        cls.logger.info('Rquested to create customer: name ={}, address={}'.format(name, addr))
        if Customer.not_exist_by_name_addr(name, addr)==False:
            ut.print_error('Customer already exsited. Can not create a new one')
            cls.logger.info('Customer already exsited. Can not create a new one')
            return None


        customer = Customer(name, addr).insert_to_db()

                # db creaton succeeds
        if customer != None:
                msg = 'customer has been created: id={}, name={}, addr={}'.format(customer.id, customer.name, customer.address)
                ut.print_success(msg)
                cls.logger.info(msg)
                return  customer
        else:
            ut.print_error('Customer creation failed')
            return None

    @staticmethod
    def menu():
        user_input = input("Enter 'a' to add a customer or  Enter 'l' to list customers or  'q' to quit: ")
        while user_input != 'q':
            if user_input == 'a':
                name = input("Please enter the new customer's name: ")
                addr = input("Please enter the new customer's address: ")
                CustomerManager.create_customer(name, addr)

            user_input = input("Enter 'a' to add a customer or  Enter 'l' to list customers or  'q' to quit: ")


if __name__ == '__main__':
    CustomerManager.menu()