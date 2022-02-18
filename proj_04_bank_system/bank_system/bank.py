import doctest
class Bank:
    """
    A class to represent a bank.

    Attributes
    ----------
    name (str):          name of the bank
    bank_no (str):       9 digit transit number of bank

    Methods
    -------
    get_name(): get bank name
    get_bank_no(): get bank's 9 digit trans number

     #  >>> addnum(23,77)
       100


    """
    __bank_no = "123456789"
    __name = "New Life Banks"



    @staticmethod
    def get_name():
        """ return bank name
        :return: string

            >>> Bank.get_name()
            'New Life Banks'
        """
        return Bank.__name

    @staticmethod
    def get_bank_no():
        return Bank.__bank_no


if __name__ =="__main__" :
   # b_name = Bank.get_name()
    #print(b_name)
    pass

