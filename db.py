from typing import Dict
from helper import checkStr
'''
Your API should accept the following:

-void createTransaction(String transactionId)
    *Starts a transaction with the specified ID. The ID must not be an active
    transaction ID.
    *Throws an exception or returns an error on failure
-void rollbackTransaction(String transactionId)
    *Aborts the transaction and invalidates the transaction with the specified
    transaction ID
    *Throws an exception or returns an error on failure
-void commitTransaction(String transactionId)
    *Commits the transaction and invalidates the ID. If there is a conflict (meaning the
    transaction attempts to change a value for a key that was mutated after the
    transaction was created), the transaction always fails with an exception or an
    error is returned.
    Transaction should be isolated at the read committed level, as defined by this Wikipedia page.
    Any put, delete, get operation that is issued without a transaction ID should commit immediately.
'''

class SimpleDB(object):
    def __init__(self, preset_data: Dict[str, str]=None):
        # In memory JSON Object
        if preset_data != None:
            self.db = preset_data
        else:
            self.db = {}

    # Assist in testing
    def getDB(self):
        return self.db
    
    

    '''
    -void put(String key, String value)
        *Set the variable “key” to the provided “value”
        *Throws an exception or returns an error on failure
    -void put(String key, String value, String transactionId)
        *Set the variable “key” to the provided “value” within the transaction with ID
        “transactionId”
        *Throws an exception or returns an error on failure
    '''
    def put(self, key: str, value: str, transactionId: str = None):
        if not checkStr(key, value):
            raise TypeError
        try:
            self.db[str(key)] = value
        except Exception as error:
            raise error

    '''
    -String get(String key)
        *Returns the value associated with “key”
        *Throws an exception or returns an error on failure
    -String get(String key, String transactionId)
        *Returns the value associated with “key” within the transaction with ID
        “transactionId”
        *Throws an exception or returns an error on failure
    '''
    def get(self, key: str, transactionId: str = None):
        if not checkStr(key):
            raise TypeError
        try:
            return self.db[key]
        except Exception as error:
            raise error

    '''
    -void delete(String key)
        *Remove the value associated with “key”
        *Throws an exception or returns an error on failure
    -void delete(String key, String transactionId)
        *Remove the value associated with “key” within the transaction with ID
        “transactionId”
        *Throws an exception or returns an error on failure
    '''
    def delete(self, key: str, transactionId: str = None):
        if not checkStr(key):
            raise TypeError
        if not key in self.db:
            raise Exception("Error, key not in db")
        try:
            del self.db[key]
        except Exception as error:
            raise error

    '''
    Your API should accept the following:

    -void createTransaction(String transactionId)
        *Starts a transaction with the specified ID. The ID must not be an active
        transaction ID.
        *Throws an exception or returns an error on failure
    -void rollbackTransaction(String transactionId)
        *Aborts the transaction and invalidates the transaction with the specified
        transaction ID
        *Throws an exception or returns an error on failure
    -void commitTransaction(String transactionId)
        *Commits the transaction and invalidates the ID. If there is a conflict (meaning the
        transaction attempts to change a value for a key that was mutated after the
        transaction was created), the transaction always fails with an exception or an
        error is returned.
        Transaction should be isolated at the read committed level, as defined by this Wikipedia page.
        Any put, delete, get operation that is issued without a transaction ID should commit immediately.
    '''