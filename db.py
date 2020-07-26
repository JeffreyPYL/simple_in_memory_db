import uuid
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
        self.transaction = {}
        self.db_transaction_id = {}

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
        if not checkStr(key, value) : raise TypeError
        
        # Transaction Operation on PUT
        if transactionId != None:
            if not checkStr(transactionId) : raise TypeError
            if transactionId not in self.transaction : raise Exception("Error, transactionId not in db")

            try:
                # Set operation within specific transaction
                self.transaction[transactionId]['value'] = {key: value}

                # Get Exisiting uuid on key that is related to transaction 
                # Note: python string are immutable
                if key in self.db:
                    self.transaction[transactionId]['transaction_uuid'] = {key: self.db_transaction_id[key]}
                else:
                    self.transaction[transactionId]['transaction_uuid'] = {key: None}

            except Exception as error:
                raise error
        # Immediate Commit Operation on PUT
        else:
            try:
                self.db[key] = value
                self.db_transaction_id[key] = uuid.uuid4()
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
        if not checkStr(key) : raise TypeError
        
        if transactionId != None:

            if transactionId not in self.transaction : raise Exception("Error, transactionId not in db")

            try:
                return self.transaction[transactionId]['value'][key]
            except Exception as error:
                raise error
        else:
            try:
                # Since dirty read is not allowed and get needed to be commit immediately without transaction id
                # Change transaction unique id to indicate transaction happen to prevent commit of old traction
                # on to the same key
                self.db_transaction_id[key] = uuid.uuid4()
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
        if not checkStr(key) : raise TypeError

        if transactionId != None:

            if transactionId not in self.transaction : raise Exception("Error, transactionId not in db")
            current_val = self.transaction[transactionId]['value'][key]
            current_val_uuid = self.transaction[transactionId]['transaction_uuid'][key]
            try:
                del self.transaction[transactionId]['value'][key]
                del self.transaction[transactionId]['transaction_uuid'][key]
            except Exception as error:
                # Revert change if somehow the operation fail
                self.transaction[transactionId]['value'][key] = current_val
                self.transaction[transactionId]['transaction_uuid'][key] = current_val_uuid
                raise error

        else:
            current_val = self.db[key]
            current_transaction_uuid = self.db_transaction_id[key]
            if not key in self.db:
                raise Exception("Error, key not in db")
            try:
                del self.db[key]
                del self.db_transaction_id[key]
            except Exception as error:
                # Revert change if somehow the operation fail
                self.db[key] = current_val
                self.db_transaction_id[key] = current_transaction_uuid
                raise error

    '''
    -void createTransaction(String transactionId)
        *Starts a transaction with the specified ID. The ID must not be an active
        transaction ID.
        *Throws an exception or returns an error on failure
    '''
    def createTransaction(self, transactionId: str):
        if not checkStr(transactionId):
            raise TypeError
        if transactionId not in self.transaction:
            self.transaction[transactionId] = {}
        else:
            raise Exception("Error, Transaction Key already exists")

    '''
    -void rollbackTransaction(String transactionId)
        *Aborts the transaction and invalidates the transaction with the specified
        transaction ID
        *Throws an exception or returns an error on failure
    '''
    def rollbackTransaction(self, transactionId: str):
        if not checkStr(transactionId):
            raise TypeError
        if transactionId in self.transaction:
            del self.transaction[transactionId]
        else:
            raise Exception("Error, Transaction Key dp not exists")

    '''
    -void commitTransaction(String transactionId)
        *Commits the transaction and invalidates the ID. If there is a conflict (meaning the
        transaction attempts to change a value for a key that was mutated after the
        transaction was created), the transaction always fails with an exception or an
        error is returned.
        Transaction should be isolated at the read committed level, as defined by this Wikipedia page.
        Any put, delete, get operation that is issued without a transaction ID should commit immediately.
    '''
    def commitTransaction(self, transactionId: str):
        current_transaction = self.transaction[transactionId]
        update = True
        # Get keys at transaction to check if keys at db have been modify
        # should return json of { key: uuid, key_1: uuid_1, etc} 
        transaction_keys = self.transaction[transactionId]['transaction_uuid']
        for key in transaction_keys:
            # Check if key still exists
            if key not in self.transaction:
                update = False
                break
            # Check if key value have been read or modify between Transaction Create and Commit
            if self.transaction[key] != transaction_keys[key]:
                update = False
                break
             
        if update:
            try:
                self.db.update(self.transaction[transactionId]['value'])
                del self.transaction[transactionId]
            except Exception as error:
                # restore transaction incase transaction fail
                self.transaction[transactionId] = current_transaction
                raise error
        else:
            raise Exception('Transaction Commit Fail')
        
    '''
    Transaction should be isolated at the read committed level, as defined by this Wikipedia page.
    Any put, delete, get operation that is issued without a transaction ID should commit immediately
    '''