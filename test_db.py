import unittest
import uuid
from db import SimpleDB


class TestSimpleDB(unittest.TestCase):
    def setUp(self):
        self.test_db = SimpleDB({
            'test_get_key': 'test_val_1',
        })

    def test_getDB_helper(self):
        expected_db = {
            'test_get_key': 'test_val_1',
        }
        self.assertEqual(self.test_db.getDB(), expected_db)

    def test_get_insert_transaction(self):
        expected_trans = {
            'test_get_transactionID': {
                'value': {
                    'test_get_key': 'test_val'
                },
                'transaction_uuid': {
                    'test_get_key': 'test_uuid'
                }
            }
        }

        self.test_db.insert_transaction(expected_trans)
        self.assertEqual(self.test_db.getTransaction(), expected_trans)

    def test_check_commit_immediately(self):
        commit_immediately, transID = self.test_db.check_commit_immediately(
            None)
        self.assertTrue(commit_immediately)
        self.assertTrue(transID != None)

        commit_immediately, transID = self.test_db.check_commit_immediately(
            'uuid')
        self.assertFalse(commit_immediately)
        self.assertEqual(transID, 'uuid')

    def test_get_DB_no_transac(self):
        expected_val = 'test_val_1'
        receive_val = self.test_db.get('test_get_key')
        self.assertEqual(receive_val, expected_val)

        with self.assertRaises(Exception):
            self.test_db.get('test_bad_key')

        with self.assertRaises(Exception):
            self.test_db.get(123)

    def test_put_DB_no_transac(self):
        expected_db = SimpleDB({
            'test_get_key': 'test_val_1',
            'test_key': 'test_value'
        })
        self.test_db.put('test_key', 'test_value')
        self.assertEqual(self.test_db.getDB(), expected_db.getDB())

        with self.assertRaises(Exception):
            self.test_db.put('test_bad_key')
        # Test bad input
        with self.assertRaises(Exception):
            self.test_db.put('test_bad_key', {'key': 123})
        # Test bad key
        with self.assertRaises(Exception):
            self.test_db.put(123, '123')

    def test_del_DB_no_transac(self):
        expected_db = SimpleDB()
        self.test_db.delete('test_get_key')
        self.assertEqual(self.test_db.getDB(), expected_db.getDB())

        with self.assertRaises(Exception):
            self.test_db.delete('test_bad_key')
        with self.assertRaises(Exception):
            self.test_db.delete(123)

    def test_get_DB_transac(self):
        self.test_db.insert_transaction({
            'test_get_transactionID': {
                'value': {
                    'test_get_key': 'test_val'
                },
                'transaction_uuid': {
                    'test_get_key': uuid.uuid4()
                }
            }
        })
        expected_val = 'test_val'
        receive_val = self.test_db.get(
            'test_get_key', 'test_get_transactionID')
        self.assertEqual(receive_val, expected_val)

        with self.assertRaises(Exception):
            self.test_db.get('test_bad_key', 'bad_transID')
        with self.assertRaises(Exception):
            self.test_db.get('test_bad_key', 123)

    def test_put_DB_transac(self):
        # Test transaction value have been populated

        # adding non exisit value via transaction hence transaction uuid should be None
        expected_val = {
            'test_put_transID': {
                'value': {
                    'test_put_key': 'test_put_val'
                },
                'transaction_uuid': {
                    'test_put_key': None
                }
            }
        }
        trans_ID = 'test_put_transID'
        test_key = 'test_put_key'
        test_val = 'test_put_val'
        self.test_db.createTransaction(trans_ID)
        self.test_db.put(test_key, test_val, trans_ID)
        receive_val = self.test_db.getTransaction()
        self.assertTrue(trans_ID in receive_val)
        self.assertTrue(test_key in receive_val[trans_ID]['transaction_uuid'])
        self.assertTrue(receive_val[trans_ID]
                        ['transaction_uuid'][test_key] == None)
        self.assertEqual(receive_val[trans_ID]['value'],
                         expected_val[trans_ID]['value'])

        # adding exisit value via transaction hence transaction uuid should be None
        expected_val = {
            'test_put_transID_2': {
                'value': {
                    'test_get_key': 'test_put_val'
                },
                'transaction_uuid': {
                    'test_get_key': None
                }
            }
        }
        trans_ID = 'test_put_transID_2'
        test_key = 'test_get_key'
        test_val = 'test_put_val'
        self.test_db.createTransaction(trans_ID)
        self.test_db.put(test_key, test_val, trans_ID)
        receive_val = self.test_db.getTransaction()
        self.assertTrue(trans_ID in receive_val)
        self.assertTrue(test_key in receive_val[trans_ID]['transaction_uuid'])
        self.assertTrue(receive_val[trans_ID]
                        ['transaction_uuid'][test_key] != None)
        self.assertEqual(receive_val[trans_ID]['value'],
                         expected_val[trans_ID]['value'])

        # Raise error on non exist transaction id
        with self.assertRaises(Exception):
            self.test_db.put('test_bad_key', 'test_bad_val', 'bad_transID')
        with self.assertRaises(Exception):
            self.test_db.put('test_bad_key', 'test_bad_val', 123)

    def test_del_DB_transac(self):
        expected_trans = {
            'test_del_transactionID': {
                'value': {
                    'test_del_key_2': 'test_val_2',
                },
                'transaction_uuid': {
                    'test_del_key_2': 'uuid_2',
                }
            }
        }
        self.test_db.insert_transaction({
            'test_del_transactionID': {
                'value': {
                    'test_del_key_1': 'test_val_1',
                    'test_del_key_2': 'test_val_2',
                },
                'transaction_uuid': {
                    'test_del_key_1': 'uuid_1',
                    'test_del_key_2': 'uuid_2',
                }
            }
        })
        self.test_db.delete('test_del_key_1', 'test_del_transactionID')
        self.assertEqual(self.test_db.getTransaction(), expected_trans)

        with self.assertRaises(Exception):
            self.test_db.delete('test_bad_key', 'bad_transID')
        with self.assertRaises(Exception):
            self.test_db.delete('test_bad_key', 123)

    def test_createTransaction(self):
        expected_trans = {
            'Test_transID': {
                'value': {},
                'transaction_uuid': {}
            }
        }
        self.test_db.createTransaction('Test_transID')
        self.assertEqual(self.test_db.getTransaction(), expected_trans)

        with self.assertRaises(Exception):
            self.test_db.createTransaction('Test_transID')
        with self.assertRaises(Exception):
            self.test_db.createTransaction(123)

    def test_rollbackTransaction(self):
        self.test_db.insert_transaction({
            'test_rollback_transID': {
                'value': {
                    'test_key_1': 'test_val_1',
                },
                'transaction_uuid': {
                    'test_key_1': 'uuid_1',
                }
            }
        })
        self.test_db.rollbackTransaction('test_rollback_transID')
        self.assertEqual(self.test_db.getTransaction(), {})

        with self.assertRaises(Exception):
            self.test_db.rollbackTransaction('bad_transID')

    def commitTransaction(self):
        expected_DB = {
            'test_get_key': 'test_val_1',
            'test_key_1': 'test_val',
        }
        self.test_db.insert_transaction({
            'test_commit_transID': {
                'value': {
                    'test_key_1': 'test_val',
                },
                'transaction_uuid': {
                    'test_key_1': 'uuid',
                }
            }
        })
        self.test_db.commitTransaction('test_commit_transID')
        self.assertEqual(self.test_db.getTransaction(), {})
        self.assertEqual(self.test_db.getDB(), expected_DB)

        # Test dirty read
        self.test_db.put('dirty_read_key', 'init_val')
        self.test_db.createTransaction('dirty_read_trans')
        self.test_db.put('dirty_read_key', 'dirty_val')
        self.test_db.get('dirty_read_key')
        with self.assertRaises(Exception):
            self.test_db.commitTransaction('dirty_read_trans')

        # Test delete will invalid transaction
        self.test_db.put('delete_key', 'init_val')
        self.test_db.createTransaction('delete_trans')
        self.test_db.put('delete_key', 'dirty_val')
        self.test_db.delete('delete_key')
        with self.assertRaises(Exception):
            self.test_db.commitTransaction('delete_trans')

        # Test modify will invalid transaction
        self.test_db.put('put_key', 'init_val')
        self.test_db.createTransaction('put_trans')
        self.test_db.put('put_key', 'put_val', 'put_trans')
        self.test_db.put('put_key', 'dirty_put_val')
        with self.assertRaises(Exception):
            self.test_db.commitTransaction('put_trans')

        with self.assertRaises(Exception):
            self.test_db.commitTransaction('bad_transID')


class TestSimpleDB_Scenario(unittest.TestCase):
    def setUp(self):
        self.test_db = SimpleDB()

    def test_flow_no_transaction(self):
        self.test_db.put('example', 'foo')

        receive_val = self.test_db.get('example')
        self.assertEqual(receive_val, 'foo')

        self.test_db.delete('example')
        self.assertEqual(self.test_db.getDB(), {})

        with self.assertRaises(Exception):
            self.test_db.get('example')
        with self.assertRaises(Exception):
            self.test_db.delete('example')

    def test_flow_transaction(self):
        self.test_db.createTransaction('abc')
        self.test_db.put('a', 'foo', 'abc')

        # returns 'foo'
        receive_val = self.test_db.get('a', 'abc')
        self.assertEqual(receive_val, 'foo')

        # returns null / error
        with self.assertRaises(Exception):
            self.test_db.get('a')

        self.test_db.createTransaction('xyz')
        self.test_db.put('a', 'bar', 'xyz')

        #  // returns 'bar'
        receive_val = self.test_db.get('a', 'xyz')
        self.assertEqual(receive_val, 'bar')

        self.test_db.commitTransaction('xyz')

        #  // returns 'bar'
        receive_val = self.test_db.get('a')
        self.assertEqual(receive_val, 'bar')

        #  // failure
        with self.assertRaises(Exception):
            self.test_db.commitTransaction('abc')

        # // returns 'bar'
        receive_val = self.test_db.get('a')
        self.assertEqual(receive_val, 'bar')

        self.test_db.createTransaction('abc')
        self.test_db.put('a', 'foo', 'abc')

        #  // returns 'bar'
        receive_val = self.test_db.get('a')
        self.assertEqual(receive_val, 'bar')

        self.test_db.rollbackTransaction('abc')

        #  // failure
        with self.assertRaises(Exception):
            self.test_db.put('a', 'foo', 'abc')

        #  // returns 'bar'
        receive_val = self.test_db.get('a')
        self.assertEqual(receive_val, 'bar')

        self.test_db.createTransaction('def')
        self.test_db.put('b', 'foo', 'def')

        '''
        NOTE: in the example this is return as bar but base on the API descibtion
        ● String get(String key, String transactionId)
            ○ Returns the value associated with “key” within the transaction with ID
            “transactionId”
            ○ Throws an exception or returns an error on failure
        This following command get('a', 'def') should throw an exception since
        the key 'a' was never assign any value within the transaction 'def'
        '''
        with self.assertRaises(Exception):
            self.test_db.get('a', 'def')

        #  // returns 'foo'
        receive_val = self.test_db.get('b', 'def')
        self.assertEqual(receive_val, 'foo')

        self.test_db.rollbackTransaction('def')

        #  // returns null
        with self.assertRaises(Exception):
            self.test_db.get('b')


if __name__ == '__main__':
    unittest.main()
