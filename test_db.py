import unittest
from db import SimpleDB

class TestSimpleDB(unittest.TestCase):
    def setUp(self):
        self.test_db = SimpleDB({
            'test_get_key': 'test_val_1',
        })

    def test_get_DB_no_transac(self):
        expected_val = 'test_val_1'
        receive_val = self.test_db.get('test_get_key')
        self.assertEqual(receive_val, expected_val)

        with self.assertRaises(Exception): self.test_db.get('test_bad_key')

        with self.assertRaises(Exception): self.test_db.put(123)

    def test_put_DB_no_transac(self):
        expected_db = SimpleDB({
            'test_get_key': 'test_val_1',
            'test_key': 'test_value'
        })
        self.test_db.put('test_key', 'test_value')
        self.assertEqual(self.test_db.getDB(), expected_db.getDB())

        with self.assertRaises(Exception): self.test_db.put('test_bad_key')
        # Test bad input
        with self.assertRaises(Exception): self.test_db.put('test_bad_key', {'key': 123})
        # Test bad key
        with self.assertRaises(Exception): self.test_db.put(123, '123')


    def test_del_DB_no_transac(self):
        expected_db = SimpleDB()
        self.test_db.delete('test_get_key')
        self.assertEqual(self.test_db.getDB(), expected_db.getDB())

        with self.assertRaises(Exception): self.test_db.put('test_bad_key')
        with self.assertRaises(Exception): self.test_db.put(123)
        


if __name__ == '__main__':
    unittest.main()