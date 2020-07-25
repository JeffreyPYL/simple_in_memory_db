import unittest
from helper import checkStr

class TestHelper(unittest.TestCase):
    
    def test_checkStr(self):
        self.assertTrue(checkStr('test_str'))
        self.assertFalse(checkStr(123))