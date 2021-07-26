import unittest
from unittest import TestCase
from unittest.mock import MagicMock

from sqlalchemy.engine.base import Connection

import covid_data.db as db
from covid_data.db.queries import country_exists
from covid_data.test_utils import MockDB


class TestCountryExists(TestCase):
    def test_execute_called(self):
        """Country exists should call execute once"""

        Connection.execute = MagicMock(return_value=[(0,)])

        country_exists("Test", db.get_db())

        Connection.execute.assert_called_once()

    def test_should_return_false(self):
        """Country exists should return false"""
        mockDb = MockDB()

        mockDb.execute_return = [(0,)]

        res = country_exists("Test", mockDb)

        self.assertFalse(res)

    def test_should_return_true(self):
        """Country exists should return true"""
        mockDb = MockDB()

        mockDb.execute_return = [(1,)]

        res = country_exists("Test", mockDb)

        self.assertTrue(res)


if __name__ == "__main__":
    unittest.main()
