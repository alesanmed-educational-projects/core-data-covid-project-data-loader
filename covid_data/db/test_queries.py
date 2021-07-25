import unittest
from unittest import TestCase
from unittest.mock import MagicMock

import covid_data.db as db
from covid_data.db.queries import country_exists
from covid_data.test_utils import MockDB


class TestQueries(TestCase):
    def test_country_exists(self):
        mockDb = MockDB()

        country_exists("Test", mockDb)


if __name__ == "__main__":
    unittest.main()
