import unittest
from unittest import TestCase
from unittest.mock import Mock, patch

from covid_data.commands.load_csv import (
    PlaceNameNotProvidedException,
    PlaceType,
    get_place_info,
)


class TestGetPlaceInfo(TestCase):
    def setUp(self) -> None:
        self.mock_place = {
            "results": [
                {
                    "components": {
                        "ISO_3166-1_alpha-2": "Test",
                        "ISO_3166-1_alpha-3": "Test",
                        "_type": "Test",
                        "_category": "Test",
                    }
                }
            ]
        }

    def test_exception_when_no_name(self):
        """Should raise an exception when no place is passed"""
        self.assertRaises(PlaceNameNotProvidedException, get_place_info, None)

    @patch("requests.get")
    def test_none_when_error(self, mock_get: Mock):
        """Should return None when the response code is an error"""
        mock_get.return_value.status_code = 400

        res = get_place_info("TestPlace")

        self.assertIsNone(res)

    @patch("requests.get")
    def test_properties_renaming(self, mock_get: Mock):
        """Should rename properties in JSON response"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = Mock(return_value=self.mock_place)

        try:
            get_place_info("Test")
        except Exception:
            pass

        self.assertDictEqual(
            self.mock_place,
            {
                "results": [
                    {
                        "components": {
                            "alpha2": "Test",
                            "alpha3": "Test",
                            "type": "Test",
                            "category": "Test",
                        }
                    }
                ]
            },
        )

    @patch("requests.get")
    def test_territory_renaming(self, mock_get: Mock):
        """Should rename territory into state"""
        mock_get.return_value.status_code = 200
        self.mock_place["results"][0]["components"]["_type"] = PlaceType.TERRITORY
        self.mock_place["results"][0]["components"]["territory"] = "Test"
        mock_get.return_value.json = Mock(return_value=self.mock_place)

        try:
            get_place_info("Test Place")
        except Exception:
            pass

        self.assertIn("state", self.mock_place["results"][0]["components"])
        self.assertNotIn("territory", self.mock_place["results"][0]["components"])
        self.assertEqual(
            self.mock_place["results"][0]["components"]["type"], PlaceType.STATE
        )


if __name__ == "__main__":
    unittest.main()
