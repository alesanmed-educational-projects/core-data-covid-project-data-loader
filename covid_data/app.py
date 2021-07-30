import os

from dotenv import load_dotenv

from .logger import init_logger

load_dotenv()

init_logger(os.path.join(os.path.dirname(__file__), "../logs/covid_data.log"))
