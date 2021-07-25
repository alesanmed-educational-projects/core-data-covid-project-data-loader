import os
import re

from dotenv import load_dotenv

from .logger import init_logger

load_dotenv()

init_logger()

with open(os.path.join(os.path.dirname(__file__), "../", "pyproject.toml"), "r") as f:
    project_info = f.read()

    regex = r"version = \"(.*)\""

    __version__ = re.findall(regex, project_info)[0]
