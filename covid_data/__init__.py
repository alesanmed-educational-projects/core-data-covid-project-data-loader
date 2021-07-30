import os
import re

with open(os.path.join(os.path.dirname(__file__), "../", "pyproject.toml"), "r") as f:
    project_info = f.read()

    regex = r"version = \"(.*)\""

    __version__ = re.findall(regex, project_info)[0]
