from ast import literal_eval # This is for interpreting [lists] from csv files
from pathlib import Path
import pandas as pd

import .templates

SITE = os.environ.get('SITE')
DATE = os.environ.get('DATE')
BASE_DIR = os.environ.get('BASE_DIR')
