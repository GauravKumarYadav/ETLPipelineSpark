import logging
import os 
from datetime import datetime

script_name = os.path.splitext(os.path.basename(__file__))[0]

LOG_FILE = f"{script_name}_{datetime.now().strftime('%m_%d_%Y_%H_')}.log"
logs_path = os.path.join(os.getcwd(),"logs",LOG_FILE)
os.makedirs(logs_path,exist_ok=True)

LOGS_FILE_PATH = os.path.join(logs_path,LOG_FILE)

logging.basicConfig(
    filename=LOGS_FILE_PATH,
    format= "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)