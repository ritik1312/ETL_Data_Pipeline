import logging
from logging.handlers import RotatingFileHandler

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File handler to log debug and higher level messages
file_handler = RotatingFileHandler('app.log', maxBytes=10240)
file_handler.setLevel(logging.DEBUG)

# Console handler for error messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)

# Formatter for both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)