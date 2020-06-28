import logging
import sys

prvk8_logger = logging.getLogger('cbe', )
prvk8_logger.setLevel(logging.DEBUG)
cbe_formatter = logging.Formatter(f'PRVk8 --> %(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')

#-------------------------------------------------------- debug messages handler

debug_handler = logging.StreamHandler(sys.stdout)
debug_handler.setFormatter(cbe_formatter)
debug_handler.setLevel(logging.DEBUG)
prvk8_logger.addHandler(debug_handler)
