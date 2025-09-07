import sys
import logging

# Configure basic logger if not already configured
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_detail):
        super().__init__(error_message)
        self.error_message = error_message

        # Extract traceback info
        tb = error_detail.__traceback__
        self.line_number = tb.tb_lineno
        self.file_name = tb.tb_frame.f_code.co_filename

    def __str__(self):
        return f"Error occurred in Python script: {self.file_name} at line {self.line_number} with message: {self.error_message}"


if __name__ == "__main__":
    try:
        logger.info("Enter the try block.")
        result = 1 / 0  # This will raise a ZeroDivisionError
        logger.info("This message will not be logged due to exception.")
    except Exception as e:
        raise NetworkSecurityException(e, e)
