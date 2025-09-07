import sys
from networksecurity.logging import logger

class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_details=None):
        """
        error_message: str -> Description of the error
        error_details: Exception -> Original exception object
        """
        super().__init__(error_message)
        self.error_message = error_message
        
        # Get traceback info if error_details is provided
        if error_details:
            _, _, exc_tb = sys.exc_info()
            if exc_tb:
                self.lineno = exc_tb.tb_lineno
                self.file_name = exc_tb.tb_frame.f_code.co_filename
            else:
                self.lineno = None
                self.file_name = None
        else:
            self.lineno = None
            self.file_name = None

    def __str__(self):
        return "Error occurred in python script name [{0}] line number [{1}] error message [{2}]".format(
            self.file_name, self.lineno, str(self.error_message)
        )

# Test the exception
if __name__ == '__main__':
    try:
        logger.logging.info("Enter the try block")
        a = 1 / 0  # This will raise ZeroDivisionError
        print("This will not be printed", a)
    except Exception as e:
        raise NetworkSecurityException("Division by zero error occurred", e)
