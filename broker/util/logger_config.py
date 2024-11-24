import logging
from colorama import Fore, Style, init
import os

# Initialize colorama for colored output
init(autoreset=True)

# Define a custom formatter with colors for console output
class ColorFormatter(logging.Formatter):
    # Define colors for different log levels
    LEVEL_COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN + Style.BRIGHT,
        'WARNING': Fore.YELLOW + Style.BRIGHT,
        'ERROR': Fore.RED + Style.BRIGHT,
        'CRITICAL': Fore.RED + Style.BRIGHT
    }
    
    def format(self, record):
        # Apply color based on the log level
        log_color = self.LEVEL_COLORS.get(record.levelname, Fore.WHITE)
        record.levelname = f"{log_color}{record.levelname}{Style.RESET_ALL}"  # Colorize level name
        
        # Return the formatted message
        return super().format(record)

# Function to configure the root logger
def setup_logger(log_file='app.log', log_level=logging.DEBUG):
    # Create a custom root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # If the logger already has handlers, avoid adding new ones
    if not logger.hasHandlers():
        # Console handler with color output
        console_handler = logging.StreamHandler()
        color_formatter = ColorFormatter(
            "%(asctime)s | PID:%(process)d | %(levelname)-7s | %(filename)s:%(lineno)d | %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(color_formatter)

        # File handler to save logs to a file
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(
            "%(asctime)s | PID:%(process)d | %(levelname)-7s | %(filename)s:%(lineno)d | %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)

        # Add both handlers to the root logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# Configure the logger once when this file is imported
setup_logger()
