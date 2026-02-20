import logging
import re
import sys

# Custom Formatter to strip ANSI codes and format consistently
class LogFormatter(logging.Formatter):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def format(self, record):
        record_copy = logging.makeLogRecord(record.__dict__)
        if isinstance(record_copy.msg, str):
            record_copy.msg = self.ansi_escape.sub('', record_copy.msg)
        record_copy.levelname = f"{record_copy.levelname:<8}"
        return super().format(record_copy)

# Test setup
logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = LogFormatter('%(levelname)s| %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Test execution
print("--- TEST OUTPUT START ---")
logger.info("\033[31mRed Message\033[0m")
logger.warning("Normal Warning")
print("--- TEST OUTPUT END ---")
