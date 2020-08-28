import logging
from util import loggers
from webflow import cms

loggers.init_root_logger()

my_logger = logging.getLogger("nectr")

items = cms.get_items("5eaf0803a0d3e484ca69b0db")

print(items)