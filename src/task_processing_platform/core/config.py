'''
All setting in one place. Other files import from here instead of hardcoding urls. Later Docker/kubernetes can  will set this via environment variables, and no code needs to change.
'''

import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://taskuser:taskpass@localhost:5433/taskdb")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380/0")
QUEUE_NAME = "tasks:pending"