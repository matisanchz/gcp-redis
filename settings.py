import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

#MONGO DB
MONGO_URI = os.environ.get('MONGO_URI')
MONGO_CLIENT = MongoClient(MONGO_URI,tlsAllowInvalidCertificates=True)

#DATABASES
DATABASES = ['users', 'campaigns']

#REDIS
REDIS_URL = os.environ.get('REDIS_URL')

#INDEXES
REDIS_ATHLETE_INDEX = "athlete_profiles_idx"
REDIS_CAMPAIGN_INDEX = "campaign_idx"
REDIS_SUBTASK_INDEX = "athlete_subtask_completion_idx"

#GEMINI
GEMINI_API_KEY=os.environ.get('GEMINI_API_KEY')

#EMBEDDING MODEL
GEMINI_EMBEDDING_MODEL = "models/embedding-001"