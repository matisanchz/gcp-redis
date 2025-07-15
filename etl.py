import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

def bulk_insert_user_documents(users):
    user_documents = []
    subtask_documents = []
    n = 1
    for user in users:
        logger.info(f"Processing user {n} - {user['_id']}")
        user_document = get_user_document(user)
        user_documents.append(user_document)
        subtask_document = get_athlete_subtask_document(user)
        if subtask_document:
            subtask_documents.append(subtask_document)
        n+=1

    insert_user_documents(user_documents)
    insert_athlete_subtask_documents(subtask_documents)

def bulk_insert_campaign_documents(campaigns):
    documents = []
    n = 1
    for campaign in campaigns:
        logger.info(f"Processing campaign {n} - {campaign['_id']}")
        document = get_campaign_document(campaign)
        documents.append(document)
        n+=1
    
    insert_campaign_documents(documents)

# <<<<<<<<<<<<<<<<<<<<<<<<<<< ETL >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def etl_users():
    try:
        logger.info("Starting ETL for collections users/useridentities/subtasks")
        users = get_athlete_users()
        bulk_insert_user_documents(users)
        logger.info("ETL Completed for collections users/useridentities/subtasks")
    except Exception as e:
        logger.error(f'Users ETL process failed: {e}')
        raise e

def etl_campaigns():
    try:
        logger.info("Starting ETL for collections campaigns/tasks")
        campaigns = get_campaigns()
        bulk_insert_campaign_documents(campaigns)
        logger.info("ETL Completed for collections campaigns/tasks")
    except Exception as e:
        logger.error(f'Campaigns ETL process failed: {e}')
        raise e

def etl():
    try:
        etl_users()
        etl_campaigns()
        logger.info(f"ETL sucessfully completed")
    except Exception as e:
        logger.error(f'ETL process failed: {e}')
        raise e