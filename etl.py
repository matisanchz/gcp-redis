import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

def bulk_insert_user_documents(users):
    documents = []
    n = 1
    for user in users:
        logger.info(f"Processing user {n} - {user['_id']}")
        document = get_user_document(user)
        documents.append(document)
        subtask_document = get_athlete_subtask_document(user)
        if subtask_document:
            insert_athlete_subtask_document(subtask_document)
        n+=1
    
    insert_user_documents(documents)

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
    logger.info("Starting ETL for collections users/useridentities/subtasks")
    users = get_athlete_users()
    bulk_insert_user_documents(users)
    logger.info("ETL Completed for collections users/useridentities/subtasks")

def etl_campaigns():
    logger.info("Starting ETL for collections campaigns/tasks")
    campaigns = get_campaigns()
    bulk_insert_campaign_documents(campaigns)
    logger.info("ETL Completed for collections campaigns/tasks")

def etl():
    try:
        etl_users()
        etl_campaigns()
    except Exception as e:
        logger.error(f'Error embedding documents')
        return e