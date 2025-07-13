import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

async def bulk_insert_user_documents(users):
    documents = []
    n = 1
    for user in users:
        logger.info(f"Processing user {n} - {user['_id']}")
        document = await get_user_document(user)
        documents.append(document)
        subtask_document = await get_athlete_subtask_document(user)
        if subtask_document:
            await insert_athlete_subtask_document(subtask_document)
        n+=1
    
    await insert_user_documents(documents)

async def bulk_insert_campaign_documents(campaigns):
    documents = []
    n = 1
    for campaign in campaigns:
        logger.info(f"Processing campaign {n} - {campaign['_id']}")
        document = await get_campaign_document(campaign)
        documents.append(document)
        n+=1
    
    await insert_campaign_documents(documents)

# <<<<<<<<<<<<<<<<<<<<<<<<<<< ETL >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
async def etl_users():
    try:
        logger.info("Starting ETL for collections users/useridentities/subtasks")
        users = await get_athlete_users()
        await bulk_insert_user_documents(users)
        logger.info("ETL Completed for collections users/useridentities/subtasks")
    except Exception as e:
        logger.error(f'Error embedding documents')
        return {"error": f"Users ETL process failed: {e}"}, 500

async def etl_campaigns():
    try:
        logger.info("Starting ETL for collections campaigns/tasks")
        campaigns = await get_campaigns()
        await bulk_insert_campaign_documents(campaigns)
        logger.info("ETL Completed for collections campaigns/tasks")
    except Exception as e:
        logger.error(f'Error embedding documents')
        return {"error": f"Campaigns ETL process failed: {e}"}, 500

async def etl():
    try:
        await etl_users()
        await etl_campaigns()
        logger.info(f"ETL sucessfully completed")
    except Exception as e:
        logger.error(f'Error embedding documents')
        return {"error": f"ETL process failed: {e}"}, 500