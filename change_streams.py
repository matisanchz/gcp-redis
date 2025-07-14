import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

async def process_change(change):
    operation_type = change.get("operationType")
    
    # Change Streams for insert activities
    if operation_type == "insert":
        if change["ns"]["coll"] == 'users':
            logger.info("User INSERT event detected")
            await insert_user_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'useridentities':
            logger.info("Useridentities INSERT event detected")
            pass
        elif change["ns"]["coll"] == 'campaigns':
            logger.info("Campaign INSERT event detected")
            await insert_campaign_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'tasks':
            logger.info("Task INSERT event detected")
            await insert_task_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'subtasks':
            logger.info("Subtask INSERT event detected")
            await insert_subtask_document(change["fullDocument"])

    # Change Streams for update activities
    elif operation_type == "update":
        if change["ns"]["coll"] == 'users':
            logger.info("User UPDATE event detected")
            await update_user_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'useridentities':
            logger.info("Useridentities UPDATE event detected")
            pass
        elif change["ns"]["coll"] == 'campaigns':
            logger.info("Campaign UPDATE event detected")
            await update_campaign_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'tasks':
            logger.info("Task UPDATE event detected")
            await update_task_document(change["fullDocument"])
        elif change["ns"]["coll"] == 'subtasks':
            logger.info("Subtask UPDATE event detected")
            await update_subtask_document(change["fullDocument"])
            
    # Change Streams for delete activities
    elif operation_type == "delete":
        if change["ns"]["coll"] == 'users':
            logger.info("User DELETE event detected")
            await delete_user_document(str(change["documentKey"]["_id"]))
        elif change["ns"]["coll"] == 'useridentities':
            logger.info("Useridentities DELETE event detected")
            pass
        elif change["ns"]["coll"] == 'campaigns':
            logger.info("Campaign DELETE event detected")
            await delete_campaign_document(str(change["documentKey"]["_id"]))
        elif change["ns"]["coll"] == 'tasks':
            logger.info("Task DELETE event detected")
            await delete_task_document(change["fullDocument"]["campaignId"])
        elif change["ns"]["coll"] == 'subtasks':
            logger.info("Subtask DELETE event detected")
            await delete_subtask_document(change["fullDocument"])
    else:
        logger.info(f"Other changes: {operation_type}")

# <----------------- USER ---------------------->

async def insert_user_document(user):
    logger.info(f"Processing INSERT user {user['_id']}")
    doc = await get_user_document(user)
    await insert_user_documents(doc)

async def update_user_document(user):
    user_id = str(user["_id"])
    logger.info(f"Re-indexing document for user_id: {user_id}")
    await delete_user_document(user_id)
    await insert_user_document(user)

# <----------------- CAMPAIGNS ---------------------->
async def insert_campaign_document(campaign):
    document = await get_campaign_document(campaign)
    await insert_campaign_documents([document])

async def update_campaign_document(campaign):
    logger.info(f"Re-indexing document for campaign_id: {str(campaign['_id'])}")
    await delete_campaign_document(str(campaign["_id"]))
    await insert_campaign_document(campaign)

# <----------------- TASKS ---------------------->
async def insert_task_document(campaign_id):
    campaign = get_campaign_by_id(campaign_id)
    await update_campaign_document(campaign)

async def update_task_document(campaign_id):
    campaign = get_campaign_by_id(campaign_id)
    await update_campaign_document(campaign)

async def delete_task_document(campaign_id):
    campaign = get_campaign_by_id(campaign_id)
    await update_campaign_document(campaign)

# <----------------- SUBTASKS ---------------------->
async def insert_subtask_document(user_id):
    user = get_user_by_id(user_id)
    await update_athlete_subtask_document(user)

async def update_subtask_document(user_id):
    user = get_user_by_id(user_id)
    await update_athlete_subtask_document(user)

async def delete_subtask_document(user_id):
    user = get_user_by_id(user_id)
    await update_athlete_subtask_document(user)

async def update_athlete_subtask_document(user):
    logger.info(f"Re-indexing subtasks document for user_id: {str(user['_id'])}")
    await delete_athlete_subtask_document(str(user["_id"]))
    document = await get_athlete_subtask_document(user)
    await insert_athlete_subtask_documents([document])