import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

def process_change(collection, operation_type, document):
    
    # Change Streams for insert activities
    if operation_type == "insert":
        if collection == 'users':
            logger.info("User INSERT event detected")
            insert_user_document(document)
        elif collection == 'useridentities':
            logger.info("Useridentities INSERT event detected")
            insert_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign INSERT event detected")
            insert_campaign_document(document)
        elif collection == 'tasks':
            logger.info("Task INSERT event detected")
            insert_task_document(document)
        elif collection == 'subtasks':
            logger.info("Subtask INSERT event detected")
            insert_subtask_document(document)

    # Change Streams for update activities
    elif operation_type == "update":
        if collection == 'users':
            logger.info("User UPDATE event detected")
            update_user_document(document)
        elif collection == 'useridentities':
            logger.info("Useridentities UPDATE event detected")
            update_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign UPDATE event detected")
            update_campaign_document(document)
        elif collection == 'tasks':
            logger.info("Task UPDATE event detected")
            update_task_document(document)
        elif collection == 'subtasks':
            logger.info("Subtask UPDATE event detected")
            update_subtask_document(document)
    
    # Change Streams for delete activities
    elif operation_type == "delete":
        change = document
        if collection == 'users':
            logger.info("User DELETE event detected")
            delete_user_document(str(change["documentKey"]["_id"]))
        elif collection == 'useridentities':
            logger.info("Useridentities DELETE event detected")
            delete_useridentities_document(str(change["fullDocumentBeforeChange"]))
        elif collection == 'campaigns':
            logger.info("Campaign DELETE event detected")
            delete_campaign_document(str(change["documentKey"]["_id"]))
        elif collection == 'tasks':
            logger.info("Task DELETE event detected")
            delete_task_document(change["fullDocument"]["campaignId"])
        elif collection == 'subtasks':
            logger.info("Subtask DELETE event detected")
            delete_subtask_document(change["fullDocument"])
    
    logger.info(f"Other changes: {operation_type}")

# <----------------- USER ---------------------->

def insert_user_document(user):
    logger.info(f"Processing INSERT user {user['_id']}")
    doc = get_user_document(user)
    insert_user_documents(doc)

def update_user_document(user):
    user_id = str(user["_id"])
    logger.info(f"Re-indexing document for user_id: {user_id}")
    doc = get_updated_user_document(user)
    delete_user_document(user_id)
    insert_user_documents(doc)

# <----------------- USERIDENTITIES ---------------------->
def insert_useridentities_document(useridentity):
    logger.info(f"Processing INSERT user {useridentity['userId']}")
    doc = get_updated_useridentities_document(useridentity)
    delete_user_document(useridentity['userId'])
    insert_user_documents(doc)

def update_useridentities_document(useridentity):
    logger.info(f"Processing UPDATE user {useridentity['userId']}")
    insert_useridentities_document(useridentity)

def delete_useridentities_document(useridentity):
    logger.info(f"Processing DELETE user {useridentity['userId']}")
    # Pass an empty entity with id
    insert_useridentities_document({'userId': useridentity['userId']})

# <----------------- CAMPAIGNS ---------------------->
def insert_campaign_document(campaign):
    document = get_campaign_document(campaign)
    insert_campaign_documents([document])

def update_campaign_document(campaign):
    logger.info(f"Re-indexing document for campaign_id: {str(campaign['_id'])}")
    doc = get_updated_campaign_document(campaign)
    delete_campaign_document(str(campaign['_id']))
    insert_campaign_documents(doc)

# <----------------- TASKS ---------------------->
def insert_task_document(task):
    logger.info(f"Re-indexing document for campaign_id: {str(task['campaignId'])}")
    doc = get_updated_task_document(task, True)
    delete_campaign_document(str(task['campaignId']))
    insert_campaign_documents(doc)

def update_task_document(task):
    logger.info(f"Re-indexing document for campaign_id: {str(task['campaignId'])}")
    doc = get_updated_task_document(task, False)
    delete_campaign_document(str(task['campaignId']))
    insert_campaign_documents(doc)

def delete_task_document(task):
    update_task_document(task)

# <----------------- SUBTASKS ---------------------->
def insert_subtask_document(subtask):
    logger.info(f"Re-indexing document for user_id: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, True)
    delete_subtask_document(str(subtask['athleteId']))
    insert_athlete_subtask_documents(doc)

def update_subtask_document(subtask):
    logger.info(f"Re-indexing document for user_id: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, False)
    delete_subtask_document(str(subtask['athleteId']))
    insert_athlete_subtask_documents(doc)

def delete_subtask_document(subtask):
    update_subtask_document(subtask)