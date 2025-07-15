import logging
from extract import *
from transform import *
from load import *

logger = logging.getLogger(__name__)

def process_change(collection, operation_type, _id, document, updatedFields, removedFields):
    
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
            update_user_document(_id, document, updatedFields)
        elif collection == 'useridentities':
            logger.info("Useridentities UPDATE event detected")
            update_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign UPDATE event detected")
            update_campaign_document(document)
        elif collection == 'tasks':
            logger.info("Task UPDATE event detected")
            update_task_document(document, updatedFields)
        elif collection == 'subtasks':
            logger.info("Subtask UPDATE event detected")
            update_subtask_document(document)
        elif collection == 'organizations':
            logger.info("Organization UPDATE event detected")
            update_organization_event(_id, updatedFields, removedFields)
    
    # Change Streams for delete activities
    elif operation_type == "delete":
        if collection == 'users':
            logger.info("User DELETE event detected")
            delete_user_document(_id)
        elif collection == 'useridentities':
            logger.info("Useridentities DELETE event detected")
            delete_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign DELETE event detected")
            delete_campaign_document(_id)
        elif collection == 'tasks':
            logger.info("Task DELETE event detected")
            delete_task_document(document["campaignId"])
        elif collection == 'subtasks':
            logger.info("Subtask DELETE event detected")
            delete_subtask_document(document)
    
    logger.info(f"Other changes: {operation_type}")

# <----------------- USER ---------------------->

def insert_user_document(user):
    logger.info(f"Processing INSERT user {user['_id']}")
    doc = get_user_document(user)
    insert_user_documents(doc)

def update_user_document(_id, user, updateFields):
    user_id = str(user["_id"])
    logger.info(f"Re-indexing document for user_id: {user_id}")
    doc = get_updated_user_document(user)
    delete_user_document(user_id)
    insert_user_documents(doc)
    # If the user change organization, we must change the subtask docs metadata
    if "organizationId" in updateFields:
        doc = get_updated_subtask_organization_document(_id)
        delete_subtask_document(_id)
        insert_athlete_subtask_documents(doc)

# <----------------- USERIDENTITIES ---------------------->
def insert_useridentities_document(useridentity):
    logger.info(f"Processing INSERT user {useridentity['userId']}")
    doc = get_updated_useridentities_document(useridentity)
    # Consider the posibility that some useridentities register belongs to user != "ATHLETE"
    if doc:
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

def update_task_document(task, updatedFields):
    logger.info(f"Re-indexing document for campaign_id: {str(task['campaignId'])}")
    doc = get_updated_task_document(task, False)
    delete_campaign_document(str(task['campaignId']))
    insert_campaign_documents(doc)

    # If the campaignId was modified, we must restore 2 documents
    if "campaignId" in updatedFields:
        doc = get_updated_task_document(updatedFields, False)
        delete_campaign_document(str(updatedFields['campaignId']))
        insert_campaign_documents(doc)

def delete_task_document(task):
    update_task_document(task)

# <----------------- SUBTASKS ---------------------->
def insert_subtask_document(subtask):
    logger.info(f"Re-indexing document for user_id: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, True)
    delete_subtask_document(str(subtask['athleteId']))
    insert_athlete_subtask_documents(doc)

def update_subtask_document(subtask, updatedFields):
    logger.info(f"Re-indexing document for user_id: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, False)
    delete_subtask_document(str(subtask['athleteId']))
    insert_athlete_subtask_documents(doc)

    # If the athleteId was modified, we must restore 2 documents
    if "athleteId" in updatedFields:
        doc = get_updated_subtask_document(updatedFields, False)
        delete_subtask_document(str(updatedFields['athleteId']))
        insert_athlete_subtask_documents(doc)

def delete_subtask_document(subtask):
    update_subtask_document(subtask)

# <----------------- ORGANIZATIONS ---------------------->
def update_organization_event(_id, updatedFields, removedFields):
    if "name" in updatedFields or "name" in removedFields:
        name = updatedFields["name"] if "name" in updatedFields else "Withouth Name"
        logger.info(f"Processing UPDATE organization name: {name}")
        doc = get_updated_user_organization_document(_id, name)
        delete_user_document(doc.metadata["user_id"])
        insert_user_documents(doc)
    else:
        logger.info(f"No changes need to be processed for organization: {_id}")