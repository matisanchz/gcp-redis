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
            return insert_user_document(document)
        elif collection == 'useridentities':
            logger.info("Useridentities INSERT event detected")
            return insert_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign INSERT event detected")
            return insert_campaign_document(document)
        elif collection == 'tasks':
            logger.info("Task INSERT event detected")
            return insert_task_document(document)
        elif collection == 'subtasks':
            logger.info("Subtask INSERT event detected")
            return insert_subtask_document(document)

    # Change Streams for update activities
    elif operation_type == "update":
        if collection == 'users':
            logger.info("User UPDATE event detected")
            return update_user_document(_id, document, updatedFields)
        elif collection == 'useridentities':
            logger.info("Useridentities UPDATE event detected")
            return update_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign UPDATE event detected")
            return update_campaign_document(document)
        elif collection == 'tasks':
            logger.info("Task UPDATE event detected")
            return update_task_document(document, updatedFields)
        elif collection == 'subtasks':
            logger.info("Subtask UPDATE event detected")
            return update_subtask_document(document, updatedFields)
        elif collection == 'organizations':
            logger.info("Organization UPDATE event detected")
            return update_organization_event(_id, updatedFields, removedFields)
    
    # Change Streams for delete activities
    elif operation_type == "delete":
        if collection == 'users':
            logger.info("User DELETE event detected")
            return delete_user_document(_id)
        elif collection == 'useridentities':
            logger.info("Useridentities DELETE event detected")
            return delete_useridentities_document(document)
        elif collection == 'campaigns':
            logger.info("Campaign DELETE event detected")
            return delete_campaign_document(_id)
        elif collection == 'tasks':
            logger.info("Task DELETE event detected")
            return delete_task_document(document)
        elif collection == 'subtasks':
            logger.info("Subtask DELETE event detected")
            return delete_subtask_document(document)
    
    logger.info(f"Other changes detected for collection {collection}: {operation_type}")

# <----------------- USER ---------------------->

def insert_user_document(user):
    logger.info(f"Processing INSERT user {user['_id']}")
    doc = get_user_document(user)
    insert_user_documents([doc])

def update_user_document(_id, user, updateFields):
    user_id = str(user["_id"])
    logger.info(f"Processing UPDATE user {user_id}")
    doc = get_updated_user_document(user)
    delete_user_document(user_id)
    insert_user_documents([doc])
    # If the user change organization, we must change the subtask docs metadata
    if "organizationId" in updateFields:
        doc = get_updated_subtask_organization_document(_id)
        delete_subtask_document(_id)
        insert_athlete_subtask_documents([doc])

# <----------------- USERIDENTITIES ---------------------->
def insert_useridentities_document(useridentity):
    logger.info(f"Processing INSERT useridentities {useridentity['userId']}")
    doc = get_updated_useridentities_document(useridentity)
    # Consider the posibility that some useridentities register belongs to user != "ATHLETE"
    if doc:
        delete_user_document(useridentity['userId'])
        insert_user_documents([doc])

def update_useridentities_document(useridentity):
    logger.info(f"Processing UPDATE useridentities {useridentity['userId']}")
    insert_useridentities_document(useridentity)

def delete_useridentities_document(useridentity):
    logger.info(f"Processing DELETE useridentities {useridentity['userId']}")
    # Pass an empty entity with id
    insert_useridentities_document({'userId': useridentity['userId']})

# <----------------- CAMPAIGNS ---------------------->
def insert_campaign_document(campaign):
    logger.info(f"Processing INSERT campaign {str(campaign['_id'])}")
    document = get_campaign_document(campaign)
    insert_campaign_documents([document])

def update_campaign_document(campaign):
    logger.info(f"Processing UPDATE campaign {str(campaign['_id'])}")
    doc = get_updated_campaign_document(campaign)
    delete_campaign_document(str(campaign['_id']))
    insert_campaign_documents([doc])

# <----------------- TASKS ---------------------->
def insert_task_document(task):
    logger.info(f"Processing INSERT task for campaign: {str(task['campaignId'])}")
    doc = get_updated_task_document(task, True)
    delete_campaign_document(str(task['campaignId']))
    insert_campaign_documents([doc])

def update_task_document(task, updatedFields):
    logger.info(f"Processing UPDATE task for campaign: {str(task['campaignId'])}")
    doc = get_updated_task_document(task, False)
    delete_campaign_document(str(task['campaignId']))
    insert_campaign_documents([doc])

    # If the campaignId was modified, we must restore 2 documents
    if updatedFields and "campaignId" in updatedFields:
        logger.info(f"Processing UPDATE task for campaign: {str(updatedFields['campaignId'])}")
        doc = get_updated_task_document(updatedFields, False)
        delete_campaign_document(str(updatedFields['campaignId']))
        insert_campaign_documents([doc])

def delete_task_document(task):
    logger.info(f"Processing DELETE task for campaign: {str(task['campaignId'])}")
    update_task_document(task, None)

# <----------------- SUBTASKS ---------------------->
def insert_subtask_document(subtask):
    logger.info(f"Processing INSERT subtask for user: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, True)
    if doc:
        delete_subtask_document(str(subtask['athleteId']))
    else:
        # If no doc is found, the user has no previous subtask.
        doc = get_new_single_subtask_document(subtask)
    
    insert_athlete_subtask_documents([doc])

def update_subtask_document(subtask, updatedFields):
    logger.info(f"Processing UPDATE subtask for user: {str(subtask['athleteId'])}")
    doc = get_updated_subtask_document(subtask, False)
    delete_subtask_document(str(subtask['athleteId']))
    insert_athlete_subtask_documents([doc])

    # If the athleteId was modified, we must restore 2 documents
    if updatedFields and "athleteId" in updatedFields:
        logger.info(f"Processing UPDATE subtask for user: {str(updatedFields['athleteId'])}")
        doc = get_updated_subtask_document(updatedFields, False)
        delete_subtask_document(str(updatedFields['athleteId']))
        insert_athlete_subtask_documents([doc])

def delete_subtask_document(subtask):
    logger.info(f"Processing DELETE subtask for user: {str(subtask['athleteId'])}")
    update_subtask_document(subtask, None)

# <----------------- ORGANIZATIONS ---------------------->
def update_organization_event(_id, updatedFields, removedFields):
    if "name" in updatedFields or "name" in removedFields:
        logger.info(f"Processing UPDATE name field for organization: {_id}")
        name = updatedFields["name"] if "name" in updatedFields else "Withouth Name"
        logger.info(f"Processing UPDATE organization name: {name}")
        doc = get_updated_user_organization_document(_id, name)
        delete_user_document(doc.metadata["user_id"])
        insert_user_documents([doc])
    else:
        logger.info(f"No changes need to be processed for organization: {_id}")