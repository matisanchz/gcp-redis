import settings
import logging
import re
from bson import ObjectId

mongo_client = settings.MONGO_CLIENT

logger = logging.getLogger(__name__)

def get_athlete_users():
    "Get all athlete users"
    try:
        logger.info('Getting users')
        db = mongo_client.get_database('users')
        user_collection = db['users']

        pipeline = [
            {
                "$match": {
                    "userType": "ATHLETE"
                }
            },
            {
                "$addFields": {
                    "organizationId": {
                        "$ifNull": ["$organizationId", None]
                    }
                }
            },
            {
                "$project": {
                    "profilePicture": 0,
                    "active": 0,
                    "createdAt": 0,
                    "updatedAt": 0,
                    "__v": 0,
                    "userType": 0,
                    "userId": 0
                }
            }
        ]

        users = list(user_collection.aggregate(pipeline))
        
        return users
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        raise e
    
def get_campaigns():
    "Get all campaigns"
    try:
        logger.info('Getting campaigns')
        db = mongo_client.get_database('campaigns')
        campaign_collection = db['campaigns']

        pipeline = [
            {
                "$project": {
                    "overview.adminId": 0,
                    "overview.vaId": 0,
                    "contractId": 0,
                    "createdAt": 0,
                    "updatedAt": 0,
                    "__v": 0
                }
            }
        ]

        campaign = list(campaign_collection.aggregate(pipeline))
        
        return campaign
    except Exception as e:
        logger.error(f"Error getting campaigns: {e}")
        raise e

def get_subtasks_by_user_id(user_id):
    "Get all subtasks by user"
    try:
        logger.info("Getting subtasks")
        db = mongo_client.get_database('campaigns')
        subtasks_collection = db['subtasks']

        subtasks = list(subtasks_collection.find({"athleteId": user_id}, {"_id": 0, "createdAt": 0, "updatedAt": 0, "__v": 0}))
        
        if subtasks:
            logger.info(f'Subtasks found for user {user_id}')
        else:
            logger.info(f'No subtasks found for user {user_id}')

        return subtasks
    except Exception as e:
        logger.error(f"Error getting user metadata: {e}")
        raise e

def get_user_metadata(user_id):
    "Get user metadata by user_id"
    try:
        logger.info(f'Getting user metadata for {user_id}')
        db = mongo_client.get_database('users')
        user_identities_collection = db['useridentities']

        user_metadata = list(user_identities_collection.find({"userId": str(user_id)}, {"__v": 0, "userId": 0, "_id": 0}))

        if user_metadata:
            logger.info(f'User metadata found')
            return user_metadata
        
        # If no useridentity is found, we return an empty list of dictionaries.
        return [{}]
    except Exception as e:
        logger.error(f"Error getting user metadata: {e}")
        raise e

def get_organization_name(organization_id):
    "Get organization name by id"
    try:
        logger.info(f'Getting organization name for organization_id {organization_id}')
        if organization_id:
            if re.fullmatch(r"[0-9a-fA-F]{24}", organization_id):
                db = mongo_client.get_database('organizations')
                organization_collection = db['organizations']

                pipeline = [
                    {"$match": {"_id": ObjectId(organization_id)}},
                    {"$project": {"_id": 0, "name": 1}}
                ]

                result = list(organization_collection.aggregate(pipeline))

                if result:
                    logger.info(f'Organization found')
                    return result[0]["name"]
            # Some of the persisted users don't contain 24-character hex in the organization_id
            return organization_id
        # Some of the persisted users don't have organization_id
        return ""
    except Exception as e:
        logger.error(f"Error getting user metadata: {e}")
        raise e
    
def get_tasks_by_campaign_id(campaign_id):
    "Get all campaign tasks"
    try:
        logger.info(f'Getting tasks for campaign {campaign_id}')
        db = mongo_client.get_database('campaigns')
        tasks_collection = db['tasks']

        pipeline = [
            {
                "$match": {
                    "campaignId": campaign_id
                }
            },
            {
                "$project": {
                    "campaignId": 0,
                    "organizationId": 0,
                    "adminId": 0,
                    "createdAt": 0,
                    "updatedAt": 0,
                    "__v": 0
                }
            }
        ]

        tasks = list(tasks_collection.aggregate(pipeline))

        if tasks:
            logger.info(f'Tasks found for campaign {campaign_id}')
        else:
            logger.info(f'No tasks found for campaign {campaign_id}')

        return tasks
    except Exception as e:
        logger.error(f"Error getting tasks for campaign {campaign_id}: {e}")
        raise e
    
def get_campaign_by_id(campaign_id):
    "Get campaign by id"
    try:
        logger.info('Getting campaign')
        db = mongo_client.get_database('campaigns')
        campaigns_collection = db['campaigns']

        campaign = list(campaigns_collection.find({"campaignId": campaign_id}, {"overview.adminId": 0, "overview.vaId": 0, "contractId": 0, "createdAt": 0, "updatedAt": 0, "__v": 0}))
        
        return campaign
    except Exception as e:
        logger.error(f"Error getting user metadata: {e}")
        raise e
    
def get_user_by_id(user_id):
    "Get user by id"
    try:
        db = mongo_client.get_database('users')
        user_collection = db["users"]

        logger.info(f"Searching for user with id: {user_id}.")

        user = user_collection.find_one({"_id": ObjectId(user_id)}, {"profilePicture": 0, "active": 0, "createdAt": 0, "updatedAt": 0, "__v": 0, "userType": 0, "userId": 0})

        return user
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        raise e