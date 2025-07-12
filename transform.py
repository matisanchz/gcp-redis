import logging
from langchain.schema import Document
from extract import *

logger = logging.getLogger(__name__)

async def get_user_document(user):
    user_metadata = await get_user_metadata(user["_id"])

    organization = await get_organization_name(user["organizationId"])
    
    # Concat all user information
    user_data = user | user_metadata[0] | organization[0]

    # Delete unnecesary fields
    user_data.pop("_id")
    user_data.pop("organizationId")

    content = ""

    for key, value in user_data.items():
        content += f"{key}: {value}. "

    doc = Document(
        page_content=content,
        metadata={
            "type": "athlete_profile",
            "user_id": str(user["_id"]),
            "organization_id": str(user["organizationId"])
        }
    )

    return doc

async def get_campaign_document(campaign):
    tasks = await get_tasks_by_campaign_id(campaign["_id"])
    campaign_id = str(campaign["_id"])
    campaign.pop("_id")
    content = "Campaign data: \n"
    for key, value in campaign.items():
        content += f"{key}: {value}. "
    content += "/n Tasks information:"
    i = 1
    for task in tasks:
        content += f"\nTask {i}:"
        for key, value in task.items():
            content += f"{key}: {value}. "
        i+=1

    doc = Document(
        page_content=content,
        metadata={
            "type": "campaign_summary",
            "campaign_id": campaign_id,
            "organization_id": str(campaign["organizationId"]),
            "user_id": [str(a_id) for a_id in campaign["overview"]["athletes"]],
            "brand": campaign["overview"]["brand"]
        }
    )
    return doc

async def get_athlete_subtask_document(user):
    subtasks = await get_subtasks_by_user_id(user["_id"])
    if subtasks:
        content = ""
        for subtask in subtasks:
            content += "User tasks data: \n"
            for key, value in subtask.items():
                content += f"{key}: {value}. "
            content += "\n"

        doc = Document(
            page_content=content,
            metadata={
                "type": "athlete_subtask_summary",
                "user_id": str(user["_id"]),
                "organization_id": str(subtasks[0]["organizationId"]),
            }
        )
    
        return doc
    return None