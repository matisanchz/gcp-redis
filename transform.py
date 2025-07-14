import logging
from langchain.schema import Document
from redisvl.query.filter import FilterExpression
from extract import *
from load import get_existing_user_document, get_existing_campaign_document, get_existing_athlete_subtask_document

logger = logging.getLogger(__name__)

async def get_user_document(user):
    user_metadata = await get_user_metadata(user["_id"])

    organization = await get_organization_name(user["organizationId"])

    # Organize information with labels
    
    # Label 1 -> basicInfo
    content = "<basicInfo>\n"
    for key, value in user.items():
        if key not in ["_id", "organizationId"]:
            content += f"{key}: {value}.\n"
    content += "</basicInfo>\n"

    # Label 2 -> extraInfo
    content += "\n<extraInfo>\n"
    for key, value in user_metadata[0].items():
        content += f"{key}: {value}.\n"
    content += "</extraInfo>\n"

    # Label 3 -> organizationName
    content += "\n<organizationName>\n"
    content += organization
    content += "\n</organizationName>"

    doc = Document(
        page_content=content,
        metadata={
            "type": "athlete_profile",
            "user_id": str(user["_id"]),
            "organization_id": str(user["organizationId"])
        }
    )

    return doc

async def get_updated_user_document(user):
    old_document = get_existing_user_document(str(user["_id"]))

    pattern = r'(<basicInfo>)(.*?)(</basicInfo>)'

    new_content = ""

    for key, value in user.items():
        if key not in ["_id", "organizationId"]:
            new_content += f"{key}: {value}.\n"

    updated_data = re.sub(
        pattern,
        lambda m: f"{m.group(1)}\n{new_content}\n{m.group(3)}",
        old_document.page_content,
        flags=re.DOTALL
    )

    doc = Document(
        page_content=updated_data,
        metadata={
            "type": old_document.metadata["type"],
            "user_id": old_document.metadata["user_id"],
            "organization_id": str(user["organizationId"])
        }
    )

    return doc

async def get_updated_useridentities_document(useridentity):
    old_document = get_existing_user_document(str(useridentity["userId"]))

    pattern = r'(<extraInfo>)(.*?)(</extraInfo>)'

    new_content = ""

    for key, value in useridentity.items():
        if key not in ["__v", "userId", "_id"]:
            new_content += f"{key}: {value}.\n"


    updated_data = re.sub(
        pattern,
        lambda m: f"{m.group(1)}\n{new_content}\n{m.group(3)}",
        old_document.page_content,
        flags=re.DOTALL
    )

    doc = Document(
        page_content=updated_data,
        metadata={
            "type": old_document.metadata["type"],
            "user_id": old_document.metadata["user_id"],
            "organization_id": old_document.metadata["organization_id"]
        }
    )

    return doc

async def get_campaign_document(campaign):
    tasks = await get_tasks_by_campaign_id(campaign["_id"])
    campaign_id = str(campaign["_id"])
    content = "<campaignData>\n"
    for key, value in campaign.items():
        if key not in ["_id"]:
            content += f"{key}: {value}.\n"
    content = "\n</campaignData>\n"
    content = "\n<taskData>\n"
    for task in tasks:
        content += f"\nTask:"
        for key, value in task.items():
            content += f"{key}: {value}. "
    content = "\n</taskData>\n"

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

async def get_updated_campaign_document(campaign):
    old_document = get_existing_campaign_document(str(campaign["_id"]))

    pattern = r'(<campaignData>)(.*?)(</campaignData>)'

    new_content = ""

    for key, value in campaign.items():
        if key not in ["_id"]:
            new_content += f"{key}: {value}.\n"


    updated_data = re.sub(
        pattern,
        lambda m: f"{m.group(1)}\n{new_content}\n{m.group(3)}",
        old_document.page_content,
        flags=re.DOTALL
    )

    doc = Document(
        page_content=updated_data,
        metadata={
            "type": old_document.metadata["type"],
            "campaign_id": old_document.metadata["campaign_id"],
            "organization_id": str(campaign["organizationId"]),
            "user_id": [str(a_id) for a_id in campaign["overview"]["athletes"]],
            "brand": campaign["overview"]["brand"]
        }
    )
    return doc

async def get_updated_task_document(task, insert: bool = False):
    old_document = get_existing_campaign_document(str(task["campaignId"]))

    pattern = r'(<taskData>)(.*?)(</taskData>)'

    new_content = ""

    if insert:
        match = re.search(pattern, old_document.page_content, re.DOTALL)
        if match:
            new_content = match.group(1).strip()
        else:
            new_content = ""
        new_content += f"\nTask:"
        for key, value in task.items():
            new_content += f"{key}: {value}. "
    else:
        tasks = await get_tasks_by_campaign_id(str(task["campaignId"]))

        for t in tasks:
            new_content += f"\nTask:"
            for key, value in t.items():
                new_conew_contentntent += f"{key}: {value}. "
         
    updated_data = re.sub(
        pattern,
        lambda m: f"{m.group(1)}\n{new_content}\n{m.group(3)}",
        old_document.page_content,
        flags=re.DOTALL
    )

    doc = Document(
        page_content=updated_data,
        metadata={
            "type": old_document.metadata["type"],
            "campaign_id": old_document.metadata["campaign_id"],
            "organization_id": old_document.metadata["organizationId"],
            "user_id": old_document.metadata["user_id"],
            "brand": old_document.metadata["brand"]
        }
    )
    return doc

async def get_athlete_subtask_document(user):
    subtasks = await get_subtasks_by_user_id(str(user["_id"]))
    if subtasks:
        content = ""
        for subtask in subtasks:
            content += "Subtask: \n"
            for key, value in subtask.items():
                content += f"{key}: {value}.\n"
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

async def get_updated_subtask_document(subtask, insert: bool = False):
    old_document = get_existing_athlete_subtask_document(str(subtask["athleteId"]))

    new_content = ""

    if insert:
        new_content += old_document.page_content
        new_content += "Subtask: \n"
        for key, value in subtask.items():
            new_content += f"{key}: {value}.\n"
        new_content += "\n"
    else:
        subtasks = await get_subtasks_by_user_id(str(subtask["athleteId"]))
        if subtasks:
            for subtask in subtasks:
                new_content += "Subtask: \n"
                for key, value in subtask.items():
                    new_content += f"{key}: {value}.\n"
                new_content += "\n"

    doc = Document(
        page_content=new_content,
        metadata={
            "type": old_document.metadata["type"],
            "user_id": old_document.metadata["user_id"],
            "organization_id": old_document.metadata["organizationId"]
        }
    )
    return doc