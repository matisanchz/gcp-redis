import logging
from langchain.schema import Document
from extract import *
from load import get_existing_user_document_by_field, get_existing_campaign_document, get_existing_athlete_subtask_document_by_field

logger = logging.getLogger(__name__)

def get_user_document(user):
    user_metadata = get_user_metadata(user["_id"])

    organization = get_organization_name(user["organizationId"])

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

def get_updated_user_document(user):
    old_document = get_existing_user_document_by_field("user_id", str(user["_id"]))

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

def get_updated_useridentities_document(useridentity):
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

def get_updated_user_organization_document(_id, name):
    old_document = get_existing_user_document_by_field("organization_id", _id)

    pattern = r'(<organizationName>)(.*?)(</organizationName>)'

    new_content = name

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

def get_campaign_document(campaign):
    tasks = get_tasks_by_campaign_id(campaign["_id"])
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

def get_updated_campaign_document(campaign):
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

def get_updated_task_document(task, insert: bool = False):
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
        tasks = get_tasks_by_campaign_id(str(task["campaignId"]))

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

def get_athlete_subtask_document(user):
    subtasks = get_subtasks_by_user_id(user["_id"])
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

def get_updated_subtask_document(subtask, insert: bool = False):
    field_name = None
    value = None

    if "athleteId" in subtask:
        field_name = "user_id"
        value = str(subtask["athleteId"])
    else:
        field_name = "organization_id"
        value = str(subtask["organizationId"])
    
    old_document = get_existing_athlete_subtask_document_by_field(field_name, value)

    new_content = ""

    if insert:
        new_content += old_document.page_content
        new_content += "Subtask: \n"
        for key, value in subtask.items():
            if key not in ["_id", "createdAt", "updatedAt", "__v"]:
                new_content += f"{key}: {value}.\n"
        new_content += "\n"
    else:
        subtasks = get_subtasks_by_user_id(subtask["athleteId"])
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
            "user_id": str(subtask["athleteId"]) if "athleteId" in subtask else old_document.metadata["user_id"],
            "organization_id": str(subtask["organizationId"]) if "organizationId" in subtask else old_document.metadata["organizationId"]
        }
    )
    return doc

def get_updated_subtask_organization_document(user_id):
    old_document = get_existing_athlete_subtask_document(user_id)

    doc = Document(
        page_content=old_document.page_content,
        metadata={
            "type": old_document.metadata["type"],
            "user_id": old_document.metadata["user_id"],
            "organization_id": str(user_id["organizationId"])
        }
    )
    return doc