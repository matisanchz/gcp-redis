import logging
import settings
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_redis.vectorstores import RedisVectorStore
from langchain_redis.config import RedisConfig
from redisvl.query.filter import FilterExpression

logger = logging.getLogger(__name__)

embeddings = GoogleGenerativeAIEmbeddings(google_api_key = settings.GEMINI_API_KEY, model = settings.GEMINI_EMBEDDING_MODEL)

athlete_config = RedisConfig(
    index_name = settings.REDIS_ATHLETE_INDEX,
    key_prefix = settings.REDIS_ATHLETE_INDEX,
    redis_url=settings.REDIS_URL,
    metadata_schema=settings.ATHLETE_METADATA_CONFIG
)

athlete_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=athlete_config
)

campaign_config = RedisConfig(
    index_name = settings.REDIS_CAMPAIGN_INDEX,
    key_prefix = settings.REDIS_CAMPAIGN_INDEX,
    redis_url=settings.REDIS_URL,
    metadata_schema=settings.CAMPAIGN_METADATA_CONFIG
)

campaign_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=campaign_config
)

subtask_config = RedisConfig(
    index_name = settings.REDIS_SUBTASK_INDEX,
    key_prefix = settings.REDIS_SUBTASK_INDEX,
    redis_url=settings.REDIS_URL,
    metadata_schema=settings.SUBTASK_METADATA_CONFIG
)

subtask_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=subtask_config
)

def insert_user_documents(documents):
    # The id for each document, will be the user_id
    ids = [d.metadata.get('user_id') for d in documents]

    athlete_vectorstore.add_documents(
        documents=documents,
        ids=ids
    )

    logger.info(f"Inserted {len(documents)} documents for users: {ids}")

def insert_campaign_documents(documents):

    ids = [d.metadata.get('campaign_id') for d in documents]
        
    campaign_vectorstore.add_documents(
        documents=documents,
        ids=ids
    )
    logger.info(f"Inserted {len(documents)} documents for campaigns: {ids}")

def insert_athlete_subtask_documents(documents):
    ids = [d.metadata.get('user_id') for d in documents]

    subtask_vectorstore.add_documents(
        documents=documents,
        ids=ids
    )
    logger.info(f"Inserted {len(documents)} subtasks documents for users: {ids}")

def delete_user_document(user_id):
    # Delete User Index
    deleted = athlete_vectorstore.delete(
        ids=[user_id]
    )
    if deleted:
        logger.info(f"Deleted document for user_id: {user_id}")
    else:
        logger.info(f"Error deleting document for user_id: {user_id}")

def delete_campaign_document(campaign_id):
    # Delete Campaign Index
    deleted = campaign_vectorstore.delete(
        ids=[campaign_id]
    )
    if deleted:
        logger.info(f"Deleted document for campaign_id: {campaign_id}")
    else:
        logger.info(f"Error deleting document for campaign_id: {campaign_id}")

def delete_athlete_subtask_document(user_id):
    # Delete Athlete Subtask Index
    deleted = subtask_vectorstore.delete(
        ids=[user_id]
    )
    if deleted:
        logger.info(f"Deleted subtask document for user_id: {user_id}")
    else:
        logger.info(f"Error deleting subtask document for user_id: {user_id}")
    
def get_existing_user_document_by_field(field_name, value):
    expression = FilterExpression(f"@{field_name}:{{{value}}}")
    
    document = athlete_vectorstore.similarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None
    
def get_existing_campaign_document(campaign_id):
    expression = FilterExpression(f"@campaign_id:{{{campaign_id}}}")
    
    document = campaign_vectorstore.similarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None
    
def get_existing_athlete_subtask_document_by_field(field_name, value):
    expression = FilterExpression(f"@{field_name}:{{{value}}}")
    
    document = subtask_vectorstore.similarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None