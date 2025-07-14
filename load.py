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
    metadata_schema=settings.athlete_metadata_config
)

athlete_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=athlete_config
)

campaign_config = RedisConfig(
    index_name = settings.REDIS_CAMPAIGN_INDEX,
    key_prefix = settings.REDIS_CAMPAIGN_INDEX,
    redis_url=settings.REDIS_URL,
    metadata_schema=settings.campaign_metadata_config
)

campaign_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=campaign_config
)

subtask_config = RedisConfig(
    index_name = settings.REDIS_SUBTASK_INDEX,
    key_prefix = settings.REDIS_SUBTASK_INDEX,
    redis_url=settings.REDIS_URL,
    metadata_schema=settings.subtask_metadata_config
)

subtask_vectorstore = RedisVectorStore(
    embeddings=embeddings,
    config=campaign_config
)

async def insert_user_documents(documents):
    # The id for each document, will be the user_id
    ids = [d.metadata.get('user_id') for d in documents]

    await athlete_vectorstore.aadd_documents(
        documents=documents,
        ids=ids
    )

    logger.info(f"Inserted {len(documents)} documents for users: {ids}")

async def insert_campaign_documents(documents):

    ids = [d.metadata.get('campaign_id') for d in documents]
        
    await campaign_vectorstore.aadd_documents(
        documents=documents
    )
    logger.info(f"Inserted {len(documents)} documents for campaigns: {ids}")

async def insert_athlete_subtask_documents(documents):
    ids = {[d.metadata.get('user_id') for d in documents]}

    await subtask_vectorstore.aadd_documents(
        documents=documents
    )
    logger.info(f"Inserted {len(documents)} subtasks documents for users: {ids}")

async def delete_user_document(user_id):
    # Delete User Index
    deleted = await athlete_vectorstore.adelete(
        ids=[user_id]
    )
    if deleted:
        logger.info(f"Deleted document for user_id: {user_id}")
    else:
        logger.info(f"Error deleting document for user_id: {user_id}")

async def delete_campaign_document(campaign_id):
    # Delete Campaign Index
    deleted = await campaign_vectorstore.adelete(
        ids=[campaign_id]
    )
    if deleted:
        logger.info(f"Deleted document for campaign_id: {campaign_id}")
    else:
        logger.info(f"Error deleting document for campaign_id: {campaign_id}")

async def delete_athlete_subtask_document(user_id):
    # Delete Athlete Subtask Index
    deleted = await subtask_vectorstore.adelete(
        ids=[user_id]
    )
    if deleted:
        logger.info(f"Deleted subtask document for user_id: {user_id}")
    else:
        logger.info(f"Error deleting subtask document for user_id: {user_id}")

async def get_existing_user_document(user_id):
    expression = FilterExpression(f"@user_id:{{{user_id}}}")
    
    document = await athlete_vectorstore.asimilarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None
    
async def get_existing_campaign_document(campaign_id):
    expression = FilterExpression(f"@campaign_id:{{{campaign_id}}}")
    
    document = await campaign_vectorstore.asimilarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None
    
async def get_existing_athlete_subtask_document(user_id):
    expression = FilterExpression(f"@user_id:{{{user_id}}}")
    
    document = await subtask_vectorstore.asimilarity_search(
        query="",
        k=1,
        filter=expression
    )

    if document:
        return document[0]
    else:
        return None