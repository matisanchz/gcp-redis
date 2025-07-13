import logging
import settings
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_redis.vectorstores import RedisVectorStore

logger = logging.getLogger(__name__)

embeddings = GoogleGenerativeAIEmbeddings(google_api_key = settings.GEMINI_API_KEY, model = settings.GEMINI_EMBEDDING_MODEL)

athlete_vectorstore = RedisVectorStore(
        embeddings=embeddings,
        redis_url=settings.REDIS_URL,
        index_name=settings.REDIS_ATHLETE_INDEX
)

campaign_vectorstore = RedisVectorStore(
        embeddings=embeddings,
        redis_url=settings.REDIS_URL,
        index_name=settings.REDIS_CAMPAIGN_INDEX
)

subtask_vectorstore = RedisVectorStore(
        embeddings=embeddings,
        redis_url=settings.REDIS_URL,
        index_name=settings.REDIS_SUBTASK_INDEX
)

async def insert_user_documents(documents):

    await athlete_vectorstore.aadd_documents(
        documents=documents
    )
    logger.info(f"Inserted {len(documents)} documents for users: {[d.metadata.get('user_id') for d in documents]}")

async def insert_campaign_documents(documents):
        
    await campaign_vectorstore.aadd_documents(
        documents=documents
    )
    logger.info(f"Inserted {len(documents)} documents for campaigns: {[d.metadata.get('campaign_id') for d in documents]}")

async def insert_athlete_subtask_documents(documents):
    await subtask_vectorstore.aadd_documents(
        documents=documents
    )
    logger.info(f"Inserted {len(documents)} subtasks documents for users: {[d.metadata.get('user_id') for d in documents]}")

async def delete_user_document(user_id):
    # Delete User Index
    await athlete_vectorstore.adelete(
        ids=None,
        filter={"user_id": user_id}
    )
    logger.info(f"Deleted document for user_id: {user_id}")

async def delete_campaign_document(campaign_id):
    # Delete Campaign Index
    await campaign_vectorstore.adelete(
        ids=None,
        filter={"campaign_id": campaign_id}
    )

async def delete_athlete_subtask_document(user_id):
    # Delete Athlete Subtask Index
    await subtask_vectorstore.adelete(
        ids=None,
        filter={"user_id": user_id}
    )