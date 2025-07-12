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

def insert_user_documents(documents):

    athlete_vectorstore.add_documents(
        documents=documents
    )

    logger.info(f"Inserted documents for users: {[d.metadata.get('user_id') for d in documents]}")

def insert_campaign_documents(documents):
        
    campaign_vectorstore.add_documents(
        documents=documents
    )
    logger.info(f"Inserted documents for campaigns: {[d.metadata.get('campaign_id') for d in documents]}")

def insert_athlete_subtask_document(document):
    subtask_vectorstore.add_documents(
        documents=[document]
    )
    logger.info(f"Inserted subtasks for user: {document.metadata.get('user_id')}")

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