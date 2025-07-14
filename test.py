import logging
import settings
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_redis.vectorstores import RedisVectorStore
from langchain_redis.config import RedisConfig
from redisvl.schema import IndexSchema

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

from redisvl.query.filter import FilterExpression

organization_id_to_find = "675a4fc6c3c708b7478d87f8"
expression = FilterExpression(f"@user_id:{{{organization_id_to_find}}}")

print(f"RedisVL Filter Expression created for organization_id: {expression}")
print(f"Type: {type(expression)}")

found_document = athlete_vectorstore.similarity_search(
    query="",
    k=1,
    filter=expression
)

content = found_document[0]



"""ids_delete = ['675a1b0ff2cc6b08fd2a239d']

delete = athlete_vectorstore.delete(
        ids=ids_delete
    )

print(f"DELETED {delete}")"""
