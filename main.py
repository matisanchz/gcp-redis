import functions_framework
import logging
import threading
import settings
import asyncio
from requests import Request
from etl import etl
from change_streams import process_change

logger = logging.getLogger(__name__)

mongo_client = settings.MONGO_CLIENT

db_watcher_threads = []

@functions_framework.http
def main(request: Request):
    """
    HTTP Cloud Function to execure the ETL Redis process.
    """
    try:
        # WARNING: ONLY EXECUTE ONCE
        if request.path == "/redis-etl":
            logger.info("Starting Redis ETL process from main function...")
            try:
                etl_thread = threading.Thread(
                    target=run_async_in_thread,
                    args=(etl(),),
                    daemon=True
                )
                etl_thread.start()
                return {"message": "ETL process started."}, 200
            except Exception as e:
                logger.error(f"Error during ETL process: {e}")
                return {"error": f"ETL process failed: {e}"}, 500
        elif request.path == "/change-streams":
            try:
                global db_watcher_threads

                if not db_watcher_threads:
                    logger.info("Initializing MongoDB Change Stream listeners...")
                    for db_name in settings.DATABASES:
                        thread_name = f"MongoDB_Watcher_{db_name}"
                        thread = threading.Thread(
                            target=run_async_in_thread,
                            args=(watch_single_database(db_name),),
                            name=thread_name,
                            daemon=True
                        )
                        thread.start()
                        db_watcher_threads.append(thread)
                        logger.info(f"Started thread for database: {db_name}")
                logger.info("All database listening threads started.")
                return "Background MongoDB Change Stream listeners initialized.", 200
            except Exception as e:
                logger.error(f"An unexpected error occurred in Cloud Function: {e}")
                return {"error": f"Change Stream process failed: {e}"}, 500
        return "Invalid endpoint", 404
    except Exception as e:
        logger.error(f"An unexpected error occurred in Cloud Function: {e}")
        return {"error": str(e)}, 500

def run_async_in_thread(coro):
    """Helper to run an async coroutine in a new event loop within a thread."""
    asyncio.run(coro)

async def watch_single_database(db_name):
    logger.info(f"Start watching collections for db {db_name}...")
    try:
        db = mongo_client.get_database(db_name)
        with db.watch(full_document='updateLookup', full_document_before_change="required") as stream:
            asyncio.get_running_loop()
            for change in stream:
                await process_change(change)
        logger.info(f"Ending {db_name} listening...")
        return {"error": "The change stream listener for db {db_name} closed"}, 500
    except Exception as e:
        logger.error(f"Error during Change Stream listening for db {db_name}: {e}")
        raise e