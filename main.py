import functions_framework
import logging
import settings
from requests import Request
from etl import etl
from change_streams import process_change

logger = logging.getLogger(__name__)

mongo_client = settings.MONGO_CLIENT

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
                etl()
                return {"message": "ETL process started."}, 200
            except Exception as e:
                logger.error(f"Error during ETL process: {e}")
                return {"error": f"ETL process failed: {e}"}, 500
        elif request.path == "/trigger":
            try:
                payload = request.get_json()

                logger.info(f"Received trigger payload: {payload}")

                operation_type = payload.get("operationType")

                document = payload.get("document")

                process_change(operation_type, document)
                return {"message": "Trigger successfully executed."}, 200
            except Exception as e:
                logger.error(f"An unexpected error occurred in Cloud Function: {e}")
                return {"error": f"Trigger process failed: {e}"}, 500
        return {"error": "Invalid endpoint"}, 404
    except Exception as e:
        logger.error(f"An unexpected error occurred in Cloud Function: {e}")
        return {"error": str(e)}, 500