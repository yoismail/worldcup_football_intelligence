from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ClientAuthenticationError
import logging
from elt.logger import setup_logging, section, timed
import os
from typing import Optional


# Load environment variables
load_dotenv()
CONNECTION_STRING: Optional[str] = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME: Optional[str] = os.getenv("CONTAINER_NAME")
BRONZE_PATH: Optional[str] = os.getenv("BRONZE_PATH")

# Validate config
if not CONNECTION_STRING:
    raise ValueError("Missing environment variable: AZURE_CONNECTION_STRING")
if not CONTAINER_NAME:
    raise ValueError("Missing environment variable: CONTAINER_NAME")
if not BRONZE_PATH or not os.path.isdir(BRONZE_PATH):
    raise NotADirectoryError(
        f"BRONZE_PATH is invalid or does not exist: {BRONZE_PATH}")


def get_container_client():
    """Get a fresh container client instance."""
    try:
        blob_service = BlobServiceClient.from_connection_string(
            CONNECTION_STRING)
        return blob_service.get_container_client(CONTAINER_NAME)
    except ClientAuthenticationError:
        logging.error("Authentication failed — check your connection string.")
        raise


def load_to_azure(file_path: str, blob_name: str) -> bool:
    """Upload a single file to Azure Blob Storage."""
    container_client = get_container_client()
    try:
        if not os.path.isfile(file_path):
            logging.error(f"Not a valid file: {file_path}")
            return False

        file_size = os.path.getsize(file_path)
        with open(file_path, "rb") as data:
            container_client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True,
                blob_type="BlockBlob",
                max_concurrency=4,
                length=file_size
            )

        logging.info(f"Uploaded: {file_path} → {blob_name}")
        return True

    except PermissionError:
        logging.error(f"Permission denied when reading: {file_path}")
    except ClientAuthenticationError:
        logging.error("Authentication failed for Azure Blob Storage.")
    except AzureError as ae:
        logging.error(
            f"Azure error uploading {file_path}: {ae}", exc_info=True)
    except Exception as e:
        logging.error(
            f"Unexpected error uploading {file_path}: {e}", exc_info=True)

    return False


def load_bronze_data_to_azure() -> None:
    """Upload all files from BRONZE_PATH, including all subfolders."""
    try:
        # os.walk goes through every folder and subfolder
        # We don't use 'dirs' here since we only iterate over files

        for root, dirs, files in os.walk(BRONZE_PATH):
            # root = full path of current folder
            # dirs = list of subfolders in that folder
            # files = list of files in that folder
            for file_name in files:
                full_file_path = os.path.join(root, file_name)

                # Create blob path matching your folder structure
                # Remove the base BRONZE_PATH part to keep only relative path
                relative_path = os.path.relpath(full_file_path, BRONZE_PATH)
                # Convert Windows backslashes to Azure forward slashes
                blob_name = relative_path.replace("\\", "/")

                # Upload the file
                success = load_to_azure(full_file_path, blob_name)
                if success:
                    logging.info(f"Loaded: {blob_name}")
                else:
                    logging.warning(f"Failed: {blob_name}")

    except Exception as e:
        logging.error(f"Error loading bronze data: {e}", exc_info=True)


def main():
    """Main function to load bronze data to Azure."""
    setup_logging()
    section("Starting Bronze Data Load to Azure")
    timed(load_bronze_data_to_azure)()


# Run the function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error in main execution: {e}", exc_info=True)
    finally:
        logging.info(
            "=============Finished Bronze Data Load to Azure.=============")
