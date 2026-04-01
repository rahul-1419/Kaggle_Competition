import os
import gdown
from pathlib import Path
from urllib.error import URLError
from time import sleep
from etl import logger
from etl.entity.config_entity import DataExtractionConfig  
import zipfile

class DataExtraction:

    def __init__(self, config):
        self.config = config

    def download_file(self, retries: int = 3, delay: int = 5):
        try:
            file_path = Path(self.config.local_data_file)

            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            if not self.config.file_id:
                raise ValueError("file_id is missing in config")

            # Skip if already exists
            if file_path.exists() and file_path.stat().st_size > 0:
                logger.info(f"File already exists: {file_path}")
                return

            url = f"https://drive.google.com/uc?id={self.config.file_id}"

            for attempt in range(1, retries + 1):
                try:
                    logger.info(f"Downloading (Attempt {attempt}) from {url}")

                    gdown.download(url, str(file_path), quiet=False, fuzzy=True)

                    if not file_path.exists() or file_path.stat().st_size == 0:
                        raise Exception("Downloaded file is empty")

                    logger.info(f"Download successful: {file_path}")
                    break

                except Exception as e:
                    logger.error(f"Attempt {attempt} failed: {e}")

                    if attempt < retries:
                        sleep(delay)
                    else:
                        raise RuntimeError("Download failed after retries")

        except Exception as e:
            logger.exception(f"Download error: {e}")
            raise

    def extract_zip_file(self):
        try:
            zip_path = self.config.local_data_file
            unzip_path = self.config.unzip_dir

            os.makedirs(unzip_path, exist_ok=True)

            # Validate zip file
            if not zipfile.is_zipfile(zip_path):
                raise ValueError("Invalid ZIP file")

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)

            logger.info(f"Extraction completed at {unzip_path}")

        except Exception as e:
            logger.exception(f"Extraction error: {e}")
            raise