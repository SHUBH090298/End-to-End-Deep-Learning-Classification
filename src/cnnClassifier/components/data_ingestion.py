import os
import zipfile
import gdown
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
from cnnClassifier.entity.config_entity import (DataIngestionConfig)
class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config


    
     
    def download_file(self)-> str:
        '''
        Fetch data from the url
        '''

        try: 
            dataset_url = self.config.source_URL
            zip_download_dir = self.config.local_data_file
            os.makedirs(os.path.dirname(zip_download_dir), exist_ok=True)
            logger.info(f"Downloading data from {dataset_url} into file {zip_download_dir}")

            file_id = dataset_url.split("/")[-2]
            prefix = 'https://drive.google.com/uc?/export=download&id='
            gdown.download(prefix+file_id,zip_download_dir)

            logger.info(f"Downloaded data from {dataset_url} into file {zip_download_dir}")

        except Exception as e:
            raise e
        
    

    def extract_zip_file(self):
        """
        Extracts the ZIP file into the configured unzip directory.
        """
        try:
            unzip_path = self.config.unzip_dir  # e.g., "artifacts/data_ingestion/Chest-CT-Scan-data"
            os.makedirs(unzip_path, exist_ok=True)

            logger.info(f"Extracting ZIP file to {unzip_path}")

            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            # Check if the ZIP contains a single folder and flatten it
                root_folders = set(
                    os.path.normpath(name).split(os.sep)[0] for name in zip_ref.namelist()
                )

                if len(root_folders) == 1:
                # Single folder in ZIP: Extract contents directly to the target folder
                    temp_extract_path = os.path.join(unzip_path, "__temp__")
                    zip_ref.extractall(temp_extract_path)

                # Move contents of the temp folder to the target directory
                    for item in os.listdir(temp_extract_path):
                        os.rename(
                            os.path.join(temp_extract_path, item),
                            os.path.join(unzip_path, item)
                        )
                    os.rmdir(temp_extract_path)  # Remove the temp folder
                else:
                    # Extract files directly
                    zip_ref.extractall(unzip_path)

            logger.info(f"Extraction complete. Files are available in {unzip_path}")

        except Exception as e:
            raise e
