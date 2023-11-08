from abc import ABC, abstractmethod
from src.utils.setting_logger import Logger

logger = Logger(__name__).get_logger()


class PipelineStepABC(ABC):
    def __init__(self):
        self.df = None
        self.df_out = None

    @abstractmethod
    def execute_step(self):
        """Perform main action of the transformation step"""
        pass

    @abstractmethod
    def load_previous_step_data(self):
        pass

#    @abstractmethod
#    def upload_results_to_db(self):
#        pass

    def save_results(self, file_name):
        logger.info(f"Saving data table as {file_name}")
        self.df_out.to_csv(file_name)
