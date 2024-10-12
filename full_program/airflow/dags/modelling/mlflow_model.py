''' Import modules '''
import mlflow

class MlflowModel:
    ''' Class for retrieving ml flow experiment and experiment id to 
    be used for logging purposes when training machine learning model '''
    def __init__(self, experiment_name: str, tracking_uri: str) -> None:
        self.experiment_name = experiment_name
        self.tracking_uri = tracking_uri
    
    def set_tracking_uri(self) -> None:
        ''' Sets mlflow tracking uri '''
        mlflow.set_tracking_uri(self.tracking_uri)
    
    def retrieve_experiment_id(self) -> str:
        ''' Retrieves experiment id for a given experiment '''
        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        experiment_id = experiment.experiment_id
        return experiment_id

