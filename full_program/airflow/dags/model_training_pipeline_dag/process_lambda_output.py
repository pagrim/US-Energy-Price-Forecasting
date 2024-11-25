''' Import modules '''
from airflow.exceptions import AirflowException

def process_lambda_output(*args, **kwargs):
    '''Function that processes output from natural_gas_spot_prices_transformation_trigger '''
    ti = kwargs['ti']
    
    # Pull the Lambda response from Xcom
    lambda_response = ti.xcom_pull(task_ids='invoke_lambda_function')
    status_code = lambda_response.get('status_code')
    
    if status_code == 200:
        return 'merge_dataframes'  # Return the task ID of the next task to execute
    elif status_code == 400:
        raise AirflowException('Transformed natural gas spot prices do not exist for today. DAG will terminate.')
    else:
        print(f'Unexpected status code: {status_code}')
        raise AirflowException('Unexpected status code from Lambda.')
