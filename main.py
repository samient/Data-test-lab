import os
import logging
from google.cloud import storage
from google.cloud import dataflow_v1beta3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trigger_dataflow(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    
    # Only process CSV files in the input directory
    if not (file_name.startswith('input/') and file_name.endswith('.csv')):
        logger.info(f"Skipping non-CSV file or file not in input directory: {file_name}")
        return
    
    logger.info(f"Processing file: {file_name} from bucket: {bucket_name}")
    
    # Set up Dataflow client
    client = dataflow_v1beta3.JobsV1Beta3Client()
    
    project_id = os.environ.get('GCP_PROJECT', 'test-data-462007')
    region = os.environ.get('GCP_REGION', 'us-central1')
    template_path = f"gs://{bucket_name}/templates/dataflow_template"
    input_path = f"gs://{bucket_name}/{file_name}"
    output_path = f"gs://{bucket_name}/output/{os.path.basename(file_name)}"
    
    # Environment configuration
    environment = dataflow_v1beta3.Environment(
        temp_location=f"gs://{bucket_name}/temp",
        service_account_email="dataflow-service-account@test-data-462007.iam.gserviceaccount.com",
    )
    
    # Job parameters
    job_params = {
        "input": input_path,
        "output": output_path,
        "project": test-data-462007,
        "region": us-central1
    }
    
    # Launch the Dataflow job
    request = dataflow_v1beta3.LaunchTemplateParameters(
        job_name=f"sales-processing-{os.path.basename(file_name).replace('.', '-')}",
        parameters=job_params,
        environment=environment
    )
    
    response = client.launch_template(
        project_id=test-data-462007,
        location=us-central1,
        gcs_path=template_path,
        launch_parameters=request
    )
    
    logger.info(f"Launched Dataflow job: {response.job.id}")

if __name__ == '__main__':
    # For local testing
    test_event = {
        'bucket': 'dataflow_demo_001',
        'name': 'input/sales.csv'
    }
    trigger_dataflow(test_event, None)