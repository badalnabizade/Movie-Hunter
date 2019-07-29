import time, pymysql
from pymysql.err import OperationalError
pymysql.install_as_MySQLdb()
from os import getenv
from google.cloud import dataproc_v1beta2
from google.cloud.dataproc_v1beta2.gapic.transports import (
    cluster_controller_grpc_transport)
from google.cloud.dataproc_v1beta2.gapic.transports import (
    job_controller_grpc_transport)

from google.oauth2 import service_account
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration

PROJECT_ID = getenv('PROJECT_ID')
CONNECTION_NAME = getenv('CloudSQL_Connection_Name')
DB_USER = getenv('CloudSQL_User')
DB_PASSWORD = getenv('CloudSQL_Pass')
DB_NAME = getenv('CloudSQL_DB_Name')
CLOUDSQL_INSTANCE_IP = getenv('CLOUDSQL_INSTANCE_IP')

mysql_config = {
  'host': CLOUDSQL_INSTANCE_IP,
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'cursorclass': pymysql.cursors.DictCursor,
  'autocommit': True
}


def __get_cursor():
    """
    Helper function to get a cursor
      PyMySQL does NOT automatically reconnect,
      so we must reconnect explicitly using ping()
    """
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()


try:
    mysql_conn = pymysql.connect(**mysql_config)
except OperationalError:
    # If production settings fail, use local development ones
    mysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
    mysql_conn = pymysql.connect(**mysql_config)


json_string = getenv('CREDENTIALS_JSON')
credentials = service_account.Credentials.from_service_account_info(json_string)
waiting_callback = False


class DataprocManager:
    def __init__(self, project_id, cluster_name, bucket_name):
        self.credentials = credentials
        self.zone = 'europe-west6-b'
        self.project_id = project_id
        self.cluster_name = cluster_name
        self.bucket_name = bucket_name
        self.region = self._get_region_from_zone()
        # Use a regional gRPC endpoint. See:
        # https://cloud.google.com/dataproc/docs/concepts/regional-endpoints
        self.client_transport = \
            cluster_controller_grpc_transport.ClusterControllerGrpcTransport(credentials=self.credentials,
                                                                             address='{}-dataproc.googleapis.com:443' \
                                                                             .format(self.region))

        self.job_transport = \
            job_controller_grpc_transport.JobControllerGrpcTransport(credentials=self.credentials,
                                                                     address='{}-dataproc.googleapis.com:443'.format(
                                                                         self.region))

        self.dataproc_cluster_client = dataproc_v1beta2.ClusterControllerClient(
            self.client_transport)

        self.dataproc_job_client = dataproc_v1beta2.JobControllerClient(self.job_transport)

    def _get_region_from_zone(self):
        try:
            region_as_list = self.zone.split('-')[:-1]
            return '-'.join(region_as_list)
        except (AttributeError, IndexError, ValueError):
            raise ValueError('Invalid zone provided, please check your input.')

    def create_cluster(self):
        """Create the cluster."""
        print('Creating cluster...')

        # idle_delete_ttl only accepts google.protobuf.duration d-type as a duration
        start = Timestamp()
        end = Timestamp()
        duration = Duration()
        start.FromJsonString('2019-06-01T10:00:20.021-05:00')
        end.FromJsonString('2019-06-01T10:10:20.021-05:00')
        duration.seconds = end.seconds - start.seconds  # duration will be 10 minute.
        zone_uri = \
            'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                self.project_id, self.zone)

        cluster_data = {
            'project_id': self.project_id,
            'cluster_name': self.cluster_name,
            'config': {
                'gce_cluster_config': {
                    'zone_uri': zone_uri,
                    "metadata": {
                        'PIP_PACKAGES': 'pandas requests beautifulsoup4 PyMySQL'
                    }
                },
                'master_config': {
                    'num_instances': 1,
                    'machine_type_uri': 'n1-standard-8'
                },
                'worker_config': {
                    'num_instances': 2,
                    'machine_type_uri': 'n1-standard-8',
                },
                "software_config": {"image_version": "1.4-ubuntu18",
                                    "properties": {"dataproc:alpha.state.shuffle.hcfs.enabled": "false"
                                                   }
                                    },
                "lifecycle_config": {
                    "idle_delete_ttl": duration
                },
                'initialization_actions': [
                    {
                        'executable_file': 'gs://sparkrecommendationengine/packages.sh'
                    }
                ]
            }
        }

        cluster = self.dataproc_cluster_client.create_cluster(self.project_id, self.region, cluster_data)
        cluster.add_done_callback(self._callback)
        global waiting_callback
        waiting_callback = True

    def _callback(self, operation_future):
        # Reset global when callback returns.
        global waiting_callback
        waiting_callback = False

    def wait_for_cluster_creation(self):
        """Wait for cluster creation."""
        print('Waiting for cluster creation...')

        while True:
            if not waiting_callback:
                print("Cluster created.")
                break

    def submit_pyspark_job(self):
        """Submit the Pyspark job to the cluster (assumes `filename` was uploaded
        to `bucket_name."""
        job_details = {
            'placement': {
                'cluster_name': self.cluster_name
            },
            'pyspark_job': {
                'main_python_file_uri': 'gs://{}/engine.py'.format(self.bucket_name),
                'args': [
                    CLOUDSQL_INSTANCE_IP,
                    DB_NAME,
                    DB_USER,
                    DB_PASSWORD
                ]
            }
        }

        result = self.dataproc_job_client.submit_job(
            project_id=self.project_id, region=self.region, job=job_details)
        job_id = result.reference.job_id
        print('Submitted job ID {}.'.format(job_id))
        return job_id

    def list_clusters_with_details(self):
        """List the details of clusters in the region."""
        for cluster in self.dataproc_cluster_client.list_clusters(self.project_id, self.region):
            return cluster.status.State.Name(cluster.status.state)

    def wait_for_cluster_deletion(self):
        print('waiting for cluster deletion...')
        while True:
            if self.list_clusters_with_details() != 'DELETING':
                break


manager = DataprocManager(PROJECT_ID, PROJECT_ID, PROJECT_ID)

def run_job(event, context):
    with __get_cursor() as cursor:
        cursor.execute(
            'SELECT id, movieIds FROM USER WHERE id <> 610 AND movieIds IS NOT null AND job_submited IS null')
        # users who has selected movies to get recommendations and also who's spark job wasn't submitted in
        # order to get those recommendations.
        rows = cursor.fetchall()
    if len(rows) != 0:
        if not manager.list_clusters_with_details() or manager.list_clusters_with_details() == 'DELETING':
            print('submitting job...')
            manager.wait_for_cluster_deletion()
            manager.create_cluster()
            time.sleep(7)
            manager.submit_pyspark_job()
        else:
            manager.submit_pyspark_job()
    else:
        return 'there is no new user.'

