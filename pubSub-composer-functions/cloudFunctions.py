"""Cloud Functions can take PubSub as input.
   This can be a powerful combination to perform some light processing with every message.
   Additionally, a personal favorite is setting up a PubSub subscriber to StackDriver that listens
   for specified events. In this context, the combination of StackDriver, PubSub, and Cloud Functions
   can be used to trigger some operation whenever a specified event occurs in your GCP project.
   Data and context is always required as arguments for the executable function."""

import base64
from google.cloud import bigquery, storage
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

"""*********************************************************************************************************************
                                    Cloud Function with Basic PubSub Message Processing
*********************************************************************************************************************"""

def data_process(data, context):
    """data is the data part of a message. context is the metadata of a message."""
    try:
        msg_data = base64.b64decode(data['data']).decode('utf-8')
    except KeyError:
        print('Data not found in message.')
    # From here do whatever operation we want to the pubsub message data.
    return msg_data

"""*********************************************************************************************************************
                                StackDriver Triggered Events to Cloud Functions via PubSub
*********************************************************************************************************************"""
# HOW TO:
# 1) go to stackdriver logging in the console and select 'EXPORTS' >> "CREATE EXPORT"
# 2) Filter through services to select the log you want exported. Example, creating a BQ table
# 3) Select a sink name (subscriber name)
# 4) Set sink service to PubSub
# 5) Set Sink Destination to a topic (this will publish to that topic). Should prolly make a new topic.
# 6) Set up a Cloud Function to subscribe to that topic. Gets triggered then whenever that log occurs in SD.

"""In the example below, I use a Cloud Function that queries a BigQuery table, creates a data visualization
   for every column, and writes it into a PDF report. That pdf report then gets sent to a GCS Bucket.
   This all gets triggered whenever the BQ table is updated. Executable is bottom function."""

class Reports:
    """Triggered by table creation/update in BQ. PubSub topic will listen to StackDriver for the table create/edit."""
    def __init__(self):
        """Set up client connections to GCP"""
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        return

    def bq_df(self, query):
        """Query BQ and store results into a PD Dataframe"""
        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()
        return df

    def visuals(self, dataframe, filename):
        """Take our dataframe and make chart visualizations.
           Visualizations will be written to a PDF"""
        #cwd = os.getcwd()
        # Use a tmpdir to store pdf file
        # Write pdf charts to GCS
        with PdfPages(filename=f"/tmp/{filename}") as pdf:
            for column in dataframe:
                dataframe[column].value_counts().plot(kind='bar')
                plt.title(column.replace("_", " "))
                pdf.savefig()
                plt.close()
                """Take the report pdf from tmp storage and move it to GCS bucket"""
        bucket = self.storage_client.bucket('survey413reports')
        blob = bucket.blob('surveyreport.pdf')
        blob.upload_from_filename(filename=f"/tmp/{filename}", content_type='application/pdf')
        #os.chdir(cwd)
        return

def cloudfunction_report(data, context):
    # Execute flow of the above class to get reports to GCS
    report = 'surveycharts.pdf'
    q1 = "SELECT * FROM esce.AggregateSurvey"
    df1 = Reports().bq_df(query=q1)
    Reports().visuals(dataframe=df1, filename=report)
    return
