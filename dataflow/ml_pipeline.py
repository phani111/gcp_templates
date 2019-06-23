# -*- coding: utf-8 -*-
"""Dataflow is a fully scalable data processing pipeline service on GCP. It is valuable for ETL processes that take in
   data feeds and transfer them to warehousing. For example, PubSub to BigQuery or old CSV reports to BigQuery are
   common flows. Dataflow runs on Apache Beam and data is transferred as PCollections. This essentially means every
   row is evaluated independently of one another, so aggregating data is not a straight process - but is possible.

   The pipeline below is an example that reads BigQuery table, performs a transformation for ML input,
   then loads a fitted ML model to make a prediction on the PCollection being passed. Data is written to BigQuery.

   Purpose is to detect whether two students shared answers for an assignment given similarity between open ended
   questions and multiple choice questions.

   This pipeline contains conceptual notes in addition to syntax comments to explain why certain methods are used.
   @author: william.spangler"""


"""////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""
"""Step One: Import apache beam which provides the foundation for the pipeline to run. Set GCP vars"""

import apache_beam as beam
from datetime import date
# Parameters that will be used for the BQ read and write pipeline steps.
PROJECT = 'project1'
JOB_NAME = 'ml_pipe_{}'.format(date.today().strftime('%m%d'))
BUCKET = 'sample-pipe'
REQUIREMENT_FILE = 'requirements.txt'


"""////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""
"""Step Two: Create custom class functions that will be called as transforms in the pipeline.
   Packages are imported on the method level so the master node knows to distribute package to worker node.
   If the packages are called outside the class, the requirements will not be transferred for processing.
   Every transformation belongs to its own class, which is called in pipeline at bottom of script."""


class OpenEnded(beam.DoFn):
    """transform that calculates the jaro-winkler scores (string similarity) of open ended question"""
    
    def __init__(self):
        """__init__ function needed at start of each class to initiate the transform""" 
        super(JaroCalc, self).__init__()

    def process(self, row):
        #now that the class is initiated, the process function contains the details of the transform
        import jellyfish
    
        try:
            row['Answer1_similarity'] = float(jellyfish.jaro_winkler(row['student1_a1'], row['student2_a1']))
        except ValueError:
            row['Answer1_similarity'] = 0  # if null value than student did not answer question.
        return [row]


class MultipleChoice(beam.DoFn):
    """transform that flags entry comparisons when they have the same answers for multiple choice questions"""
    def __init__(self):
        super(Flagging, self).__init__()

    def process(self, row):
        # question2
        row['answer2_similarity'] = 1 if row['student1_a2'] == row['student2_a2'] else 0
        # question 3
        row['answer3_similarity'] = 1 if row['student1_a3'] == row['student2_a3'] else 0
        return [row]


class TimeDifference(beam.DoFn):
    """transform that calculates the time between two students submitting their assignments."""
    def __init__(self):
        super(TimeDifference, self).__init__()

    def process(self, row):
        from datetime import datetime, timedelta
        student1_submission, student2_submission = row['student1_time'], row['student1_time']
        row['time_difference'] = student1_submission - student2_submission  # time delta value.
        return [row]


class MlPrediction(beam.DoFn):
    """transform that takes the calculated variables from the previous transforms to make a prediction
    of whether two students copied assignments. The model is trained locally from another script and loaded
    as a pickle file from Google Cloud Storage."""
    def __init__(self):
        super(MlPrediction, self).__init__()
    
    def process(self, row):
        """Import packages"""
        from datetime import timedelta
        import numpy as np
        import pickle  # pickle suffices. However, more complex models might want to use joblib for better performance.
        from sklearn.ensemble import RandomForestClassifier # sklearn versions should match from training the model
        import numpy as np
        from apache_beam.io.gcp import gcsio  # package to read file (pkl) from GCS
        
        """load the fitted model to make predictions given the calculated inputs"""
        gcs = gcsio.GcsIO()  # function to read file from GCS
        file = gcs.open('gs://sample_pipe/model.pkl')
        loaded_model = pickle.load(file)  # loads the fitted model stored in pickle file
        """Setup the model input then use the model to make a prediction."""
        # Input should be in the form of an array.
        ml_input = np.array([int(row['answer1_similarity']), int(row['answer2_similarity']),
                             int(row['answer3_similarity']), timedelta(row['time_difference'])])
        # When using python2, it is necessary to put the array in a list [] to use sklearn prediction.
        # Prediction returns a prediction and confidence measure.
        row['cheated_prediction'] = loaded_model.predict([ml_input])[0]  # only return prediction
        return [row]  # need to return object as a key value array instead of json dictionary


"""////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""
"""Run function will set pipeline execution parameters, then lay out steps to the pipeline.""" 


def run():
    # Setting a schema is necessary when creating a new table.
    schema = ('student1:integer, student1_a1:string, student1_a2:integer, student1_a3:integer, student1_time:datetime,'
              'student2:integer, student2_a1:string, student2_a2:integer, student2_a3:integer, student2_time:datetime,'
              'answer1_similiarity:float, answer2_similarity:integer, answer3_similarity:integer, '
              'timedifference:datetime, cheated_prediction:float, datepartition:date')

    """Execution args to run pipeline"""
    argv = [
        '--project={}'.format(PROJECT),  # necessary to specify project.
        '--job_name={}'.format(JOB_NAME),  # set a custom job name for logging purposes.
        '--save_main_session',  # in event of server interruption, this arg will save the data that has been processed
        '--staging_location=gs://{}/staging/'.format(BUCKET),  # need to set location for setup
        '--temp_location=gs://{}/staging/'.format(BUCKET),  # need to set location for setup
        '--runner=DataflowRunner',  # dataflow runner will run job on dataflow. DirectRunner will run on a single VM
        '--MaxNumWorkers=100',  # set an upperbound of workers (optional)
        '--MachineType=n1-standard-1',  # specifiy machine types (optional). Standard suffices for this data.
        '--zone=us-central1-c',  # set a zone in the same region as the data storage if possible for cheaper processing.
        '--requirements_file={}'.format(REQUIREMENT_FILE),
        '--user']  # identify the role of who executes the pipeline.
    
    """Pipeline steps"""
    with beam.Pipeline(argv=argv) as p: #argv satisfies execution args here.
        (p
             | 'read bq table' >> beam.io.Read(beam.io.BigQuerySource('cpb100-213205:sample.ml_sample'))
             | 'similarity of score' >> (beam.ParDo(OpenEnded()))
             | 'multiple choice flags' >> (beam.ParDo(MultipleChoice()))
             | 'model prediction' >> (beam.ParDo(MlPrediction()))
             | 'write to bq' >> beam.io.Write(beam.io.BigQuerySink(
                     'cpb100-213205:sample.cheaters'.format('cpb100-213205','sample','cheaters'),
                     schema=schema,
                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))


"""////////////////////////////////////////////////////////////////////////////////////////////////////////////////"""
"""Execution of script"""
if __name__ == '__main__':
   run()
