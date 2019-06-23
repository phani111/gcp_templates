"""Below is a sample Flask App for posting and retrieving Datastore entities.
   We use a Flask App because DataStore is a frequent DB pairing for an App
   Engine app. These functions can also be used outside of applications.
   App routes are meant to serve as APIs for accessing Datastore DB.

   1) Will retrieve entity as a json. Useful for API operations.
   2) Upload a GCS file into Datastore as its own entity.
   3) Query DataStore."""

import logging
import os
from flask import Flask, redirect, jsonify
from google.cloud import datastore, storage
app = Flask(__name__)
GCS_BUCKET = 'sample_data'

@app.route('/')
def homepage():
    return "making API calls to Datastore for HTML files"


@app.route('/get/<entity_name>')
def get_json(entity_name):
    """retrieves file content from datastore given key"""
    #setup datastore client for connection
    datastore_client = datastore.Client()

    #set retrieval key for datastore by giving kind and URL input for key
    kind = 'files'
    get_key = datastore_client.key(kind, entity_name)

    #GET datastore entity given the key and return as json.
    get_vals = dict(datastore_client.get(get_key))
    return jsonify(get_vals)


@app.route('/post/<file_name>', methods=['GET', 'POST'])
def upload_html(file_name):
    """Retrieves a text file from GCS and uploads content as a Datastore entity"""

    """ 1) GCS Retrieval """
    # Create a Cloud Storage client for connection.
    storage_client = storage.Client()
    # Retrieve file from storage bucket
    bucket = storage_client.get_bucket(GCS_BUCKET)
    blob = bucket.get_blob(file_name)
    bcontent = blob.download_as_string().decode("utf-8")

    """ 2) Datastore Write"""
    # Create a Cloud Datastore client for connection.
    datastore_client = datastore.Client()

    # Create the Cloud Datastore key for the new entity. Name is file name.
    kind = 'files'
    name = blob.name
    key = datastore_client.key(kind, name)

    # Construct the new entity using the key. Set dictionary values for entity
    entity = datastore.Entity(key)
    entity['content'] = bcontent

    # Save the new entity to Datastore.
    datastore_client.put(entity)

    # Redirect to the home page.
    return redirect('/')

@app.route('/query/<kind>/<name>',methods=['GET'])
def query_datastore(kind, name):
    datastore_client = datastore.Client()
    query = datastore_client.query(kind=kind).fetch()
    query.add_filter('name', '=', name).fetch()
    return query    


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
