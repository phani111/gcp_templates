___CloudSQL___
Cloud SQL is favored by many teams because of its easy transfer from existing systems.
It is essentially MySQL hosted on the Cloud, so the only thing old scripts will need to change is connection parameters.
It is also a low cost storage solution that can be configured to be an App Engine app's backend DB.
I do not provide code because it is just MySQL. Instead, I provide an app.yaml file for connecting App Engine to Cloud SQL backend.

___BigQuery___
BigQuery is a massively scalable and powerful relational database that can store and query petabytes of data in seconds.
Storage is also very cheap in BigQuery making it optimal for warehousing. After 3 months of inactivity, a table is reclassified as Nearline Storage, reducing costs further.
Paritioning is available in BigQuery, but only on datetime columns (weird right?) So, the work around is to create some datetime column in your tables to partition by, that is some logic for the values you actually want to partition by.

I do not provide much code for BigQuery here, but it is utilized in quite a few other directories of this repository.
Datastore has a bash section on exporting Datastore data to a BigQuery table.
Dataflow has a pipeline that reads and writes to BigQuery.
PubSub has a Cloud Functions component that queries BigQuery.
