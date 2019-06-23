Dataflow is a No-Ops fully scalable pipeline service on GCP. It is an optimal solution for ETL processes for both streaming and batch data.

Dataflow is able to connect and write to various GCP services. A common practice is to have Dataflow perform ETL between some data source like PubSub into a Data Warehouse such as BigQuery.

Dataflow runs on Apache Beam. It is possible to route pipelines into different processes dependent on condition. It also supports side inputs which has a PCollection pair with a dataset for process purposes.

While powerful, Dataflow is a potentially expensive service and should be used carefully in scale operations.

There are some nuances to working with Dataflow that structure code differently than standard scripts. More is demonstrated in example pipeline.
