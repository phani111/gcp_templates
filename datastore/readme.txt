This is a directory containing source code and information on GCP Datastore/Firestore. Datastore has recently been revamped as Firestore with Datastore mode, oppose to Firestore Native Mode.


About Datastore

Datastore is a semi-structured NoSQL Database with limited querying abilities. It is also a default backend DB for App Engine.
Because Datastore is NoSQL, it is a horizontally scalable DB. This means that entities within a kind do not have to share the same elements.
This allows for flexible data entry where unique elements can be added for observations and will not require a strict schema to follow.
Additionally, Datastore is optimal for fast reads and writes by key retrieval. Can scale up to TBs of data.

Elements of Datastore

Kind - Kind is a grouping of entities, can be thought of as a table.
Parents - kinds can be grouped into multi-tiered categories, parents are an above tier for a kind
Entity - An entity is one observation within a kind. Can be thought of as one row. 
Key - Entities are differentiated and mapped to unique keys within their kind.
Element - An element is a key-value pair belonging to a key/entity. Entities can have multiple elements.

When to use Datastore

Datastore is a good solution when you want fast reads and writes with data, especially when the data is supporting
an application on App Engine or Kubernetes Engine. Also if your data requirements are flexible, i.e not following a strict schema, 
using Datastore as a NoSQL DB will save some headaches by being able to store unique schema data within one table. Also, horizontal scalability means
that over time you can build out horizontally on an entity/observation. So say you enter data for a customer over time, you can track that customer in
a horizontally scaling "row" instead of needing to scale vertically with multiple rows in a SQL DB.
Depending on the makeup of your data, there are limited query capabilities that can be used for analytics similar to a relational DB. But, 
keep in mind while it is possible for analytics, if that is your sole intention for data storage you should look into another DB.
