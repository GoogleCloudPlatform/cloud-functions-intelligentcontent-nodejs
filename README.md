# Processing User-generated Content Using the Cloud Video Intelligence and Cloud Vision APIs 
This code is an example reference implementation for the [solution](https://cloud.google.com/solutions/processing-marketing-submissions-using-video-intelligence) on the Google Cloud solutions site.

# Components
* index.js - the Google Cloud Functions implementation in NodeJS that accepts files, processes them through the Vision and Video Intelligence API and loads the results into BigQuery
* intelligent_content_bq_schema.json - The BigQuery schema used to create the BigQuery table.


# Installation
1. Follow the detailed steps in the Processing User-generated Content Using the Cloud Video Intelligence and Cloud Vision APIs [tutorial](https://cloud.google.com/solutions/processing-user-generated-content-using-video-intelligence) to configure and deploy the code
