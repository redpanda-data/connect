---
title: Google Cloud Platform
description: Find out about GCP components in Benthos
---

There are many components within Benthos which utilise Google Cloud Platform (GCP) services. You will find that each of
these components require valid credentials.

When running Benthos inside a Google Cloud environment that has a
[default service account](https://cloud.google.com/iam/docs/service-accounts#default), it can automatically retrieve the
service account credentials to call Google Cloud APIs through a library called Application Default Credentials (ADC).

Otherwise, if your application runs outside Google Cloud environments that provide a default service account, you need
to manually create one. Once you have a service account set up which has the required permissions, you can
[create](https://console.cloud.google.com/apis/credentials/serviceaccountkey) a new Service Account Key and download it
as a JSON file. Then you have 2 options to set this json 
- either, Set the path to this JSON file in the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
- or, set the base64 encoded json in the individual GCP component's config (`credentials_json`). Please remember that this field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets). The corresponding documentation for each component is 
    - [input_bigquery_select](/docs/components/inputs/gcp_bigquery_select)
    - [output_bigquery](/docs/components/outputs/gcp_bigquery)
    - [processor_bigquery_select](/docs/components/processors/gcp_bigquery_select)
    - [input_cloudstorage](/docs/components/inputs/gcp_cloud_storage)
    - [output_cloudstorage](/docs/components/outputs/gcp_cloud_storage)
    - [cache_cloudstorage](/docs/components/caches/gcp_cloud_storage)
    - [input_pubsub](/docs/components/inputs/gcp_pubsub)
    - [output_pubsub](/docs/components/outputs/gcp_pubsub)


Please refer to [this document](https://cloud.google.com/docs/authentication/production) for details.
