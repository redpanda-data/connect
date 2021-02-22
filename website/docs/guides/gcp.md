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
as a JSON file. Then all you need to do set the path to this JSON file in the `GOOGLE_APPLICATION_CREDENTIALS`
environment variable.

Please refer to [this document](https://cloud.google.com/docs/authentication/production) for details.
