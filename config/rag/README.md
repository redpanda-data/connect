## RAG with Redpanda Connect

This folder hosts a series of RAG pipelines using Redpanda + Redpanda Connect.

We have a series of ingestion pipelines in the `ingestion` directory, these all write
data we want to ingest into our vector database in topics with the pattern: `rp.ai.rag.*`

Next, there is a matrix of different indexing piplines that use the following sets of options:

Vector Database: `{postgres, elasticsearch, qdrant}`

Embeddings Model: `{openai, cohere}`

These are implemented as resources in the `indexing/resources` directory, then each instance of a pipline
is created in the `indexing` pipeline.

Lastly, there is a set of retrivial pipelines that mirror each one of our indexing pipelines. These pipelines
are exposed over HTTP and they can be used to retrive documents/chunks from. These pipelines all live in the
`retrivial` directory.

Lastly, there is an evalutation framework setup by running a final pipeline after indexing is complete, that exists
in the `evaluation` directory, which also has some golden files/snapshots of various versions of the pipelines we can
use to rank/compare the quality of different indexing options.

## Running the pipelines

First bootup the required services:

```
docker compose up -d
```

Then you need to set the environment variables, specifically the `*_API_KEY` ones.

```
cp env.sample env && vim env
```

Ingest and index all the documents for our knowledge graph

```
rpk connect streams -t 'templates/*.yaml' -e env indexing/* ingestion/*
```

Once everything has been ingested, then indexed, we can stop the pipeline and startup the retrival pipelines

```
rpk connect streams -t 'templates/*.yaml' -e env retrivial/*
```

Now concurrently run the eval pipeline to run the evaluations:

```
rpk connect run -e env eval.yaml
```

Now there should be a file in ./results with all the resulting fetched documents
