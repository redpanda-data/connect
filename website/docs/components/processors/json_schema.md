---
title: json_schema
type: processor
---

```yaml
json_schema:
  parts: []
  schema: ""
  schema_path: ""
```

Checks messages against a provided JSONSchema definition but does not change the
payload under any circumstances. If a message does not match the schema it can
be caught using error handling methods outlined [here](/docs/configuration/error_handling).

Please refer to the [JSON Schema website](https://json-schema.org/) for
information and tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

```json
{
	"$id": "https://example.com/person.schema.json",
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Person",
	"type": "object",
	"properties": {
	  "firstName": {
		"type": "string",
		"description": "The person's first name."
	  },
	  "lastName": {
		"type": "string",
		"description": "The person's last name."
	  },
	  "age": {
		"description": "Age in years which must be equal to or greater than zero.",
		"type": "integer",
		"minimum": 0
	  }
	}
}
```

And the following Benthos configuration:

```yaml
pipeline:
  processors:
  - json_schema:
      schema_path: "file://path_to_schema.json"
  - catch:
    - log:
        level: ERROR
        message: "Schema validation failed due to: ${!error}"
```

If a payload being processed looked like:

```json
{"firstName":"John","lastName":"Doe","age":-21}
```

Then the payload would be unchanged but a log message would appear explaining
the fault. This gives you flexibility in how you may handle schema errors, but
for a simpler use case you might instead wish to use the
[`json_schema`](/docs/components/conditions/json_schema) condition with a
[`filter`](filter).


