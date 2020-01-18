---
title: json_schema
type: condition
---

```yaml
json_schema:
  part: 0
  schema: ""
  schema_path: ""
```

Validates a message against the provided JSONSchema definition to retrieve a
boolean response indicating whether the message matches the schema or not.
If the response is true the condition passes, otherwise it does not. Please
refer to the [JSON Schema website](https://json-schema.org/) for information and
tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

``` json
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

``` yaml
json_schema:
  part: 0
  schema_path: "file://path_to_schema.json"
```

If the message being processed looked like:

``` json
{"firstName":"John","lastName":"Doe","age":21}
```

Then the condition would pass.


