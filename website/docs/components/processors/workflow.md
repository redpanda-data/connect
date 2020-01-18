---
title: workflow
type: processor
---

```yaml
workflow:
  meta_path: meta.workflow
  stages: {}
```

Performs the same workflow stages as the [`process_dag`](process_dag)
processor, but uses a record of workflow statuses stored in the path specified
by the field `meta_path` in order to report which workflow stages
succeeded, were skipped, or failed for a document. The record takes this form:

```json
{
	"succeeded": [ "foo" ],
	"skipped": [ "bar" ],
	"failed": [ "baz" ]
}
```

If a document is consumed that already contains these records then they will be
used in order to only perform stages that haven't already succeeded or have been
skipped. For example, if a document received contained the above snippet then
the foo and bar stages would not be attempted. Before writing the new records to
the resulting payloads the old one will be moved into
`<meta_path>.previous`.

If a field `<meta_path>.apply` exists in the record for a document and
is an array then it will be used as a whitelist of stages to apply, all other
stages will be skipped.

You can read more about workflows in Benthos
[in this document](/docs/configuration/workflows).


