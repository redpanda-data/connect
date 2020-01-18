---
title: xml
type: processor
---

```yaml
xml:
  operator: to_json
  parts: []
```

EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Parses messages as an XML document, performs a mutation on the data, and then
overwrites the previous contents with the new value.

### Operators

#### `to_json`

Converts an XML document into a JSON structure, where elements appear as keys of
an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen,
  `-`, to the attribute label.
- If the element is a simple element and has attributes, the element value
  is given the key `#text`.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.

For example, given the following XML:

```xml
<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>
```

The resulting JSON structure would look like this:

```json
{
  "root":{
    "title":"This is a title",
    "description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "elements":[
      {"#text":"foo1","-id":"1"},
      {"#text":"foo2","-id":"2"},
      "foo3"
    ]
  }
}
```


