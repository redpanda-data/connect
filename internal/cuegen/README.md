# CUE AST tips

## Adding optional fields to structs

Code:

```go
requiredField := &ast.Field {
  Label: ast.NewIdent("username"),
  Value: ast.NewIdent("string"),
}
optionalField := &ast.Field {
  Label: ast.NewIdent("age"),
  Value: ast.NewIdent("uint"),
}

ast.NewStruct(
  requiredField,

  optionalField.Label,
  token.OPTION,
  optionalField.Value,
)
```

## Given a struct, create a disjunction

In other words, given:

```cue
#AllInputs: {
  http_client: {url: string}
  generate: {mapping: string}
  file: {paths: [...string]}
}
```

Generate a type `#Input` that conforms to:

```cue
#Input: {http_client: {url: string}} | {generate: {mapping: string}} | {file: {paths: [...string]}}
```

This can be done using the `or` built-in function in Cue and field comprehension:

```cue
#Input: or([for name, config in #AllInputs {(name): config}])
```

Expressing that using the `ast` package looks like this:

Code:

```go
collectionIdent := ast.NewIdent("#AllInputs")
disjunctionIdent := ast.NewIdent("#Input")

&ast.Field{
  Label: disjunctionIdent,
  Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
    Clauses: []ast.Clause{
      &ast.ForClause{
        Key:    ast.NewIdent("name"),
        Value:  ast.NewIdent("config"),
        Source: collectionIdent,
      },
    },
    Value: ast.NewStruct(&ast.Field{
      Label: interpolateIdent(ast.NewIdent("name")),
      Value: ast.NewIdent("config"),
    }),
  })),
},
```
