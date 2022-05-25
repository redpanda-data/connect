---
title: Bloblang Walkthrough
sidebar_label: Walkthrough
description: A step by step introduction to Bloblang
---

Bloblang is the most advanced mapping language that you'll learn from this walkthrough (probably). It is designed for readability, the power to shape even the most outrageous input documents, and to easily make erratic schemas bend to your will. Bloblang is the native mapping language of Benthos, but it has been designed as a general purpose technology ready to be adopted by other tools.

In this walkthrough you'll learn how to make new friends by mapping their documents, and lose old friends as they grow jealous and bitter of your mapping abilities. There are a few ways to execute Bloblang but the way we'll do it in this guide is to pull a Benthos docker image and run the command `benthos blobl server`, which opens up an interactive Bloblang editor:

```sh
docker pull jeffail/benthos:latest
docker run -p 4195:4195 --rm jeffail/benthos blobl server --no-open --host 0.0.0.0
```

:::note Alternatives
For alternative Benthos installation options check out the [getting started guide][guides.getting_started].
:::

Next, open your browser at `http://localhost:4195` and you should see an app with three panels, the top-left is where you paste an input document, the bottom is your Bloblang mapping and on the top-right is the output.

## Your first assignment

The primary goal of a Bloblang mapping is to construct a brand new document by using an input document as a reference, which we achieve through a series of assignments. Bloblang is traditionally used to map JSON documents and that's mostly what we'll be doing in this walkthrough. The first mapping you'll see when you open the editor is a single assignment:

```coffee
root = this
```

On the left-hand side of the assignment is our assignment target, where `root` is a keyword referring to the root of the new document being constructed. On the right-hand side is a query which determines the value to be assigned, where `this` is a keyword that refers to the context of the mapping which begins as the root of the input document.

As you can see the input document in the editor begins as a JSON object `{"message":"hello world"}`, and the output panel should show the result as:

```json
{
  "message": "hello world"
}
```

Which is a (neatly formatted) replica of the input document. This is the result of our mapping because we assigned the entire input document to the root of our new thing. However, you won't get far in life by trapping yourself in the past, let's create a brand new document by assigning a fresh object to the root:

```coffee
root = {}
root.foo = this.message
```

Bloblang supports a bunch of [literal types][blobl.literals], and the first line of this mapping assigns an empty object literal to the root. The second line then creates a new field `foo` on that object by assigning it the value of `message` from the input document. You should see that our output has changed to:

```json
{
  "foo": "hello world"
}
```

In Bloblang, when the path that we assign to contains fields that are themselves unset then they are created as empty objects. This rule also applies to `root` itself, which means the mapping:

```coffee
root.foo.bar = this.message
root.foo."buz me".baz = "I like mapping"
```

Will automatically create the objects required to produce the output document:

```json
{
  "foo": {
    "bar": "hello world",
    "buz me": {
      "baz": "I like mapping"
    }
  }
}
```

Also note that we can use quotes in order to express path segments that contain symbols or whitespace. Great, let's move on quick before our self-satisfaction gets in the way of progress.

## Basic Methods and Functions

Nothing is ever good enough for you, why should the input document be any different? Usually in our mappings it's necessary to mutate values whilst we map them over, this is almost always done with methods, of which [there are many][blobl.methods]. To demonstrate we're going to change our mapping to [uppercase][blobl.methods.uppercase] the field `message` from our input document:

```coffee
root.foo.bar = this.message.uppercase()
root.foo."buz me".baz = "I like mapping"
```

As you can see the syntax for a method is similar to many languages, simply add a dot on the target value followed by the method name and arguments within brackets. With this method added our output document should look like this:

```json
{
  "foo": {
    "bar": "HELLO WORLD",
    "buz me": {
      "baz": "I like mapping"
    }
  }
}
```

Since the result of any Bloblang query is a value you can use methods on anything, including other methods. For example, we could expand our mapping of `message` to also replace `WORLD` with `EARTH` using the [`replace_all` method][blobl.methods.replace_all]:

```coffee
root.foo.bar = this.message.uppercase().replace_all("WORLD", "EARTH")
root.foo."buz me".baz = "I like mapping"
```

As you can see this method required some arguments. Methods support both nameless (like above) and named arguments, which are often literal values but can also be queries themselves. For example try out the following mapping using both named style and a dynamic argument:

```coffee
root.foo.bar = this.message.uppercase().replace_all(old: "WORLD", new: this.message.capitalize())
root.foo."buz me".baz = "I like mapping"
```

Woah, I think that's the plot to Inception, let's move onto functions. Functions are just boring methods that don't have a target, and there are [plenty of them as well][blobl.functions]. Functions are often used to extract information unrelated to the input document, such as [environment variables][blobl.functions.env], or to generate data such as [timestamps][blobl.functions.now] or [UUIDs][blobl.functions.uuid_v4].

Since we're completionists let's add one to our mapping:

```coffee
root.foo.bar = this.message.uppercase().replace_all("WORLD", "EARTH")
root.foo."buz me".baz = "I like mapping"
root.foo.id = uuid_v4()
```

Now I can't tell you what the output looks like since it will be different each time it's mapped, how fun!

### Deletions

Everything in Bloblang is an expression to be assigned, including deletions, which is a [function `deleted()`][blobl.functions.deleted]. To illustrate let's create a field we want to delete by changing our input to the following:

```json
{
  "name": "fooman barson",
  "age": 7,
  "opinions": ["trucks are cool","trains are cool","chores are bad"]
}
```

If we wanted a full copy of this document without the field `name` then we can assign `deleted()` to it:

```coffee
root = this
root.name = deleted()
```

And it won't be included in the output:

```json
{
  "age": 7,
  "opinions": [
    "trucks are cool",
    "trains are cool",
    "chores are bad"
  ]
}
```

An alternative way to delete fields is the [method `without`][blobl.methods.without], our above example could be rewritten as a single assignment `root = this.without("name")`. However, `deleted()` is generally more powerful and will come into play more later on.

## Variables

Sometimes it's necessary to capture a value for later, but we might not want it to be added to the resulting document. In Bloblang we can achieve this with variables which are created using the `let` keyword, and can be referenced within subsequent queries with a dollar sign prefix:

```coffee
let id = uuid_v4()
root.id_sha1 = $id.hash("sha1").encode("hex")
root.id_md5 = $id.hash("md5").encode("hex")
```

Variables can be assigned any value type, including objects and arrays.

## Unstructured and Binary Data

So far in all of our examples both the input document and our newly mapped document are structured, but this does not need to be so. Try assigning some literal value types directly to the `root`, such as a string `root = "hello world"`, or a number `root = 5`.

You should notice that when a value type is assigned to the root the output is the raw value, and therefore strings are not quoted. This is what makes it possible to output data of any format, including encrypted, encoded or otherwise binary data.

Unstructured mapping is not limited to the output. Rather than referencing the input document with `this`, where it must be structured, it is possible to reference it as a binary string with the [function `content`][blobl.functions.content], try changing your mapping to:

```coffee
root = content().uppercase()
```

And then put any old gibberish in the input panel, the output panel should be the same gibberish but all uppercase.

## Conditionals

In order to play around with conditionals let's set our input to something structured:

```json
{
  "pet": {
    "type": "cat",
    "is_cute": true,
    "treats": 5,
    "toys": 3
  }
}
```

In Bloblang all conditionals are expressions, this is a core principal of Bloblang and will be important later on when we're mapping deeply nested structures.

### If Expression

The simplest conditional is the `if` expression, where the boolean condition does not need to be in parentheses. Let's create a map that modifies the number of treats our pet receives based on a field:

```coffee
root = this
root.pet.treats = if this.pet.is_cute {
  this.pet.treats + 10
}
```

Try that mapping out and you should see the number of treats in the output increased to 15. Now try changing the input field `pet.is_cute` to `false` and the output treats count should go back to the original 5.

When a conditional expression doesn't have a branch to execute then the assignment is skipped entirely, which means when the pet is not cute the value of `pet.treats` is unchanged (and remains the value set in the `root = this` assignment).

We can add an `else` block to our `if` expression to remove treats entirely when the pet is not cute:

```coffee
root = this
root.pet.treats = if this.pet.is_cute {
  this.pet.treats + 10
} else {
  deleted()
}
```

This is possible because field deletions are expressed as assigned values created with the `deleted()` function. This is cool but also in poor taste, treats should be allocated based on need, not cuteness!

### Match Expression

Another conditional expression is `match` which allows you to list many branches consisting of a condition and a query to execute separated with `=>`, where the first condition to pass is the one that is executed:

```coffee
root = this
root.pet.toys = match {
  this.pet.treats > 5 => this.pet.treats - 5,
  this.pet.type == "cat" => 3,
  this.pet.type == "dog" => this.pet.toys - 3,
  this.pet.type == "horse" => this.pet.toys + 10,
  _ => 0,
}
```

Try executing that mapping with different values for `pet.type` and `pet.treats`. Match expressions can also specify a new context for the keyword `this` which can help reduce some of the boilerplate in your boolean conditions. The following mapping is equivalent to the previous:

```coffee
root = this
root.pet.toys = match this.pet {
  this.treats > 5 => this.treats - 5,
  this.type == "cat" => 3,
  this.type == "dog" => this.toys - 3,
  this.type == "horse" => this.toys + 10,
  _ => 0,
}
```

Your boolean conditions can also be expressed as value types, in which case the context being matched will be compared to the value:

```coffee
root = this
root.pet.toys = match this.pet.type {
  "cat" => 3,
  "dog" => 5,
  "rabbit" => 8,
  "horse" => 20,
  _ => 0,
}
```

## Error Handling

Are you feeling relaxed? Well don't, because in the world of mapping anything can happen, at ANY TIME, and there are plenty of ways that a mapping can fail due to variations in the input data. Are you feeling stressed? Well don't, because Bloblang makes handling errors easy.

First, let's take a look at what happens when errors _aren't_ handled, change your input to the following:

```json
{
  "palace_guards": 10,
  "angry_peasants": "I couldn't be bothered to ask them"
}
```

And change your mapping to something simple like a number comparison:

```coffee
root.in_trouble = this.angry_peasants > this.palace_guards
```

Uh oh! It looks like our canvasser was too lazy and our `angry_peasants` count was incorrectly set for this document. You should see an error in the output window that mentions something like `cannot compare types string (from field this.angry_peasants) and number (from field this.palace_guards)`, which means the mapping was abandoned.

So what if we want to try and map something, but don't care if it fails? In this case if we are unable to compare our angry peasants with palace guards then I would still consider us in trouble just to be safe.

For that we have a special [method `catch`][blobl.methods.catch], which if we add to any query allows us to specify an argument to be returned when an error occurs. Since methods can be added to any query we can surround our arithmetic with brackets and catch the whole thing:

```coffee
root.in_trouble = (this.angry_peasants > this.palace_guards).catch(true)
```

Now instead of an error we should see an output with `in_trouble` set to `true`. Try changing to value of `angry_peasants` to a few different values, including some numbers.

One of the powerful features of `catch` is that when it is added at the end of a series of expressions and methods it will capture errors at any part of the series, allowing you to capture errors at any granularity. For example, the mapping:

```coffee
root.abort_mission = if this.mission.type == "impossible" {
  !this.user.motives.contains("must clear name")
} else {
  this.mission.difficulty > 10
}.catch(false)
```

Will catch errors caused by:

- `this.mission.type` not being a string
- `this.user.motives` not being an array
- `this.mission.difficulty` not being a number

But will always return `false` if any of those errors occur. Try it out with this input and play around by breaking some of the fields:

```json
{
  "mission": {
    "type": "impossible",
    "difficulty": 5
  },
  "user": {
    "motives": ["must clear name"]
  }
}
```

Now try out this mapping:

```coffee
root.abort_mission = if (this.mission.type == "impossible").catch(true) {
  !this.user.motives.contains("must clear name").catch(false)
} else {
  (this.mission.difficulty > 10).catch(true)
}
```

This version is more granular and will capture each of the errors individually, with each error given a unique `true` or `false` fallback.

## Validation

I'm worried that I've turned you into some sort of error hating thug, hell-bent on eliminating all errors from existence. However, sometimes errors are what we want. Failing a mapping with an error allows us to handle the bad document in other ways, such as routing it to a dead-letter queue or filtering it entirely.

You can read about common Benthos error handling patterns for bad data in the [error handling guide][configuration.error_handling], but the first step is to create the error. Luckily, Bloblang has a range of ways of creating errors under certain circumstances, which can be used in order to validate the data being mapped.

There are [a few helper methods][blobl.methods.coercion] that make validating and coercing fields nice and easy, try this mapping out:

```coffee
root.foo = this.foo.number()
root.bar = this.bar.not_null()
root.baz = this.baz.not_empty()
```

With some of these sample inputs:

```json
{"foo":"nope","bar":"hello world","baz":[1,2,3]}
{"foo":5,"baz":[1,2,3]}
{"foo":10,"bar":"hello world","baz":[]}
```

However, these methods don't cover all use cases. The general purpose error throwing technique is the [`throw` function][blobl.functions.throw], which takes an argument string that describes the error. When it's called it will throw a mapping error that abandons the mapping (unless it's caught, psych!)

For example, we can check the type of a field with the [method `type`][blobl.methods.type], and then throw an error if it's not the type we expected:

```coffee
root.foos = if this.user.foos.type() == "array" {
  this.user.foos
} else {
  throw("foos must be an array, but it ain't, what gives?")
}
```

Try this mapping out with a few sample inputs:

```json
{"user":{"foos":[1,2,3]}}
{"user":{"foos":"1,2,3"}}
```

## Context

In Bloblang, when we refer to the context we're talking about the value returned with the keyword `this`. At the beginning of a mapping the context starts off as a reference to the root of a structured input document, which is why the mapping `root = this` will result in the same document coming out as you put in.

However, in Bloblang there are mechanisms whereby the context might change, we've already seen how this can happen within a `match` expression. Another useful way to change the context is by adding a bracketed query expression as a method to a query, which looks like this:

```coffee
root = this.foo.bar.(this.baz + this.buz)
```

Within the bracketed query expression the context becomes the result of the query that it's a method of, so within the brackets in the above mapping the value of `this` points to the result of `this.foo.bar`, and the mapping is therefore equivalent to:

```coffee
root = this.foo.bar.baz + this.foo.bar.buz
```

With this handy trick the `throw` mapping from the validation section above could be rewritten as:

```coffee
root.foos = this.user.foos.(if this.type() == "array" { this } else {
  throw("foos must be an array, but it ain't, what gives?")
})
```

### Naming the Context

Shadowing the keyword `this` with new contexts can look confusing in your mappings, and it also limits you to only being able to reference one context at any given time. As an alternative, Bloblang supports context capture expressions that look similar to lambda functions from other languages, where you can name the new context with the syntax `<context name> -> <query>`, which looks like this:

```coffee
root = this.foo.bar.(thing -> thing.baz + thing.buz)
```

Within the brackets we now have a new field `thing`, which returns the context that would have otherwise been captured as `this`. This also means the value returned from `this` hasn't changed and will continue to return the root of the input document.

## Coalescing

Being able to open up bracketed query expressions on fields leads us onto another cool trick in Bloblang referred to as coalescing. It's very common in the world of document mapping that due to structural deviations a value that we wish to obtain could come from one of multiple possible paths.

To illustrate this problem change the input document to the following:

```json
{
  "thing": {
    "article": {
      "id": "foo",
      "contents": "Some people did some stuff"
    }
  }
}
```

Let's say we wish to flatten this structure with the following mapping:

```coffee
root.contents = this.thing.article.contents
```

But articles are only one of many document types we expect to receive, where the field `contents` remains the same but the field `article` could instead be `comment` or `share`. In this case we could expand our map of `contents` to use a `match` expression where we check for the existence of `article`, `comment`, etc in the input document.

However, a much cleaner way of approaching this is with the pipe operator (`|`), which in Bloblang can be used to join multiple queries, where the first to yield a non-null result is selected. Change your mapping to the following:

```coffee
root.contents = this.thing.article.contents | this.thing.comment.contents
```

And now try changing the field `article` in your input document to `comment`. You should see that the value of `contents` remains as `Some people did some stuff` in the output document.

Now, rather than write out the full path prefix `this.thing` each time we can use a bracketed query expression to change the context, giving us more space for adding other fields:

```coffee
root.contents = this.thing.(this.article | this.comment | this.share).contents
```

And by the way, the keyword `this` within queries can be omitted and made implicit, which allows us to reduce this even further:

```coffee
root.contents = this.thing.(article | comment | share).contents
```

Finally, we can also add a pipe operator at the end to fallback to a literal value when none of our candidates exists:

```coffee
root.contents = this.thing.(article | comment | share).contents | "nothing"
```

Neat.

## Advanced Methods

Congratulations for making it this far, but if you take your current level of knowledge to a map-off you'll be laughed off the stage. What happens when you need to map all of the elements of an array? Or filter the keys of an object by their values? What if the fellowship just used the eagles to fly to mount doom?

Bloblang offers a bunch of advanced methods for [manipulating structured data types][blobl.methods.object-array-manipulation], let's take a quick tour of some of the cooler ones. Set your input document to this list of things:

```json
{
  "num_friends": 5,
  "things": [
    {
      "name": "yo-yo",
      "quantity": 10,
      "is_cool": true
    },
    {
      "name": "dish soap",
      "quantity": 50,
      "is_cool": false
    },
    {
      "name": "scooter",
      "quantity": 1,
      "is_cool": true
    },
    {
      "name": "pirate hat",
      "quantity": 7,
      "is_cool": true
    }
  ]
}
```

Let's say we wanted to reduce the `things` in our input document to only those that are cool and where we have enough of them to share with our friends. We can do this with a [`filter` method][blobl.methods.filter]:

```coffee
root = this.things.filter(thing -> thing.is_cool && thing.quantity > this.num_friends)
```

Try running that mapping and you'll see that the output is reduced. What is happening here is that the `filter` method takes an argument that is a query, and that query will be mapped for each individual element of the array (where the context is changed to the element itself). We have captured the context into a field `thing` which allows us to continue referencing the root of the input with `this`.

The `filter` method requires the query parameter to resolve to a boolean `true` or `false`, and if it resolves to `true` the element will be present in the resulting array, otherwise it is removed.

Being able to express a query argument to be applied to a range in this way is one of the more powerful features of Bloblang, and when mapping complex structured data these advanced methods will likely be a common tool that you'll reach for.

Another such method is [`map_each`][blobl.methods.map_each], which allows you to mutate each element of an array, or each value of an object. Change your input document to the following:

```json
{
  "talking_heads": [
    "1:E.T. is a bad film,Pokemon corrupted an entire generation",
    "2:Digimon ripped off Pokemon,Cats are boring",
    "3:I'm important",
    "4:Science is just made up,The Pokemon films are good,The weather is good"
  ]
}
```

Here we have an array of talking heads, where each element is a string containing an identifer, a colon, and a comma separated list of their opinions. We wish to map each string into a structured object, which we can do with the following mapping:

```coffee
root = this.talking_heads.map_each(raw -> {
  "id": raw.split(":").index(0),
  "opinions": raw.split(":").index(1).split(",")
})
```

The argument to `map_each` is a query where the context is the element, which we capture into the field `raw`. The result of the query argument will become the value of the element in the resulting array, and in this case we return an object literal.

In order to separate the identifier from opinions we perform a `split` by colon on the raw string element and get the first substring with the `index` method. We then do the split again and extract the remainder, and split that by comma in order to extract all of the opinions to an array field.

However, one problem with this mapping is that the split by colon is written out twice and executed twice. A more efficient way of performing the same thing is with the bracketed query expressions we've played with before:

```coffee
root = this.talking_heads.map_each(raw -> raw.split(":").(split_string -> {
  "id": split_string.index(0),
  "opinions": split_string.index(1).split(",")
}))
```

:::note Challenge!
Try updating that map so that only opinions that mention Pokemon are kept 
:::


Cool. To find more methods for manipulating structured data types check out the [methods page][blobl.methods.object-array-manipulation].

## Reusable Mappings

Bloblang has cool methods, sure, but there's nothing cooler than methods you've made yourself. When the going gets tough in the mapping world the best solution is often to create a named mapping, which you can do with the keyword `map`:

```coffee
map parse_talking_head {
  let split_string = this.split(":")

  root.id = $split_string.index(0)
  root.opinions = $split_string.index(1).split(",")
}

root = this.talking_heads.map_each(raw -> raw.apply("parse_talking_head"))
```

The body of a named map, encapsulated with squiggly brackets, is a totally isolated mapping where `root` now refers to a new value being created for each invocation of the map, and `this` refers to the root of the context provided to the map.

Named maps are executed with the [method `apply`][blobl.methods.apply], which has a string parameter identifying the map to execute, this means it's possible to dynamically select the target map.

As you can see in the above example we were able to use a custom map in order to create our talking head objects without the object literal. Within a named map we can also create variables that exist only within the scope of the map.

A cool feature of named mappings is that they can invoke themselves recursively, allowing you to define mappings that walk deeply nested structures. The following mapping will scrub all values from a document that contain the word "Voldemort" (case insensitive):

```coffee
map remove_naughty_man {
  root = match {
    this.type() == "object" => this.map_each(item -> item.value.apply("remove_naughty_man")),
    this.type() == "array" => this.map_each(ele -> ele.apply("remove_naughty_man")),
    this.type() == "string" => if this.lowercase().contains("voldemort") { deleted() },
    this.type() == "bytes" => if this.lowercase().contains("voldemort") { deleted() },
    _ => this,
  }
}

root = this.apply("remove_naughty_man")
```

Try running that mapping with the following input document:

```json
{
  "summer_party": {
    "theme": "the woman in black",
    "guests": [
      "Emma Bunton",
      "the seal I spotted in Trebarwith",
      "Voldemort",
      "The cast of Swiss Army Man",
      "Richard"
    ],
    "notes": {
      "lisa": "I don't think voldemort eats fish",
      "monty": "Seals hate dance music"
    }
  },
  "crushes": [
    "Richard is nice but he hates pokemon",
    "Victoria Beckham but I think she's taken",
    "Charlie but they're totally into Voldemort"
  ]
}
```

Charlie will be upset but at least we'll be safe.

## Unit Testing

You are truly a champion of mappings, and you're probably feeling pretty confident right now. Maybe you even have a mapping that you're particularly proud of. Well, I'm sorry to inform you that your mapping is DOOMED, as a mapping without unit tests is like a Twitter session, with the progression of time it will inevitably descend into madness.

However, if you act now there is still time to spare your mapping from this fate, as Benthos has it's own [unit testing capabilities][configuration.unit_testing] that you can also use for your mappings. To start with save a mapping into a file called something like `naughty_man.blobl`, we can use the example above from the reusable mappings section:

```coffee
map remove_naughty_man {
  root = match {
    this.type() == "object" => this.map_each(item -> item.value.apply("remove_naughty_man")),
    this.type() == "array" => this.map_each(ele -> ele.apply("remove_naughty_man")),
    this.type() == "string" => if this.lowercase().contains("voldemort") { deleted() },
    this.type() == "bytes" => if this.lowercase().contains("voldemort") { deleted() },
    _ => this,
  }
}

root = this.apply("remove_naughty_man")
```

Next, we can define our unit tests in an accompanying YAML file in the same directory, let's call this `naughty_man_test.yaml`:

```yaml
tests:
  - name: test naughty man scrubber
    target_mapping: './naughty_man.blobl'
    environment: {}
    input_batch:
      - content: |
          {
            "summer_party": {
              "theme": "the woman in black",
              "guests": [
                "Emma Bunton",
                "the seal I spotted in Trebarwith",
                "Voldemort",
                "The cast of Swiss Army Man",
                "Richard"
              ]
            }
          }
    output_batches:
      -
        - json_equals: {
            "summer_party": {
              "theme": "the woman in black",
              "guests": [
                "Emma Bunton",
                "the dolphin I spotted in Trebarwith",
                "The cast of Swiss Army Man",
                "Richard"
              ]
            }
          }
```

As you can see we've defined a single test, where we point to our mapping file which will be executed in our test. We then specify an input message which is a reduced version of the document we tried out before, and finally we specify output predicates, which is a JSON comparison against the output document.

We can execute these tests with `benthos test ./naughty_man_test.yaml`, Benthos will also automatically find our tests if you simply run `benthos test ./...`. You should see an output something like:

```text
Test 'naughty_man_test.yaml' failed

Failures:

--- naughty_man_test.yaml ---

test naughty man scrubber [line 2]:
batch 0 message 0: json_equals: JSON content mismatch
{
    "summer_party": {
        "guests": [
            "Emma Bunton",
            "the seal I spotted in Trebarwith" => "the dolphin I spotted in Trebarwith",
            "The cast of Swiss Army Man",
            "Richard"
        ],
        "theme": "the woman in black"
    }
}
```

Because in actual fact our expected output is wrong, I'll leave it to you to spot the error. Once the test is fixed you should see:

```text
Test 'naughty_man_test.yaml' succeeded
```

And now our mapping, should we need to expand it in the future, is better protected against regressions. You can read more about the Benthos unit test specification, including alternative output predicates, in [this document][configuration.unit_testing].

## Final Words

That's it for this walkthrough, if you're hungry for more then I suggest you re-evaluate your priorities in life. If you have feedback then please [get in touch][community], despite being terrible people the Benthos community are very welcoming.

[guides.getting_started]: /docs/guides/getting_started
[blobl.methods]: /docs/guides/bloblang/methods
[blobl.methods.uppercase]: /docs/guides/bloblang/methods#uppercase
[blobl.methods.replace_all]: /docs/guides/bloblang/methods#replace_all
[blobl.methods.catch]: /docs/guides/bloblang/methods#catch
[blobl.methods.without]: /docs/guides/bloblang/methods#without
[blobl.methods.type]: /docs/guides/bloblang/methods#type
[blobl.methods.coercion]: /docs/guides/bloblang/methods#type-coercion
[blobl.methods.object-array-manipulation]: /docs/guides/bloblang/methods#object--array-manipulation
[blobl.methods.filter]: /docs/guides/bloblang/methods#filter
[blobl.methods.map_each]: /docs/guides/bloblang/methods#map_each
[blobl.methods.apply]: /docs/guides/bloblang/methods#apply
[blobl.functions]: /docs/guides/bloblang/functions
[blobl.functions.deleted]: /docs/guides/bloblang/functions#deleted
[blobl.functions.content]: /docs/guides/bloblang/functions#content
[blobl.functions.env]: /docs/guides/bloblang/functions#env
[blobl.functions.now]: /docs/guides/bloblang/functions#now
[blobl.functions.uuid_v4]: /docs/guides/bloblang/functions#uuid_v4
[blobl.functions.throw]: /docs/guides/bloblang/functions#throw
[blobl.literals]: /docs/guides/bloblang/about#literals
[configuration.error_handling]: /docs/configuration/error_handling
[configuration.unit_testing]: /docs/configuration/unit_testing
[community]: /community
