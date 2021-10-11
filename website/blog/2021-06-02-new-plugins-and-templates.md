---
title: 'Preview: Go Plugins V2 and Config Templates'
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: It's ready, now we need test subjects
keywords: [
    "go",
    "golang",
    "stream processor",
    "ETL",
]
tags: [ "v4", "plugins", "templates", "roadmap" ]
---

I need help, attention and affirmation, and therefore it's time for a development update. Around five months ago I posted a [roadmap for Benthos v4](/blog/2021/01/04/v4-roadmap) that included some utterly unattainable goals that only a super human could achieve.

Now that most of those features are ready to test, namely a new plugins API and config templating, I'm looking for people to try them out and give feedback. Please read on if that sounds like fun to you, or also if it doesn't sound fun but you intend to do it anyway.

<!--truncate-->

## Config Templates

The new config templates functionality allows you to define parameterised templates for Benthos configuration snippets. These templates can then be imported with a cli flag and used in Benthos configs like native Benthos components.

This is going to be super useful in situations where you have commonly used configuration patterns with small differences that prevent you from using resources.

The current state of templates is that they'll be included in the next release as an experimental feature, meaning any aspect of this functionality is subject to change outside of major version releases. This includes the config spec of templates, how they work, and so on.

Defining a template looks roughly like this:

```yaml
name: log_message
type: processor
summary: Print a log line that shows the contents of a message.

fields:
  - name: level
    description: The level to log at.
    type: string
    default: INFO

mapping: |
  root.log.level = this.level
  root.log.message = "${! content() }"
  root.log.fields.metadata = "${! meta() }"
  root.log.fields.error = "${! error() }"
```

And you're able to import templates with the `-t` flag:

```sh
benthos -t ./templates/foo.yaml -c ./config.yaml
```

And using it in a config looks like any other component:

```yaml
pipeline:
  processors:
    - log_message:
        level: ERROR
```

To find out more about configuration templates, including how to try them out, check out [the new templates page][configuration.templating]. More importantly, you can give feedback on them [in this Github discussion][templates-feedback-thread].

## The V2 Go Plugins API

Benthos has had Go plugins for a while now and they're fairly well received. However, they can sometimes be confusing as they expose Benthos internals that aren't necessary to understand as plugin authors.

It was also an issue for me as a maintainer that the current plugin APIs hook directly into Benthos packages that have no business being public. This makes it extra difficult to improve the service without introducing breaking changes.

The new APIs are simpler, more powerful (in the ways that matter), add milk after the water, and most importantly are air-gapped from Benthos internals so that they can evolve independently. Here's a sneaky glance of what a processor plugin looks like:

```go
type ReverseProcessor struct {
	logger *service.Logger
}

func (r *ReverseProcessor) Process(ctx context.Context, m *service.Message) ([]*service.Message, error) {
	bytesContent, err := m.AsBytes()
	if err != nil {
		return nil, err
	}

	newBytes := make([]byte, len(bytesContent))
	for i, b := range bytesContent {
		newBytes[len(newBytes)-i-1] = b
	}

	if bytes.Equal(newBytes, bytesContent) {
		r.logger.Infof("Woah! This is like totally a palindrome: %s", bytesContent)
	}

	m.SetBytes(newBytes)
	return []*service.Message{m}, nil
}

func (r *ReverseProcessor) Close(ctx context.Context) error {
	return nil
}

func main() {
	err := service.RegisterProcessor(
		"reverse", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return &ReverseProcessor{logger: mgr.Logger()}, nil
		})
	if err != nil {
		panic(err)
	}

	service.RunCLI()
}
```

You can play around with these APIs right now by pulling the latest commit with:

```sh
go get -u github.com/Jeffail/benthos/v3@master
```

And you can find more examples along with the API docs at [pkg.go.dev][plugins.api].

The package will remain in an experimental state under `public/x/service` for a month or so, and once it's "ready" (I'm personally happy with it) then it'll be moved to `public/service` and will be considered stable.

The goal is to allow everyone to migrate to the new APIs whilst still supporting the old ones, and then when Benthos V4 is tagged the old ones will vanish and we're no longer blocked on them.

Similar to the templates there is [a Github discussion open for feedback][plugins-feedback-thread]. Be honest, be brutal.

## Join the Community

I've been babbling on for months so if this stuff is news to you then you're clearly out of the loop. Worry not, for you can remedy the situation by joining one or more of our [glorious community spaces][community].

[community]: /community
[configuration.templating]: /docs/configuration/templating
[plugins.api]: https://pkg.go.dev/github.com/Jeffail/benthos/v3/public/service
[templates-feedback-thread]: https://github.com/Jeffail/benthos/discussions/785
[plugins-feedback-thread]: https://github.com/Jeffail/benthos/discussions/754
