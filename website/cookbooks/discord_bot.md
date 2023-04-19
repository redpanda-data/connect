---
slug: discord-bot
title: Create a Discord Bot
description: Learn how to use Benthos to create a vanity chat bot.
---

Stream processing is stupid and boring, and so it's important to re-purpose tools like Benthos for fun things occasionally. This cookbook outlines how Benthos can be used to create a Discord bot for important tasks such as providing insults and bad jokes to your chat. If you're a member of the [Benthos Discord server][discord-link] then you're likely already familiar with Blob Bot which is the resulting product.

import ReactPlayer from 'react-player/youtube';

<div className='container margin-vert--lg'>
  <div className='row row--no-gutters'>
    <ReactPlayer
        className='col'
        height='300px'
        url='https://www.youtube.com/embed/nX5-s1-Vrjc'
        controls={true}
    />
  </div>
</div>

## Consuming Messages

Before you start messing with Benthos you need to register a new bot with the [Discord Developer Portal][discord-applications]. Start by building an Application, then use the build-a-bot page to choose a bot name and avatar. You should end up with a token generated for the bot, and you'll also need to add it to your server.

As soon as your bot is added to your server and you have a token you can immediately begin consuming messages from a channel with the [`discord` input][inputs.discord]:

```yaml
input:
  discord:
    channel_id: ${DISCORD_CHANNEL}
    bot_token: ${DISCORD_BOT_TOKEN}
    cache: request_tracking

cache_resources:
  - label: request_tracking
    file:
      directory: /tmp/discord_bot
```

> If you aren't sure how to access the ID of a channel try [this tutorial][discord-channel-id].

The `poll_period` shouldn't be too short as it'll exhaust your rate limits. If you plan to use the bot for hitting multiple Discord APIs then give it a fair few seconds between each poll. It's also necessary to point the input to a [cache resource][caches], and this will be used to store the ID of the latest message received for paginating the messages endpoint.

The `limit` is the maximum number of messages to consume from the channel when we haven't got a message to track and are consuming a backlog. The first time we run our bot we will pull a maximum of 10 of the latest messages in the channel, the maximum you can set here is 100.

If you were to run this config (setting the channel and bot token as env vars in a file called `testing.env`) you'll see it print messages from the channel to stdout in JSON form:

```sh
$ benthos -e testing.env -c ./config.yaml
{"content":"so i like totally just tripped over my own network cables","author":{"id":"1234"}}
{"content":"like omg that is SO you!!!","author":{"id":"4321"}}
{"content":"yas totally","author":{"id":"1234"}}
{"content":"yeah","author":{"id":"4321"}}
```

It might be tempting to leave your silent surveillance bot running indefinitely but that's creepy and weird, so instead let's add the ability to respond to messages.

## Writing Messages

Writing messages to a Discord channel is pretty easy. You can feed the [`discord` output][outputs.discord] either a JSON object following the [Message Object structure][discord-message-object], or just a raw string and the structure will be created for you. Therefore we can write a hypothetical uppercasing echo bot with a simple [Bloblang mapping][bloblang]:

```yaml
pipeline:
  processors:
    - mapping: |
        root = if !this.content.has_prefix("SHOUTS BACK") {
          "SHOUTS BACK BOT SAYS " + this.content.uppercase()
        } else {
          deleted()
        }

output:
  discord:
    channel_id: ${DISCORD_CHANNEL}
    bot_token: ${DISCORD_BOT_TOKEN}
```

If we add that to the end of the first config you should see the bot respond to messages in the channel by posting an uppercase version of it with a prefix. Note that we also delete the message in our mapping if it has the same prefix that we're adding ourselves, which is a quick and dirty way of ensuring the bot doesn't echo its own messages.

## Custom Commands

Shout bot is clearly an absolute riot and a true fan favourite. However, it will get old fast. Let's make our bot more elegant by introducing some commands by swapping our plain mapping with a [`switch` processor][processors.switch]:

```yaml
pipeline:
  processors:
    - switch:
        - check: this.type == 7
          processors:
            - mapping: 'root = "Welcome to the server <@%v>!".format(this.author.id)'

        - processors:
            - mapping: 'root = deleted()'
```

By changing our mapping out to this switch we can add specialised commands for different message types, and if none of the cases match then we don't respond. Technically, we can do all of this within a single Bloblang mapping by using a match expression, but having a switch processor would also allow us to add cases where we do cool things like hit other APIs, etc.

The only case we've added here is one that activates when the message type is a specific one sent when a new person joins, and in response we give them a warm welcome. The welcome mentions the new user by injecting the user id into the welcome string with `.format(this.author.id)`, which replaces the `%v` placeholder with the author ID (the user that joined and therefore created the join message).

This response is cool but not very interactive, let's add a few commands that people can play with:

```yaml
pipeline:
  processors:
    - switch:
        - check: this.type == 7
          processors:
            - mapping: 'root = "Welcome to the server <@%v>!".format(this.author.id)'

        - check: this.content == "/joke"
          processors:
            - mapping: |
                let jokes = [
                  "What do you call a belt made of watches? A waist of time.",
                  "What does a clock do when it’s hungry? It goes back four seconds.",
                  "A company is making glass coffins. Whether they’re successful remains to be seen.",
                ]
                root = $jokes.index(timestamp_unix_nano() % $jokes.length())

        - check: this.content == "/roast"
          processors:
            - mapping: |
                let roasts = [
                  "If <@%v>'s brain was dynamite, there wouldn’t be enough to blow their hat off.",
                  "Someday you’ll go far <@%v>, and I really hope you stay there.",
                  "I’d give you a nasty look, but you’ve already got one <@%v>.",
                ]
                root = $roasts.index(timestamp_unix_nano() % $roasts.length()).format(this.author.id)

        - processors:
            - mapping: 'root = deleted()'
```

Here we have two new commands. If someone posts a message "/joke" then we respond by selecting one of several exceptionally funny jokes from a static list in the mapping.

The second new command is "/roast" and is exclusively for brave souls as the responses can be cruel and personal. The command works similarly to "/joke" with the exception being the ID of the user that made the command will be injected into the roast, as mentioning the target of the roast makes it significantly more heartbreaking (as intended).

## Hitting Other APIs

Clicking websites and browsing the internet is very difficult and most people are simply too busy for it, it'd therefore be useful if we could have our bot do some browsing for us occasionally.

The final command we're going to add to our bot is "/release", where it will hit the Github API and find out for us what the latest Benthos release is:

```yaml
pipeline:
  processors:
    - switch:
        # Other cases omitted for brevity
        - check: this.content == "/release"
          processors:
            - mapping: 'root = ""'
            - try:
              - http:
                  url: https://api.github.com/repos/benthosdev/benthos/releases/latest
                  verb: GET
              - mapping: 'root = "The latest release of Benthos is %v: %v".format(this.tag_name, this.html_url)'

    - catch:
      - log:
          fields_mapping: 'root.error = error()'
          message: "Failed to process message"
      - mapping: 'root = "Sorry, my circuits are all bent from twerking and I must have malfunctioned."'
```

Here we've added a switch case that clears the contents of the message, hits the Github API to obtain the latest Benthos release as a JSON object, and finally maps the tag name and the URL of the release to a useful message.

> We're hitting the Github API with the [generic `http` processor][processors.http], which can be configured to work with most HTTP based APIs. In fact, the Discord input and output are actually [configuration templates][templates] that use the generic HTTP components [under the hood][templates.discord].

Since this command is networked and therefore has a chance of failure we've added some [error handling][error-handling] mechanisms after the switch processor so that it'd capture errors from this new case and any new cases we add later.

Within the catch block we simply log the error for the admin to peruse and change the response message out for a generic "whoopsie daisy" apology.

## Final Words

The full config for Blob Bot (with some super secret responses redacted) can be found [in the Github repo][full-config]. To find out more about Bloblang check out [the guide page][bloblang]. To find out more about config templates check out the [templates documentation page][templates].

If you want to play with Blob Bot then [join our Discord][discord-link]. There are also some humans in there that will help you manage your disappointment when you see Blob Bot in action.

[discord-link]: https://discord.gg/6VaWjzP
[discord-applications]: https://discord.com/developers/applications
[discord-channel-id]: https://support.discord.com/hc/en-us/articles/206346498-Where-can-I-find-my-User-Server-Message-ID-
[discord-message-object]: https://discord.com/developers/docs/resources/channel#message-object
[inputs.discord]: /docs/components/inputs/discord
[outputs.discord]: /docs/components/outputs/discord
[caches]: /docs/components/caches/about
[processors.switch]: /docs/components/processors/switch
[processors.http]: /docs/components/processors/http
[bloblang]: /docs/guides/bloblang/about
[full-config]: https://github.com/benthosdev/benthos/blob/master/config/examples/discord_bot.yaml
[error-handling]: /docs/configuration/error_handling
[templates]: /docs/configuration/templating
[templates.discord]: https://github.com/benthosdev/benthos/blob/master/template/outputs/discord.yaml
