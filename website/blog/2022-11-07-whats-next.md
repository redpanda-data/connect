---
title: What's Next for Benthos?
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: A summary of what's going on and what's coming up
keywords: [
    "v4",
    "roadmap",
    "go",
    "golang",
    "stream processor",
    "ETL",
]
tags: [ "v4", "studio" ]
---

A few months ago it was announced that [v4 was coming](/blog/2022/03/03/v4-coming). Well, that happened... and also [a bunch more releases](https://github.com/benthosdev/benthos/blob/main/CHANGELOG.md) since then. Now that the fundamentals have been tidied up considerably we're adding new features and they're coming in hot and heavy (and a bit sticky). Almost entirely parallel to this effort is the work on the new [Benthos Studio](https://studio.benthos.dev), which is a visual application for creating, modifying and sharing Benthos (and Bloblang) configs.

Things are certainly moving fast. However, we've clearly been lacking in the blog department. To remedy this here's a summary of all the stuff we would have blogged about if we had more time and bother.

<!--truncate-->

## Community Growth

Benthos has now surpassed [150 contributors][contributors], and between our [discord and slack communities][community] we're steadily approaching 1,000 gossips, at which point we'll be too mainstream to be considered cool and hip. This site has daily traffic in the thousands and Benthos itself is downloaded around 2,000 times per day. The [Jeffail youtube channel][jeffail-youtube], which features all of our Benthos and stream processing related video content, is also growing steadily in both content and precious subscribers. I'd promise none of this traffic is bots but I actually don't know.

Another major milestone reached is that I watched witlessly as my wife gave birth to a human child, she's never done that before. In order to support the incoherent squawker (and our new child) I took a month away from the project. If you're a keen follower of the open source work going on in Benthos land then you'll be aware that it has historically been a mostly one-person show (me), but I'm thrilled (perhaps a bit sour) to report that during my absence the project carried on at pretty much the same intense pace.

These are all signs that our codebase, community support and developer ecosystems are becoming more decentralised. This trend in growth is entirely organic (besides a few stickers sent to conferences) and mostly based on the volunteer effort of professionals that use Benthos in their daily work. We're also blessed with a number of hobbyists getting involved just because they enjoy it, we've tried sapping the fun out of the project but they just won't go away. For now I'm still retaining absolute control over what gets merged but the bus factor is certainly becoming less significant.

The trends I'm outlining here should be soothing for Benthos fans, we're growing, we're managing the growth just fine, and you can safely bet on the project continuing on that path. Over the years some users (active or prospective) have expressed concern that Benthos does not have an organisation around it with a pot of money, their worry being the longevity of the project is at risk. I've obviously not agreed as I haven't yet sought after such a pot of money (more on that later in this post). Over time it has become easier to shrug those concerns off as we continue to move at a pace that'd easily beat the expectations of any funded operation.

## Benthos Studio

I've been quietly working on [Benthos Studio][benthos-studio] pretty much since the early years of Benthos itself. The idea being a visual editing application to complement the development and running of Benthos configs. What I like about having this exist as an entirely separate application is that the main project is still incentivised to make configuration as easy and intuitive as possible, we can't just rely on visual tooling to plug gaps in ergonomics or observability. This allows Studio to focus on lifting that experience up a few notches as an optional extra.

If you aren't familiar with it then here's a quick video introduction:

import ReactPlayer from 'react-player/youtube';

<div className='container margin-vert--lg'>
  <div className='row row--no-gutters'>
    <ReactPlayer
        className='col'
        height='300px'
        url='https://youtu.be/uvbp2LCmQMY'
        controls={true}
    />
  </div>
</div>

Studio is currently offered as an open beta with new stuff coming in constantly. The main directions we're heading in are:

- More storage flexibility, including allowing you to use Studio to view and configure streams running in your own local deployments
- More execution visibility, better visuals and ability to dig into what's happening within a Benthos configuration
- A smoother configuration experience for beginners, including wizards for building new stream pipelines

You may also have noticed that Benthos Studio is not open source. This is because I believe Studio is a good bet at putting together a scalable monetisation strategy that complements the goals of the open source work rather than causing friction against it, a sort of golden path for building a business around an existing open source ecosystem. It will take a while to work out which features should be paid for, but there will always be a free tier that provides all the interesting bits.

## Monetisation

Uh oh! He said monetisation, bag him! Monetising an open source project is a hefty topic and gets lots of people mad and sweaty, one far end of the spectrum believing it's entirely counter to the open source movement, the other end refusing to take seriously any project that _isn't_ monetised.

The reality is obviously that neither extreme holds merit. People need to eat and when projects reach a certain size the time required to maintain them goes well beyond the scope of a fun hobby, but conversely there's nothing preventing a non-funded project from outperforming a funded one even with enterpisey things like support and product stability. A common feedback we get is that our stability and support goes well beyond what people are used to even with paid products. So, given that and all the stuff I outlined at the beginning of this post, do we even need funding?

Well yes, we do. In fact, as the main driving force behind Benthos I've been funded since day one, just not directly. I get returns from my Benthos work in the form of job opportunities that feed back into the project, support contracts, issue bounties and sponsorships dotted throughout. This is a similar story for many open source maintainers, where if you keep yourself from burning out then a project can get very far indeed without needing to commit to a more scalable business model.

This set up has been working out just fine but it's far from ideal. I spend a lot of energy keeping these gigs going which could be much better spent growing a team around a paid product. This team would then steward and give back to the open source community and we would all win, the only losers being people that hate teams, products or the abstract concept of winning, or me as a person.

Many may have jumped straight into the deep end and asked for VC funding up front. If/when we choose to go down that route we're locked in for the whole ride, where in abstract terms if that aforementioned golden path of monetisation gives us problems then we're going to be pressured into following a new path that prioritises the venture and not the open source. Well, that's life, every decision has risks and in reality we'd still do well as a community in any outcome, but for now I've been exploring that golden path and seeing how far I can get on my own.

So that's how we got to here, we have a ship that seems pretty much sea worthy and without any obligations or milestones to deliver. Dare I remain bootstrapped? Perhaps I should join a fleet? Do we finally rocket boost these salty decks and aim for the stars? Is that a dumb metaphor? Well keep checking this blog to find out a few months after all the Discord and Slack users do.

## Supporting the Project

The longer this project remains sustainable without obligation the more we can experiment freely and independently. If you want to help stretch that progress further and maybe help keep us on the golden path then [get involved in the project][open-source], get your organisation to [sponsor my work][sponsor-jeffail], or consider some of the [paid support options][paid-support] we're currently offering.

[benthos-studio]: https://studio.benthos.dev
[contributors]: https://github.com/benthosdev/benthos/graphs/contributors
[community]: /community
[jeffail-youtube]: https://www.youtube.com/c/Jeffail
[sponsor-jeffail]: https://github.com/sponsors/Jeffail
[open-source]: https://github.com/benthosdev/benthos
[paid-support]: /support#paid-services
