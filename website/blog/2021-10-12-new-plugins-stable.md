---
title: 'Go Plugins V2 are Ready'
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: In case you didn't see the Tweets, Discord posts and Github activity
keywords: [
    "go",
    "golang",
    "stream processor",
    "ETL",
]
tags: [ "v4", "plugins" ]
---

The [new plugin APIs](https://pkg.go.dev/github.com/Jeffail/benthos/v3/public/service) are ready to use, are being used, and [here's a video of them in action](https://youtu.be/uH6mKw-Ly0g).

import ReactPlayer from 'react-player/youtube';

<div className='container margin-vert--lg'>
  <div className='row row--no-gutters'>
    <ReactPlayer
        className='col'
        height='300px'
        url='https://www.youtube.com/embed/uH6mKw-Ly0g'
        controls={true}
    />
  </div>
</div>

The full API docs can be found at [pkg.go.dev/github.com/Jeffail/benthos/v3/public](https://pkg.go.dev/github.com/Jeffail/benthos/v3/public), and there's an example repository demonstrating a few different component plugin types at [github.com/benthosdev/benthos-plugin-example](https://github.com/benthosdev/benthos-plugin-example).
