---
title: bounds_check
slug: bounds_check
type: processor
status: stable
categories: ["Utility"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the corresponding source file under internal/impl/<provider>.
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Removes messages (and batches) that do not fit within certain size boundaries.


<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

<TabItem value="common">

```yml
# Common config fields, showing default values
label: ""
bounds_check:
  max_part_size: 1073741824
  min_part_size: 1
```

</TabItem>
<TabItem value="advanced">

```yml
# All config fields, showing default values
label: ""
bounds_check:
  max_part_size: 1073741824
  min_part_size: 1
  max_parts: 100
  min_parts: 1
```

</TabItem>
</Tabs>

## Fields

### `max_part_size`

The maximum size of a message to allow (in bytes)


Type: `int`  
Default: `1073741824`  

### `min_part_size`

The minimum size of a message to allow (in bytes)


Type: `int`  
Default: `1`  

### `max_parts`

The maximum size of message batches to allow (in message count)


Type: `int`  
Default: `100`  

### `min_parts`

The minimum size of message batches to allow (in message count)


Type: `int`  
Default: `1`  

