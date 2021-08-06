---
title: Buffers
sidebar_label: About
---

Benthos uses a transaction based model for guaranteeing delivery of messages without the need for a buffer. This ensures that messages are never acknowledged from a source until the message has left the target sink.

However, sometimes the transaction model is undesired, in which case there are a range of buffer options available which decouple input sources from the rest of the Benthos pipeline. Buffers can therefore solve a number of typical streaming problems but come at the cost of weakening the delivery guarantees of your pipeline.

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="buffers"></ComponentsByCategory>

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="buffers"></ComponentSelect>