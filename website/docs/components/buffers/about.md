---
title: Buffers
sidebar_label: About
---

Benthos uses a transaction model internally for guaranteeing delivery of messages, this means that a message from an input is not acknowledged (or its offset committed, etc) until that message has been processed and either intentionally deleted or successfully delivered to all outputs. This transaction model makes Benthos safe to deploy in scenarios where data loss is unacceptable. However, sometimes it's useful to customize the way in which messages are delivered, and this is where buffers come in.

A buffer is an optional component type that comes immediately after the input layer and can be used as a way of decoupling the transaction model from components downstream such as the processing layer and outputs. This is considered an advanced component as most users will likely not benefit from a buffer, but they enable you to do things like group messages using window algorithms or intentionally weaken the delivery guarantees of the pipeline depending on the buffer you choose.

Since buffers are able to modify (or disable) the transaction model within Benthos it is important that when you choose a buffer you read its documentation to understand the implication it will have on delivery guarantees.

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="buffers"></ComponentsByCategory>

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="buffers"></ComponentSelect>