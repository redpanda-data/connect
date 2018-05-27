Metrics
=======

This document outlines the main metrics exposed from Benthos, there are some
more granular metrics available that may not appear here.

## Inputs

### `input.<type>.count`

Incremented every time a new message is seen by this component.

### `input.<type>.send.success`

Incremented every time a message has been successfully propagated to the next
component (processor, buffer, output, etc), and reached its final destination
(buffer, output).

### `input.<type>.send.error`

Incremented every time a message was dispatched to the next component but was
not able to reach its final destination.

## Processors

### `processor.<type>.count`

Incremented every time a processor component sees a message.

### `processor.<type>.dropped`

Incremented every time a processor component decides to drop a message.

## Pipelines

Pipelines are generic components that contain some form of logic, usually
one or more processors.

### `pipeline.processor.count`

Incremented every time a processor pipeline receives a message.

### `pipeline.processor.dropped`

Incremented every time the processing stage resulted in a message being dropped.

### `pipeline.processor.send.success`

Incremented every time a message has been successfully propagated to the next
component (processor, buffer, output, etc), and reached its final destination
(buffer, output).

### `pipeline.processor.send.error`

Incremented every time a message was dispatched to the next component but was
not able to reach its final destination.

## Buffers

### `buffer.backlog`

This is a gauge of the current size of the buffers backlog in bytes.

### `buffer.write.count`

Incremented every time a message has been received and successfully written to
the buffer.

### `buffer.write.error`

Incremented every time a message has been received but was not successfully
written to the buffer.

### `buffer.read.count`

Incremented every time a message has been read from the buffer.

### `buffer.read.error`

Incremented every time an error was returned when trying to read a new message
from the buffer.

### `buffer.send.success`

Incremented every time a read message from the buffer has been sent and reached
its final destination, and was therefore removed from the buffer.

### `buffer.send.error`

Incremented every time a message was dispatched to the next component but was
not able to reach its final destination. The message remains in the buffer in
this case.

## Outputs

### `output.<type>.count`

Incremented every time a new message is seen by this component.

### `output.<type>.send.success`

Incremented every time a message has been successfully written to the
appropriate output.

### `output.<type>.send.error`

Incremented every time a message failed to write to the appropriate output.
