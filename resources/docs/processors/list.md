PROCESSORS
==========

This document has been generated with `benthos --list-processors`.

## `blob_to_multi`

If a multiple part message has been encoded into a single part message using the
multi to blob processor then this processor is able to convert it back into a
multiple part message.

You can therefore use this processor when multiple Benthos instances are
bridging between message queues that don't support multiple parts.

E.g. ZMQ => Benthos(multi to blob) => Kafka => Benthos(blob to multi)

## `bounds_check`

Checks whether each message fits within certain boundaries, and drops messages
that do not (log warning message and a metric).

## `combine`

If a message queue contains multiple part messages as individual parts it can
be useful to 'squash' them back into a single message. We can then push it
through a protocol that natively supports multiple part messages.

For example, if we started with N messages each containing M parts, pushed those
messages into Kafka by splitting the parts. We could now consume our N*M
messages from Kafka and squash them back into M part messages with the combine
processor, and then subsequently push them into something like ZMQ.

## `hash_sample`

Passes on a percentage of messages, deterministically by hashing the message and
checking the hash against a valid range, and drops all others.

## `multi_to_blob`

If an input supports multiple part messages but your output does not you will
end up with each part being sent as a unique message. This can cause confusion
and complexity regarding delivery guarantees.

You can instead use this processor to encode multiple part messages into a
binary single part message, which can be converted back further down the
platform pipeline using the blob to multi processor.

E.g. ZMQ => Benthos(multi to blob) => Kafka => Benthos(blob to multi)

## `noop`

Noop is a no-op processor that does nothing, the message passes through
unchanged.

## `sample`

Passes on a percentage of messages, either randomly or sequentially, and drops
all others.

## `select_parts`

Cherry pick a set of parts from messages by their index. Indexes larger than the
number of parts are simply ignored.

The selected parts are added to the new message in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].

If none of the selected parts exist in the input message (resulting in an empty
output message) the message is dropped entirely.
