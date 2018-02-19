PROCESSORS
==========

This document has been generated with `benthos --list-processors`.

## `archive`

Archives all the parts of a message into a single part according to the selected
archive type. Supported archive types are: tar, binary (I'll add more later).

Some archive types (such as tar) treat each archive item (message part) as a
file with a path. Since message parts only contain raw data a unique path must
be generated for each part. This can be done by using function interpolations on
the 'path' field as described [here](../config_interpolation.md#functions). For
types that aren't file based (such as binary) the file field is ignored.

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

If a message received has more parts than the 'combine' amount it will be sent
unchanged with its original parts. This occurs even if there are cached parts
waiting to be combined, which will change the ordering of message parts through
the platform.

When a message part is received that increases the total cached number of parts
beyond the threshold it will have _all_ of its parts appended to the resuling
message. E.g. if you set the threshold at 4 and send a message of 2 parts
followed by a message of 3 parts then you will receive one output message of 5
parts.

## `compress`

Compresses parts of a message according to the selected algorithm. Supported
compression types are: gzip (I'll add more later). If the list of target parts
is empty the compression will be applied to all message parts.

The 'level' field might not apply to all algorithms.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

## `decompress`

Decompresses the parts of a message according to the selected algorithm.
Supported decompression types are: gzip (I'll add more later). If the list of
target parts is empty the decompression will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

Parts that fail to decompress (invalid format) will be removed from the message.
If the message results in zero parts it is skipped entirely.

## `hash_sample`

Passes on a percentage of messages deterministically by hashing selected parts
of the message and checking the hash against a valid range, dropping all others.

For example, a 'hash_sample' with 'retain_min' of 0.0 and 'remain_max' of 50.0
will receive half of the input stream, and a 'hash_sample' with 'retain_min' of
50.0 and 'retain_max' of 100.1 will receive the other half.

The part indexes can be negative, and if so the part will be selected from the
end counting backwards starting from -1. E.g. if index = -1 then the selected
part will be the last part of the message, if index = -2 then the part before
the last element with be selected, and so on.

## `insert_part`

Insert a new message part at an index. If the specified index is greater than
the length of the existing parts it will be appended to the end.

The index can be negative, and if so the part will be inserted from the end
counting backwards starting from -1. E.g. if index = -1 then the new part will
become the last part of the message, if index = -2 then the new part will be
inserted before the last element, and so on. If the negative index is greater
than the length of the existing parts it will be inserted at the beginning.

This processor will interpolate functions within the 'content' field, you can
find a list of functions [here](../config_interpolation.md#functions).

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

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

## `set_json`

Parses a message part as a JSON blob, sets a path to a value, and writes the
modified JSON back to the message part.

Values can be any value type, including objects and arrays. When using YAML
configuration files a YAML object will be converted into a JSON object, i.e.
with the config:

``` yaml
set_json:
  part: 0
  path: some.path
  value:
    foo:
      bar: 5
```

The value will be converted into '{"foo":{"bar":5}}'. If the YAML object
contains keys that aren't strings those fields will be ignored.

The part index can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

## `split`

Extracts the individual parts of a multipart message and turns them each into a
unique message. It is NOT necessary to use the split processor when your output
only supports single part messages, since those message parts will automatically
be sent as individual messages.

Please note that when you split a message you will lose the coupling between the
acknowledgement from the output destination to the origin message at the input
source. If all but one part of a split message is successfully propagated to the
destination the source will still see an error and may attempt to resent the
entire message again.

The split operator is useful for breaking down messages containing a large
number of parts into smaller batches by using the split processor followed by
the combine processor. For example:

1 Message of 1000 parts -> Split -> Combine 10 -> 100 Messages of 10 parts.

## `unarchive`

Unarchives parts of a message according to the selected archive type into
multiple parts. Supported archive types are: tar, binary. If the list of target
parts is empty the unarchive will be applied to all message parts.

When a part is unarchived it is split into more message parts that replace the
original part. If you wish to split the archive into one message per file then
follow this with the 'split' processor.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

Parts that are selected but fail to unarchive (invalid format) will be removed
from the message. If the message results in zero parts it is skipped entirely.
