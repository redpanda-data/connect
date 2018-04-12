Pipeline
========

Within a Benthos configuration, in between `input` and `output`, is a `pipeline`
section. This section describes an array of [processors][processors] that are to
be applied to *all* messages, and are not bound to any particular input or
output. These processors are applied before the messages are sent to a buffer.

If you have processors that are heavy on CPU and aren't specific to a certain
input or output they are best suited for the pipeline section. It is
advantageous to use the pipeline section as it allows you to set an explicit
number of parallel threads of execution.

[processors]: ./processors
