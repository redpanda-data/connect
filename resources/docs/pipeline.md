Pipeline
========

Within a Benthos configuration, in between `input` and `output`, is a `pipeline`
section. This section describes an array of [processors][processors] that are to
be applied to *all* messages, and are not bound to any particular input or
output.

If you have processors that are heavy on CPU and aren't specific to a certain
input or output they are best suited for the pipeline section. It is
advantageous to use the pipeline section as it allows you to set an explicit
number of parallel threads of execution which, should ideally match the number
of available logical CPU cores.

If [a buffer is chosen][buffers] these processors are applied to messages read
from it. It is therefore possible to use buffers to distribute messages from a
single input across multiple parallel processing threads.

[processors]: ./processors
[buffers]: ./buffers
