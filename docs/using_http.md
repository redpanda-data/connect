Using HTTP
==========

Benthos supports using HTTP as a stream input and output. This method is useful
for debugging systems linked to a benthos output as data can be piped through
using simple curl requests.

## Input

### HTTP Server

Currently the only supported HTTP input type, also only supports POST requests
at this time. This input type will only preserve the BODY of the request.

#### Config

```yaml
http_server:
	address: localhost:8080
	path: /post
	timeout_ms: 5000
```

You need to specify the address to bind to as well as the specific path to serve
from. You may also specify a timeout.

##### Singlepart and Multipart

By default all POST requests are received and treated as single part unless the
`Content-Type` header is set to one of the following multipart formats:

`multipart/mixed; boundary=<boundary>`. Benthos recognises this header and
parses the body according to RFC 2046 (http://www.ietf.org/rfc/rfc2046.txt).

`application/x-benthos-multipart`. This is a special case where the body is
parsed as the following format:

```
- Four bytes containing number of message parts in big endian
- For each message part:
	+ Four bytes containing length of message part in big endian
	+ Content of message part

                                         # Of bytes in message 2
                                         |
# Of message parts (big endian)          |           Content of message 2
|                                        |           |
v                                        v           v
| 0| 0| 0| 2| 0| 0| 0| 5| h| e| l| l| o| 0| 0| 0| 5| w| o| r| l| d|
  0  1  2  3  4  5  6  7  8  9 10 11 13 14 15 16 17 18 19 20 21 22
              ^           ^
              |           |
              |           Content of message 1
              |
              # Of bytes in message 1 (big endian)
```

To see an example of how to use this encoding look at `./types/message.go`.

## Output

### HTTP Client

Currently the only supported HTTP output type. Messages passed through Benthos
will be sent to an HTTP server synchronously as POST requests.

#### Config

```yaml
http_client:
	url: localhost:8081/post
	timeout_ms: 5000
	retry_period_ms: 1000
```

You need to specify the URL to send to. You may also specify a timeout as well
as a retry period to wait between failed attempt retries.

##### Singlepart and Multipart

Singlepart messages will be sent as a single blob with the `Content-Type` header
`application/octet-stream`. Multipart messages will be sent using RFC 2046
(http://www.ietf.org/rfc/rfc2046.txt).
