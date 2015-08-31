/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

/*
Package broker - Implements types used for routing an input to outputs in one to many fashion, with
the potential for either lock-stepped and unbuffered queuing or with buffers for each output.

The broker expects an array of outputs, which can be changed at runtime, these outputs must be
wrapped in an Agent type, which determines whether the output should be lock-stepped or buffered.

The broker loop is like this:

for each input
	Input -> Broker: send_message()
	for each agent
		Broker -> Agent: dispatch_message()
	end
	for each agent
		Broker -> Agent: confirm_dispatched()
	end
end

A lock-stepped (unbuffered) agent would look like this:

for each message dispatch
	Broker -> Agent: dispatch_message()
	Agent -> Output: send_message()
	Agent -> Output: confirm_send()
	Broker -> Agent: confirm_dispatched()
end

An asynchronous (buffered) agent would look like this:

(async) for each message dispatch
	Broker -> Agent: dispatch_message()
	Agent -> Agent: push_to_queue()

	while buffered > threshold
		Agent -> Agent: wait_until_below_threshold()
	end
	Broker -> Agent: confirm_dispatched()
end
(async) for each queued message
	Agent -> Output: send_message()
	Agent -> Output: confirmed_message_sent()
	Agent -> Agent: update_num_buffered()
end

Note that the buffer is not unlimited, as we must set a threshold and it is up to the agent to track
how much memory is consumed by the buffer.
*/
package broker
