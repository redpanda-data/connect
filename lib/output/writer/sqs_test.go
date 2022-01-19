package writer

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

func TestSQSHeaderCheck(t *testing.T) {
	type testCase struct {
		k, v     string
		expected bool
	}

	tests := []testCase{
		{
			k: "foo", v: "bar",
			expected: true,
		},
		{
			k: "foo.bar", v: "bar.baz",
			expected: true,
		},
		{
			k: "foo_bar", v: "bar_baz",
			expected: true,
		},
		{
			k: "foo-bar", v: "bar-baz",
			expected: true,
		},
		{
			k: ".foo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: ".bar",
			expected: true,
		},
		{
			k: "f..oo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba..r",
			expected: true,
		},
		{
			k: "aws.foo", v: "bar",
			expected: false,
		},
		{
			k: "amazon.foo", v: "bar",
			expected: false,
		},
		{
			k: "foo.", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "bar.",
			expected: true,
		},
		{
			k: "fo$o", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba$r",
			expected: true,
		},
		{
			k: "foo_with_10_numbers", v: "bar",
			expected: true,
		},
		{
			k: "foo", v: "bar_with_10_numbers and a space",
			expected: true,
		},
		{
			k: "foo with space", v: "bar",
			expected: false,
		},
		{
			k: "iso_date", v: "1997-07-16T19:20:30.45+01:00",
			expected: true,
		},
		{
			k: "has_a_char_in_the_valid_range", v: "#x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF - Ѱ",
			expected: true,
		},
	}

	for i, test := range tests {
		if act, exp := isValidSQSAttribute(test.k, test.v), test.expected; act != exp {
			t.Errorf("Unexpected result for test '%v': %v != %v", i, act, exp)
		}
	}
}

type mockSqs struct {
	sqsiface.SQSAPI
	fn func(*sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
}

func (m *mockSqs) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	return m.fn(input)
}
