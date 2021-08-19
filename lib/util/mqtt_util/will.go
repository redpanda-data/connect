package mqtt_util

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/docs"
)

// Configuration for the last will message that the broker emits, should
// benthos exit abnormally.
type Will struct {
	QoS      uint8  `json:"qos" yaml:"qos"`
	Retained bool   `json:"retained" yaml:"retained"`
	Topic    string `json:"topic" yaml:"topic"`
	Payload  string `json:"payload" yaml:"payload"`
}

// Return an empty will, meaning last will message should not be registered.
func EmptyWill() Will {
	return Will{}
}

// Validate the Will configuration and return nil or error accordingly.
func (w *Will) Validate() error {
	if w.Topic == "" {
		if w.Payload != "" || w.QoS > 0 || w.Retained {
			return errors.New("include topic to register a last will")
		}
	}

	return nil
}

// FieldSpec for a last will message registration.
func WillFieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced(
		"will", "Set last will message in case of Benthos failure",
	).WithChildren(
		docs.FieldCommon("qos", "Set QoS for last will message."),
		docs.FieldCommon("retained", "Set retained for last will message."),
		docs.FieldCommon("topic", "Set topic for last will message."),
		docs.FieldCommon("payload", "Set payload for last will message."),
	)
}
