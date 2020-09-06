package processor

import (
	"database/sql"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"

	// SQL Drivers
	_ "github.com/go-sql-driver/mysql"
)

//------------------------------------------------------------------------------

type sqlResultCodecDeprecated func(rows *sql.Rows, msg types.Message) error

func sqlResultJSONArrayCodecDeprecated(rows *sql.Rows, msg types.Message) error {
	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}
	jArray := []interface{}{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuesWrapped := make([]interface{}, len(columnNames))
		for i := range values {
			valuesWrapped[i] = &values[i]
		}
		if err := rows.Scan(valuesWrapped...); err != nil {
			return err
		}
		jObj := map[string]interface{}{}
		for i, v := range values {
			switch t := v.(type) {
			case string:
				jObj[columnNames[i]] = t
			case []byte:
				jObj[columnNames[i]] = string(t)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				jObj[columnNames[i]] = t
			case float32, float64:
				jObj[columnNames[i]] = t
			case bool:
				jObj[columnNames[i]] = t
			default:
				jObj[columnNames[i]] = t
			}
		}
		jArray = append(jArray, jObj)
	}
	if msg.Len() > 0 {
		p := msg.Get(0)
		msg.SetAll([]types.Part{p})
		return msg.Get(0).SetJSON(jArray)
	}
	msg.Append(message.NewPart(nil))
	return msg.Get(0).SetJSON(jArray)
}

func strToSQLResultCodecDeprecated(codec string) (sqlResultCodecDeprecated, error) {
	switch codec {
	case "json_array":
		return sqlResultJSONArrayCodecDeprecated, nil
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unrecognised result codec: %v", codec)
}

//------------------------------------------------------------------------------

func (s *SQL) doExecuteDeprecated(args ...interface{}) error {
	_, err := s.query.Exec(args...)
	return err
}

// processMessageDeprecated.
// TODO: V4 Remove this
func (s *SQL) processMessageDeprecated(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	result := msg.Copy()

	spans := tracing.CreateChildSpans(TypeSQL, result)

	args := make([]interface{}, len(s.args))
	for i, v := range s.args {
		args[i] = v.String(0, result)
	}
	var err error
	if s.resCodecDeprecated == nil {
		if err = s.doExecuteDeprecated(args...); err != nil {
			err = fmt.Errorf("failed to execute query: %v", err)
		}
	} else {
		var rows *sql.Rows
		if rows, err = s.query.Query(args...); err == nil {
			defer rows.Close()
			if err = s.resCodecDeprecated(rows, result); err != nil {
				err = fmt.Errorf("failed to apply result codec: %v", err)
			}
		} else {
			err = fmt.Errorf("failed to execute query: %v", err)
		}
	}
	if err != nil {
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			spans[i].LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
			return nil
		})
		s.log.Errorf("SQL error: %v\n", err)
		s.mErr.Incr(1)
	}
	for _, s := range spans {
		s.Finish()
	}

	s.mSent.Incr(int64(result.Len()))
	s.mBatchSent.Incr(1)

	return []types.Message{result}, nil
}

//------------------------------------------------------------------------------
