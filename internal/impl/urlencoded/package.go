package urlencoded

import "net/url"

func toMap(values url.Values) map[string]interface{} {
	root := make(map[string]interface{})

	for k, v := range values {
		if len(v) == 1 {
			root[k] = v[0]
		} else {
			root[k] = v
		}
	}

	return root
}
