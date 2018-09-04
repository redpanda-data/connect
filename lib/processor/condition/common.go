package condition

import "encoding/json"

type rawJSONValue []byte

func (r *rawJSONValue) UnmarshalJSON(bytes []byte) error {
	*r = append((*r)[0:0], bytes...)
	return nil
}

func (r rawJSONValue) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return r, nil
}

func (r *rawJSONValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var yamlObj interface{}
	if err := unmarshal(&yamlObj); err != nil {
		return err
	}

	var convertMap func(m map[interface{}]interface{}) map[string]interface{}
	convertMap = func(m map[interface{}]interface{}) map[string]interface{} {
		newMap := map[string]interface{}{}
		for k, v := range m {
			keyStr, ok := k.(string)
			if !ok {
				continue
			}
			newVal := v
			if iMap, isIMap := v.(map[interface{}]interface{}); isIMap {
				newVal = convertMap(iMap)
			}
			newMap[keyStr] = newVal
		}
		return newMap
	}

	if iMap, isIMap := yamlObj.(map[interface{}]interface{}); isIMap {
		yamlObj = convertMap(iMap)
	}

	rawJSON, err := json.Marshal(yamlObj)
	if err != nil {
		return err
	}

	*r = append((*r)[0:0], rawJSON...)
	return nil
}

func (r rawJSONValue) MarshalYAML() (interface{}, error) {
	var val interface{}
	if err := json.Unmarshal(r, &val); err != nil {
		return nil, err
	}
	return val, nil
}
