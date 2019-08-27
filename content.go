package ravenworker

import (
	"encoding/json"
)

func StringContent(s string) []byte {
	return []byte(s)
}

func JsonContent(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}
