package ravenworker

import (
	"encoding/json"
)

func StringContent(s string) Content {
	return Content([]byte(s))
}

func JsonContent(v interface{}) Content {
	data, _ := json.Marshal(v)
	return Content(data)
}
