package ravenworker

import "encoding/json"

// NewMessage will return a new empty Message struct
//
//     message := NewMessage()
func NewMessage() Message {
	return Message{}
}

type Content []byte

type Message struct {
	MetaData map[string]interface{}

	Content Content
}

func (r *Message) UnmarshalJSON(data []byte) error {
	v := struct {
		Content  string                 `json:"content,omitempty"`
		MetaData map[string]interface{} `json:"metadata,omitempty"`
	}{}

	if err := json.Unmarshal(data, &v); err != nil {
		return nil
	}

	r.Content = []byte(v.Content)
	r.MetaData = v.MetaData
	return nil
}

func (r *Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Content  string                 `json:"content,omitempty"`
		MetaData map[string]interface{} `json:"metadata,omitempty"`
	}{
		Content:  string(r.Content),
		MetaData: r.MetaData,
	})
}
