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
	MetaData []Metadata

	Content Content
}

func (r *Message) UnmarshalJSON(data []byte) error {
	v := struct {
		Content  []byte     `json:"content,omitempty"`
		MetaData []Metadata `json:"metadata"`
	}{}

	if err := json.Unmarshal(data, &v); err != nil {
		return nil
	}

	r.Content = v.Content
	r.MetaData = v.MetaData
	return nil
}

func (r Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Content  []byte     `json:"content,omitempty"`
		MetaData []Metadata `json:"metadata"`
	}{
		Content:  r.Content,
		MetaData: r.MetaData,
	})
}
