package ravenworker

import "encoding/json"

// NewMessage will return a new empty Message struct
//
//     message := NewMessage()
//         .Reference(ref)
//         .Content(content)
//         .Metadata(metadata)
func NewMessage() Message {
	return Message{}
}

type Message struct {
	ref *Reference

	metaData map[string]interface{}

	content []byte
}

func (r *Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Content  string                 `json:"content,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}{
		Content:  string(r.content),
		Metadata: r.metaData,
	})
}

func (m Message) Reference(ref *Reference) Message {
	m.ref = ref
	return m
}

func (m Message) Metadata(metadata map[string]interface{}) Message {
	m.metaData = metadata
	return m
}

func (m Message) Content(content []byte) Message {
	m.content = content
	return m
}
