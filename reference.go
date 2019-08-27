package ravenworker

type Reference struct {
	AckID   string `json:"ack_id"`
	EventID string `json:"event_id"`
	FlowID  string `json:"flow_id"`
}
