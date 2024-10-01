package pglogicalstream

type StreamMessageChanges struct {
	Operation string `json:"operation"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	// For deleted messages - there will be old changes if replica identity set to full or empty changes
	Data map[string]any `json:"data"`
}

type StreamMessage struct {
	Lsn     *string                `json:"lsn"`
	Changes []StreamMessageChanges `json:"changes"`
}
