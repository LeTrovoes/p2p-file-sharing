package message

import "encoding/json"

type Message struct {
	Id      int      `json:"id"`
	Command string   `json:"command"`
	File    string   `json:"file"`
	Files   []string `json:"details"`
}

func (msg *Message) ToByteArray() ([]byte, error) {
	return json.Marshal(msg)
}

func MessageFromByteArray(data []byte) (msg Message, err error) {
	err = json.Unmarshal(data, &msg)
	return
}
