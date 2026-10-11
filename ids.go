package shardpilot

import (
	"strings"

	"github.com/shardpilot/shardpilot-go/internal/uuidv7"
)

func newEventID() (string, error) {
	return uuidv7.New()
}

// Host IDs are normalized before queueing, so wire, retry and spool identities agree.
func (c *Client) validateHostEventID(id string) (string, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" || validEventID(id) {
		return id, nil
	}
	c.stats.dropped.Add(1)
	c.stats.setLastError(ErrInvalidEventID.Error())
	return "", ErrInvalidEventID
}

func validEventID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch i {
		case 8, 13, 18, 23:
			if id[i] != '-' {
				return false
			}
		default:
			if !(id[i] >= '0' && id[i] <= '9' || id[i] >= 'a' && id[i] <= 'f') {
				return false
			}
		}
	}
	return (id[14] == '4' || id[14] == '5' || id[14] == '7') &&
		(id[19] == '8' || id[19] == '9' || id[19] == 'a' || id[19] == 'b')
}
