package indexer

import (
	"fmt"

	vyletkafka "github.com/vylet-app/go/bus/proto"
)

func firehoseEventToUri(evt *vyletkafka.FirehoseEvent) string {
	return fmt.Sprintf("at://%s/%s/%s", evt.Did, evt.Commit.Collection, evt.Commit.Rkey)
}
