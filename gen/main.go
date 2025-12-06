package main

import (
	"reflect"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/mst"
	"github.com/vylet-app/go/api/vylet"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	var typVals []any
	for _, typ := range mst.CBORTypes() {
		typVals = append(typVals, reflect.New(typ).Elem().Interface())
	}

	genCfg := cbg.Gen{
		MaxStringLength: 1_000_000,
	}

	if err := genCfg.WriteMapEncodersToFile("api/vylet/cbor_gen.go", "vylet",
		vylet.ActorProfile{},
	); err != nil {
		panic(err)
	}

	if err := genCfg.WriteMapEncodersToFile("api/atproto/cbor_gen.go", "atproto",
		atproto.RepoStrongRef{},
	); err != nil {
		panic(err)
	}
}
