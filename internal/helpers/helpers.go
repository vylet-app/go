package helpers

import (
	"fmt"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func ToStringPtr(str string) *string {
	return &str
}

func ImageCidToCdnUrl(cid string, size string) string {
	return fmt.Sprintf("https://cdn.vylet.app/%s/%s@png", cid, size)
}

func StrToCid(str string) cid.Cid {
	h, err := multihash.Sum([]byte(str), multihash.IDENTITY, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

func StrToLexLink(str string) util.LexLink {
	cid := StrToCid(str)
	return (util.LexLink)(cid)
}

func ValidateUris(uris []string) (bool, error) {
	for _, uri := range uris {
		if _, err := syntax.ParseATURI(uri); err != nil {
			return false, err
		}
	}
	return true, nil
}
