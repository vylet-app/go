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

func ToIntPtr(num int) *int {
	return &num
}

func ToInt64Ptr(num int64) *int64 {
	return &num
}

func ImageCidToCdnUrl(host, size, did, cid string) string {
	// http://localhost:9525/img/fullsize/plain/did:plc:oisofpd7lj26yvgiivf3lxsi/bafkreiesoy5p2kcc73o7qv4iywlxnzssdjivvsoa3ivhnaqy2uyjgmmnbq@jpeg
	return fmt.Sprintf("%s/img/%s/plain/%s/%s@jpeg", host, size, did, cid)
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
