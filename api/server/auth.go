package server

import (
	"fmt"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/golang-jwt/jwt/v5"
)

func initSigningMethods() {
	if jwt.GetSigningMethod("ES256K") == nil {
		jwt.RegisterSigningMethod("ES256K", func() jwt.SigningMethod {
			return &SigningMethodAtproto{
				alg: "ES256K",
			}
		})
	}

	if jwt.GetSigningMethod("ES256") == nil {
		jwt.RegisterSigningMethod("ES256", func() jwt.SigningMethod {
			return &SigningMethodAtproto{
				alg: "ES256",
			}
		})
	}
}

type SigningMethodAtproto struct {
	alg string
}

func (sm *SigningMethodAtproto) Verify(signingString string, sig []byte, key any) error {
	pub, ok := key.(atcrypto.PublicKey)
	if !ok {
		return fmt.Errorf("wrong key type")
	}
	return pub.HashAndVerifyLenient([]byte(signingString), sig)
}

func (sm *SigningMethodAtproto) Sign(signingString string, key any) ([]byte, error) {
	return nil, fmt.Errorf("signing not supported")
}

func (sm *SigningMethodAtproto) Alg() string {
	return sm.alg
}
