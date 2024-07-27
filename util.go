package redistools

import (
	"crypto/rand"
	"encoding/hex"
)

func randomClaim() string {
	claim := make([]byte, 16)
	_, err := rand.Read(claim)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(claim)
}
