package utils

import (
	"crypto/sha256"
	"fmt"
)

func Hash(item any) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", item)))

	return fmt.Sprintf("%x", h.Sum(nil))
}
