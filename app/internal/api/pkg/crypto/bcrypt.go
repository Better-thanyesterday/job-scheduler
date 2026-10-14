package crypto

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"golang.org/x/crypto/bcrypt"
)

func GeneratePassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	return string(bytes), err
}

func VerifyPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(password), []byte(hash))
	return err == nil
}

func GenerateUserName(salt string) (string, error) {
	randbuf := make([]byte, 16)
	_, err := rand.Read(randbuf)
	if err != nil {
		return "", err
	}
	input := append([]byte(salt), randbuf...)
	hash := sha256.Sum256(input)
	num := binary.BigEndian.Uint64(hash[:8])
	max := uint64(1)
	for i := 0; i < 12; i++ {
		max *= 10
	}
	val := num % max
	format := fmt.Sprintf("%%0%dd", 12)
	return fmt.Sprintf(format, val), nil
}
