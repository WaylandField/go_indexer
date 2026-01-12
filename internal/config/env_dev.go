//go:build dev

package config

import (
	"os"

	"github.com/joho/godotenv"
)

func loadDotEnv() error {
	if _, err := os.Stat(".env"); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return godotenv.Load(".env")
}
