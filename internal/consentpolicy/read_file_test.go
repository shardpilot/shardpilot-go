package consentpolicy

import "os"

func readFile(name string) (string, error) {
	body, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
