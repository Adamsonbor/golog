package config

import (
	"os"
	"path/filepath"
)

const (
	CONFIG_PATH = "CONFIG_PATH"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
	RootCertFile   = configFile("root-client.pem")
	RootKetFile    = configFile("root-client-key.pem")
	NobodyCertFile = configFile("nobody-client.pem")
	NobodyKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile   = configFile("model.conf")
	ACLPolicyFile  = configFile("policy.csv")
)

func configFile(filename string) string {
	dir := os.Getenv(CONFIG_PATH)
	if dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(homeDir, filename)
	}

	panic(err)
}
