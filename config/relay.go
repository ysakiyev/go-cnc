package config

import "time"

type RelayConf struct {
	Address    string
	SigningKey  string
	LogPath    string

	KaepMinTime               time.Duration
	KaepPermitWithoutStream   bool
	KaspMaxConnectionIdle     time.Duration
	KaspMaxConnectionAgeGrace time.Duration
	KaspTime                  time.Duration
	KaspTimeout               time.Duration
}
