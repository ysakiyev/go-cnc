package config

import "time"

type ControlConf struct {
	Address    string
	RelayAddr  string
	SigningKey  string
	LogPath    string

	KaepMinTime               time.Duration
	KaepPermitWithoutStream   bool
	KaspMaxConnectionIdle     time.Duration
	KaspMaxConnectionAgeGrace time.Duration
	KaspTime                  time.Duration
	KaspTimeout               time.Duration
}
