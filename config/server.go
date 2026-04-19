package config

import "time"

type ServerConf struct {
	Address string // Address {ip}:{port}
	LogPath string

	// gRPC configs
	KaepMinTime               time.Duration // If a client pings more than once every 5 seconds, terminate the connection
	KaepPermitWithoutStream   bool          // Allow pings even when there are no active streams
	KaspMaxConnectionIdle     time.Duration // If a client is idle for 15 seconds, send a GOAWAY
	KaspMaxConnectionAgeGrace time.Duration // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	KaspTime                  time.Duration // Ping the client if it is idle for 5 seconds to ensure the connection is still active
	KaspTimeout               time.Duration // Wait 1 second for the ping ack before assuming the connection is dead
}
