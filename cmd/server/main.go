package main

import (
	"flag"
	"go-cnc2/config"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/server"
	"io"
	"log"
	"os"

	logger "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var conf config.Conf

func initConfig(configPath, configName string) {
	// Reading and parsing config file
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)

	if err := viper.ReadInConfig(); err != nil {
		logger.Fatalf(common.StrErrReadConfig, err)
	}
	err := viper.Unmarshal(&conf)
	if err != nil {
		logger.Fatalf(common.StrErrUnableDecodeConfig, err)
	}
}

func initLogs() {
	// initialize logs
	logFile, err := os.OpenFile(conf.Server.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	logger.SetFormatter(&logger.JSONFormatter{})
	mw := io.MultiWriter(os.Stdout, logFile)
	logger.SetOutput(mw)
}

func main() {
	configPath := flag.String("cp", "", "Config path")
	configName := flag.String("cn", "", "Config name")
	flag.Parse()

	if *configPath == "" || *configName == "" {
		logger.Fatalf(common.StrErrSpecifyPathName)
	}

	initConfig(*configPath, *configName)
	//initLogs()

	srv := server.NewServer(conf)
	err := srv.Run(conf.Server)
	if err != nil {
		logger.Fatalf(common.StrErrRunServ, err)
	}
}
