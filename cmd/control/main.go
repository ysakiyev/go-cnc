package main

import (
	"flag"

	"go-cnc2/config"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/control"

	logger "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var conf config.Conf

func initConfig(configPath, configName string) {
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)
	if err := viper.ReadInConfig(); err != nil {
		logger.Fatalf(common.StrErrReadConfig, err)
	}
	if err := viper.Unmarshal(&conf); err != nil {
		logger.Fatalf(common.StrErrUnableDecodeConfig, err)
	}
}

func main() {
	configPath := flag.String("cp", "", "Config path")
	configName := flag.String("cn", "", "Config name")
	flag.Parse()

	if *configPath == "" || *configName == "" {
		logger.Fatalf(common.StrErrSpecifyPathName)
	}

	initConfig(*configPath, *configName)

	srv := control.NewServer(conf.Control)
	if err := srv.Run(); err != nil {
		logger.Fatalf(common.StrErrRunServ, err)
	}
}
