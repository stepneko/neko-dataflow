package utils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger = nil

func InitLogger() {
	config := zap.NewDevelopmentEncoderConfig()
	encoder := zapcore.NewConsoleEncoder(config)
	logger = zap.New(zapcore.NewCore(encoder, os.Stdout, zapcore.DebugLevel))
}

func Logger() *zap.Logger {
	if logger == nil {
		InitLogger()
	}
	return logger
}
