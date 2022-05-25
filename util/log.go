package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger = newLogger()

func newLogger() (logger *zap.Logger) {
	cores := []zapcore.Core{stdErrCore()}

	core := zapcore.NewTee(cores...)
	logger = zap.New(core)

	return logger
}

func stdErrCore() zapcore.Core {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006/01/02 15:04:05")
	logger, _ := cfg.Build()
	return logger.Core()
}
