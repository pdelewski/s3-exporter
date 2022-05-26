package awss3exporter

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type Marshaler interface {
	MarshalTraces(td ptrace.Traces) ([]byte, error)
	MarshalLogs(ld plog.Logs) ([]byte, error)
}

var (
	ErrUnknownMarshaler = errors.New("unknown marshaler")
)

type S3Marshaler struct {
	logsMarshaler   plog.Marshaler
	tracesMarshaler ptrace.Marshaler
	logger          *zap.Logger
}

func (marshaler *S3Marshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return marshaler.tracesMarshaler.MarshalTraces(td)
}

func (marshaler *S3Marshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return marshaler.logsMarshaler.MarshalLogs(ld)
}

func NewMarshaler(name string, logger *zap.Logger) (Marshaler, error) {
	marshaler := &S3Marshaler{logger: logger}
	switch name {
	case "otlp", "otlp_proto":
		marshaler.logsMarshaler = plog.NewProtoMarshaler()
		marshaler.tracesMarshaler = ptrace.NewProtoMarshaler()
	case "otlp_json":
		marshaler.logsMarshaler = plog.NewJSONMarshaler()
		marshaler.tracesMarshaler = ptrace.NewJSONMarshaler()
	case "sumo_ic":
		marshaler.logsMarshaler = NewSumoICLogsMarshaler()
		marshaler.tracesMarshaler = NewSumoICTracesMarshaler()
	default:
		return nil, ErrUnknownMarshaler
	}
	return marshaler, nil
}
