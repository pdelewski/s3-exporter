package awss3exporter

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type SumoICLogsMarshaler struct{}
type SumoICTracesMarshaler struct{}

func NewSumoICLogsMarshaler() SumoICLogsMarshaler {
	return SumoICLogsMarshaler{}
}

func NewSumoICTracesMarshaler() SumoICTracesMarshaler {
	return SumoICTracesMarshaler{}
}

func (SumoICLogsMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	buf := dataBuffer{}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		ills := rl.ScopeLogs()
		for j := 0; j < ills.Len(); j++ {
			ils := ills.At(j)
			logs := ils.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				// dump only log entry body
				buf.logEntry("%s", attributeValueToString(lr.Body()))
			}
		}
	}

	return buf.buf.Bytes(), nil
}

func (SumoICTracesMarshaler) MarshalTraces(traces ptrace.Traces) ([]byte, error) {
	return nil, nil
}
