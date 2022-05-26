package awss3exporter

import (
	"github.com/scaleway/scaleway-sdk-go/logger"
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
		sourceCategory, exists := rl.Resource().Attributes().Get("_sourceCategory")
		if exists == false {
			logger.Errorf("_sourceCategory attribute does not exists")
		}
		sourceHost, exists := rl.Resource().Attributes().Get("_sourceHost")
		if exists == false {
			logger.Errorf("_sourceHost attribute does not exists")
		}
		ills := rl.ScopeLogs()
		for j := 0; j < ills.Len(); j++ {
			ils := ills.At(j)
			logs := ils.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				dateVal := lr.ObservedTimestamp()
				body := attributeValueToString(lr.Body())
				sourceName, exists := lr.Attributes().Get("log.file.path_resolved")
				if exists == false {
					logger.Errorf("_sourceName attribute does not exists")
				}
				buf.logEntry("{\"data\": \"%s\",\"sourceName\":\"%s\",\"sourceHost\":\"%s\",\"sourceCategory\":\"%s\",\"fields\":{},\"message\":\"%s\"}",
					dateVal, attributeValueToString(sourceName), attributeValueToString(sourceHost), attributeValueToString(sourceCategory), body)
			}
		}
	}
	return buf.buf.Bytes(), nil
}

func (SumoICTracesMarshaler) MarshalTraces(traces ptrace.Traces) ([]byte, error) {
	return nil, nil
}
