package elastic

import (
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

type Logger struct {
	elasticLogger elastictransport.Logger
}

func NewLogger(elasticLogger elastictransport.Logger) *Logger {
	return &Logger{
		elasticLogger: elasticLogger,
	}
}

func (l *Logger) LogRoundTrip(request *http.Request, response *http.Response, err error, time time.Time, duration time.Duration) error {

	// err is present only for transport errors such as network failure, timeout, DNS
	if err != nil {
		return l.elasticLogger.LogRoundTrip(request, response, err, time, duration)
	}

	// this filters for retriable errors
	if response != nil && response.StatusCode > 201 {
		return l.elasticLogger.LogRoundTrip(request, response, err, time, duration)
	}
	return nil
}

func (l *Logger) RequestBodyEnabled() bool {
	return l.elasticLogger.RequestBodyEnabled()
}

func (l *Logger) ResponseBodyEnabled() bool {
	return l.elasticLogger.ResponseBodyEnabled()
}
