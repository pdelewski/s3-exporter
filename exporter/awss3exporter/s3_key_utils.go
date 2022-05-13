// Copyright 2022 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package awss3exporter

import (
	"math/rand"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// generate the s3 time key based on partition configuration
func (e *S3Exporter) getTimeKey(partition string) string {
	var timeKey string
	t := time.Now()
	year, month, day := t.Date()
	hour, minute, _ := t.Clock()

	rand.Int()

	if partition == "hour" {
		timeKey = fmt.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d/", year, month, day, hour)
	} else {
		timeKey = fmt.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d/minute=%02d/", year, month, day, hour, minute)
	}

	e.logger.Info("Start processing resource metrics", zap.Any("timeKey", timeKey))
	return timeKey
}

func (e *S3Exporter) getS3Key(bucket string, keyPrefix string, partition string, filePrefix string, fileformat string) string {
	timeKey := e.getTimeKey(partition)
	randomID := rand.Int()

	s3Key := bucket + "/" + keyPrefix + "/" + timeKey + "/" + filePrefix + "_" + strconv.Itoa(randomID) + "." + fileformat

	e.logger.Info("Start processing resource metrics", zap.Any("s3Key", s3Key))

	return s3Key
}
