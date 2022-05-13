// Copyright 2022, OpenTelemetry Authors
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
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	jsonFormat = "json"
)

func writeJson(e *S3Exporter, buf []byte, config *Config) error {

	key := e.getS3Key(config.S3Uploader.S3Bucket,
		config.S3Uploader.S3Prefix, config.S3Uploader.S3Partition,
		config.S3Uploader.FilePrefix, jsonFormat)

	// create a reader from data data in memory
	reader := bytes.NewReader(buf)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.S3Uploader.Region)},
	)
	uploader := s3manager.NewUploader(sess)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.S3Uploader.S3Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		fmt.Printf("Unable to upload %q to %q, %v", key, config.S3Uploader.S3Bucket, err)
		e.logger.Error("Uploading to S3")
	}

	return nil
}
