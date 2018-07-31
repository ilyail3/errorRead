package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func isError(line string) bool {
	return strings.Contains(line, "ERROR") &&
		!strings.Contains(line, "Final Counters for") &&
		!strings.Contains(line, "No log4j2 configuration file found") &&
		!strings.Contains(line, "TASK_FINISHED")
}

func processKey(s3Service *s3.S3, bucket *string, key *string) error {
	f, err := ioutil.TempFile("", "logscan_")

	if err != nil {
		return fmt.Errorf("failed to create temporary file:%v", err)
	}

	defer f.Close()
	defer os.Remove(f.Name())

	result, err := s3Service.GetObject(&s3.GetObjectInput{Bucket: bucket, Key: key})

	if err != nil {
		return fmt.Errorf("failed to get object:%v", err)
	}

	defer result.Body.Close()

	_, err = io.Copy(f, result.Body)

	if err != nil {
		return fmt.Errorf("failed to read get file:%v", err)
	}

	f.Seek(0, io.SeekStart)

	var fh io.Reader = f

	if strings.HasPrefix(*key, ".gz") {
		fh, err := gzip.NewReader(f)

		if err != nil {
			return fmt.Errorf("failed to start reader for gzip file")
		}

		defer fh.Close()
	}

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	nextLines := 0

	for scanner.Scan() {
		line := scanner.Text()

		if isError(line) {
			fmt.Printf("key:%s\n", *key)
			fmt.Print(line + "\n")
			nextLines = 10

		} else if nextLines > 0 {
			fmt.Print(line + "\n")
			nextLines -= 1

		}

	}

	return nil
}

func main() {
	awsProfileFile := flag.String("aws-profile-file", "", "AWS profile file")
	awsProfile := flag.String("aws-profile", "", "AWS profile name")
	regionFlag := flag.String("region", "", "AWS Region")

	bucket := flag.String("bucket", "", "Bucket name")
	path := flag.String("path", "", "Path")

	flag.Parse()

	conf := aws.Config{
		Region: regionFlag,
		S3DisableContentMD5Validation: aws.Bool(true)}

	if *awsProfile != "" {
		conf.WithCredentials(credentials.NewSharedCredentials(*awsProfileFile, *awsProfile))
	}

	s, err := session.NewSession(&conf)

	if err != nil {
		log.Panicf("failed to open s3 session:%v", err)
	}

	s3Service := s3.New(s)

	listOutput, err := s3Service.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: path})

	if err != nil {
		log.Panicf("list bucket failed:%v", err)
	}

	log.Printf("total %d files", len(listOutput.Contents))

	for _, key := range listOutput.Contents {
		err = processKey(s3Service, bucket, key.Key)

		if err != nil {
			log.Panicf("error on file %s, err:%v", *key.Key, err)
		}
	}
}
