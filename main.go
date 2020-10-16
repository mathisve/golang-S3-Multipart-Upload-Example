package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	BUCKET_NAME = "mutlipartuploadyoutubetest"
	REGION      = "eu-central-1"

	FILE      = "video.mp4"
	PART_SIZE = 6_000_000
	RETRIES   = 2
)

var (
	s3session *s3.S3
)

func init() {
	s3session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(REGION),
	})))
}

func main() {
	file, _ := os.Open(FILE)
	defer file.Close()

	stats, _ := file.Stat()
	fileSize := stats.Size()

	buffer := make([]byte, fileSize)
	file.Read(buffer)

	expiryDate := time.Now().AddDate(0, 0, 1)
	createdResp, err := s3session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:  aws.String(BUCKET_NAME),
		Key:     aws.String(file.Name()),
		Expires: &expiryDate,
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	var completedParts []*s3.CompletedPart
	for start = 0; remaining != 0; start += PART_SIZE {
		if remaining < PART_SIZE {
			currentSize = remaining
		} else {
			currentSize = PART_SIZE
		}

		completed, err := Upload(createdResp, buffer[start:start+currentSize], partNum)
		if err != nil {
			_, err = s3session.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   createdResp.Bucket,
				Key:      createdResp.Key,
				UploadId: createdResp.UploadId,
			})
			if err != nil {
				fmt.Println(err)
				return
			}
		}

		remaining -= currentSize
		fmt.Printf("Part %v complete, %v btyes remaining\n", partNum, remaining)

		partNum++
		completedParts = append(completedParts, completed)
	}

	resp, err := s3session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp.String())
	}

}

func Upload(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int) (completedPart *s3.CompletedPart, err error) {
	var try int
	for try <= RETRIES {
		uploadResp, err := s3session.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		if err != nil {
			fmt.Println(err)
			if try == RETRIES {
				return nil, err
			} else {
				try++
			}
		} else {
			return &s3.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: aws.Int64(int64(partNum)),
			}, nil
		}
	}

	return nil, nil
}
