package main

import (
	"flag"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	S3_MAX_KEYS        = 1000
	CHAN_BUFFER_SIZE   = 1000
	CHAN_UPLOAD_WORKER = 5
)

var wg_list sync.WaitGroup
var wg_upload sync.WaitGroup

func receive_s3_chunks(svc *s3.S3, bucket_name string, chan_s3_chunks chan *s3.ListObjectsOutput) {
	defer wg_list.Done()
	defer close(chan_s3_chunks)

	params := &s3.ListObjectsInput{
		Bucket:  aws.String(bucket_name), // Required
		MaxKeys: aws.Int64(S3_MAX_KEYS),
	}

	err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		chan_s3_chunks <- page
		return !lastPage
	})

	if err != nil {

		fmt.Println(err.Error())
		return
	}

}

func extract_contents(chan_s3_chunks chan *s3.ListObjectsOutput, output_chan chan *s3.Object) {
	defer wg_list.Done()
	for {
		s3_list_chunk, ok := <-chan_s3_chunks
		if !ok {

			break
		}

		for _, s3_key := range s3_list_chunk.Contents {

			output_chan <- s3_key
		}
	}

	close(output_chan)
}

func Copy_S3_to_S3(svc *s3.S3, bucket_name_src, bucket_name_dest string, s3_contents chan *s3.Object, worker_id int) {
	defer wg_upload.Done()
	var elem *s3.Object
	var ok bool

	for {
		elem, ok = <-s3_contents

		if ok && elem != nil {

			fmt.Printf("Transfer from Worker %v: %v\n", worker_id, *elem.Key)
			params := &s3.CopyObjectInput{
				Bucket:     aws.String(bucket_name_dest),                  // Required
				CopySource: aws.String(bucket_name_src + "/" + *elem.Key), // Required
				Key:        aws.String(*elem.Key),                         // Required
			}
			_, err := svc.CopyObject(params)

			if err != nil {
				fmt.Println(err.Error())
				return
			}

		} else {
			break
		}
	}
	fmt.Printf("Transfer done\n")
}

var (
	arg_bucket_src  = flag.String("Sbucket", "network-task2-3", "Enter the source Bucket")
	arg_bucket_dest = flag.String("Dbucket", "destination-aqfer", "Enter the Destination Bucket")
	arg_profile     = flag.String("profile", "aqfer-test", "Enter the profile")
)

func main() {
	flag.Parse()
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           *arg_profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(sess)
	chan_s3_chunks_src := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)

	s3_contents_src := make(chan *s3.Object, CHAN_BUFFER_SIZE)

	go receive_s3_chunks(svc, *arg_bucket_src, chan_s3_chunks_src)
	wg_list.Add(1)

	go extract_contents(chan_s3_chunks_src, s3_contents_src)
	wg_list.Add(1)

	fmt.Printf("Worker-Count: %v\n", CHAN_UPLOAD_WORKER)
	for i := 0; i < CHAN_UPLOAD_WORKER; i++ {
		fmt.Printf("Starting Worker %v\n", i)
		wg_upload.Add(1)
		//Copy func
		go Copy_S3_to_S3(svc, *arg_bucket_src, *arg_bucket_dest, s3_contents_src, i)

	}

	wg_list.Wait()
	wg_upload.Wait()

}
