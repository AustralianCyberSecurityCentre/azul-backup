package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/models"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const MILLISECONDS_IN_A_DAY = 1000 * 60 * 60 * 24
const NO_LIFECYCLE_ERROR_CODE = "NoSuchLifecycleConfiguration"

// NOTE - changing this will have negative effects on any existing system that uses these rules.
const EXPIRY_POLICY_PREFIX = "BackupExpiryFor-"

type OffsetAfterEnd struct {
	msg string
}

func (r *OffsetAfterEnd) Error() string {
	return r.msg
}

type AccessError struct {
	msg string
}

func (e *AccessError) Error() string {
	return fmt.Sprintf("no access: %v", e.msg)
}

type ReadError struct {
	msg string
}

func (e *ReadError) Error() string {
	return fmt.Sprintf("read error: %v", e.msg)
}

/* Store files via s3 provider. */
type StoreS3 struct {
	client *minio.Client
	bucket string
}

func NewObjectS3Store(settings *common.RecoverySettings) *StoreS3 {
	bucket := fmt.Sprintf("%s%s-streams", settings.BucketNamePrefix, settings.BackupID)
	objStore, err := NewStoreS3(
		settings.ExternalBackup.Endpoint,
		settings.ExternalBackup.AccessKey,
		settings.ExternalBackup.SecretKey,
		settings.ExternalBackup.Secure,
		bucket,
		settings.ExternalBackup.Region,
		&settings.Sources,
		settings.EnableAutomaticAgeOff,
	)
	if err != nil {
		panic(err)
	}
	return objStore
}

func NewEventS3Store(settings *common.RecoverySettings) *StoreS3 {
	bucket := fmt.Sprintf("%s%s-events", settings.BucketNamePrefix, settings.BackupID)
	eventStore, err := NewStoreS3(
		settings.ExternalBackup.Endpoint,
		settings.ExternalBackup.AccessKey,
		settings.ExternalBackup.SecretKey,
		settings.ExternalBackup.Secure,
		bucket,
		settings.ExternalBackup.Region,
		&settings.Sources,
		settings.EnableAutomaticAgeOff,
	)
	if err != nil {
		panic(err)
	}
	return eventStore
}

func getRuleId(sourceName string) string {
	return fmt.Sprintf("%s%s", EXPIRY_POLICY_PREFIX, sourceName)
}

/*Remove all the rules that were created on the bucket as part of the setLifecycleForBucket function.*/
func removeLifeCycleForBucket(client *minio.Client, bucket string, sourceConf *models.SourcesConf) error {
	var err error
	currentLifeCycleConfig, err := client.GetBucketLifecycle(context.Background(), bucket)
	if err != nil {
		// If the error is the lifecycle doesn't exist clear it.
		var minioError minio.ErrorResponse
		if errors.As(err, &minioError) {
			if minioError.Code == NO_LIFECYCLE_ERROR_CODE {
				// No lifecycle policy is set and none is required immediately exit.
				return nil
			}
		}
		if err != nil {
			log.Printf("Failed to get the bucket lifecycle policy with the error '%s'\n", err.Error())
			return err
		}
	}

	indexesToRemove := []int{}
	// Identify all the automatically created backup expiry rules.
	for srcName := range sourceConf.Sources {
		ruleId := getRuleId(srcName)
		for i := range currentLifeCycleConfig.Rules {
			if currentLifeCycleConfig.Rules[i].ID == ruleId {
				indexesToRemove = append(indexesToRemove, i)
			}
		}
	}
	// Remove all the automatically created rules.
	newRules := []lifecycle.Rule{}
	for i, rule := range currentLifeCycleConfig.Rules {
		if !slices.Contains(indexesToRemove, i) {
			newRules = append(newRules, rule)
		}
	}
	currentLifeCycleConfig.Rules = newRules

	if len(indexesToRemove) > 0 {
		log.Printf("Lifecycle policy for bucket %s has been disabled removing old policies.\n", bucket)
		err = client.SetBucketLifecycle(context.Background(), bucket, currentLifeCycleConfig)
		if err != nil {
			log.Printf("Failed to remove old lifecycle policy rules with the error '%s'\n", err.Error())
			return err
		}
	}
	log.Println("Automatic AgeOff for backup is disabled.")
	return nil

}

/*Set the lifecycle policy for the Minio storage bucket.*/
func setLifecycleForBucket(client *minio.Client, bucket string, sourceConf *models.SourcesConf) error {
	var err error
	currentLifeCycleConfig, err := client.GetBucketLifecycle(context.Background(), bucket)
	if err != nil {
		// If the error is the lifecycle doesn't exist clear it.
		var minioError minio.ErrorResponse
		if errors.As(err, &minioError) {
			if minioError.Code == NO_LIFECYCLE_ERROR_CODE {
				// No lifecycle policy is set and none is required immediately exit.
				currentLifeCycleConfig = lifecycle.NewConfiguration()
				err = nil
			}
		}
		if err != nil {
			log.Printf("Failed to get the bucket lifecycle policy with the error '%s'\n", err.Error())
			return err
		}
	}

	statusEnabled := "Enabled"

	// If a rule is modified or added an updated set of rules should be sent to minio.
	shouldUpdateLifecycle := false
	for srcName, src := range sourceConf.Sources {
		// One day plus configured expiry (extra day accounts for integer rounding)
		expiryInDays := lifecycle.ExpirationDays(1 + src.ExpireEventsAfterMs/MILLISECONDS_IN_A_DAY)
		ruleId := getRuleId(srcName)
		prefix := fmt.Sprintf("%s/", srcName)

		shouldCreateRule := true
		// Check if the rule already exists.
		for i := range currentLifeCycleConfig.Rules {
			rule := &currentLifeCycleConfig.Rules[i]
			if ruleId == rule.ID {
				// Rule should either be updated or left alone
				shouldCreateRule = false
				if expiryInDays == rule.Expiration.Days &&
					rule.RuleFilter.Prefix == prefix &&
					rule.Status == statusEnabled &&
					rule.Expiration.DeleteAll {
					// The rule to be created already exists.
					fmt.Printf("The source %s in bucket %s is set to expire objects older than %d days.\n", srcName, bucket, expiryInDays)
					break
				} else {
					// The rule to be created doesn't exist and there is an old rule that will need to be removed.
					rule.Expiration.Days = expiryInDays
					rule.Expiration.DeleteAll = true
					rule.RuleFilter = lifecycle.Filter{
						Prefix: prefix,
					}
					rule.Status = statusEnabled
					shouldUpdateLifecycle = true
					log.Printf("Rule %s was updated and will age off %s after %d days", ruleId, srcName, expiryInDays)
				}
			}
		}
		if shouldCreateRule {
			shouldUpdateLifecycle = true
			// Add new rule for creation
			currentLifeCycleConfig.Rules = append(currentLifeCycleConfig.Rules, lifecycle.Rule{
				ID: ruleId,
				Expiration: lifecycle.Expiration{
					Days:      expiryInDays,
					DeleteAll: true,
				},
				Status: statusEnabled,
				RuleFilter: lifecycle.Filter{
					Prefix: prefix,
				},
			})
			log.Printf("Rule %s was created and will age off %s after %d days", ruleId, srcName, expiryInDays)
		}
	}

	if shouldUpdateLifecycle {
		log.Printf("Lifecycle policy for bucket %s was out of date or did not exist, updating now.\n", bucket)
		err = client.SetBucketLifecycle(context.Background(), bucket, currentLifeCycleConfig)
		if err != nil {
			log.Printf("Failed to update the lifecycle policy with the error '%s'\n", err.Error())
			return err
		}
	}
	log.Println("Automatic AgeOff for backup is enabled and has finished being verified and modified.")
	return err
}

func NewStoreS3(endpoint string, accessKey string, secretKey string, secure bool, bucket string, region string, sourceConf *models.SourcesConf, enableAutomaticAgeOff bool) (*StoreS3, error) {
	var client *minio.Client
	var err error

	// default transport with response header timeout set to a minute (which doesn't appear to be default?)
	// from https://github.com/minio/minio-go/blob/master/transport.go
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   16,
		ResponseHeaderTimeout: time.Minute,
		IdleConnTimeout:       time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	}

	opts := minio.Options{
		Secure:    secure,
		Region:    region,
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Transport: transport,
	}
	// accessKey, secretKey
	client, err = minio.New(endpoint, &opts)
	if err != nil {
		return nil, err
	}
	b, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, err
	}
	if !b {
		err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{})
	}
	if err != nil {
		return nil, err
	}
	if enableAutomaticAgeOff {
		err = setLifecycleForBucket(client, bucket, sourceConf)
	} else {
		err = removeLifeCycleForBucket(client, bucket, sourceConf)
	}

	if err != nil {
		return nil, err
	}

	return &StoreS3{
		client,
		bucket,
	}, err
}

func (s *StoreS3) Put(id string, data []byte) error {
	_, err := s.client.PutObject(
		context.Background(),
		s.bucket,
		id,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: "binary/octet-stream"},
	)
	if err != nil {
		return err
	}
	return nil
}

// Fetch returns the objects bytes or nil if not found
func (s *StoreS3) Fetch(id string) ([]byte, error) {
	reader, err := s.client.GetObject(context.Background(), s.bucket, id, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return nil, nil
		}
		return nil, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}

	defer reader.Close()

	// Custom logic to ensure all implementations of the storage interface handle offset and size
	// in the same way.
	stat, err := reader.Stat()
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return nil, nil
		}
		return nil, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}
	size := stat.Size
	data := make([]byte, size)
	n, err := reader.Read(data)
	if (err == io.EOF || err == nil) && int64(n) == size {
		return data, nil
	}
	return nil, fmt.Errorf("%w", &ReadError{msg: fmt.Sprintf("%v", err)})
}

func (s *StoreS3) Exists(id string) (bool, error) {
	_, err := s.client.StatObject(context.Background(), s.bucket, id, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.Code == "NoSuchBucket" {
		return false, nil
	}
	return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", resp.Code)})
}

func (s *StoreS3) Delete(id string) (bool, error) {
	err := s.client.RemoveObject(context.Background(), s.bucket, id, minio.RemoveObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		code := resp.Code
		if code == "NoSuchKey" || code == "NoSuchBucket" {
			return false, nil
		}
		return false, fmt.Errorf("%w", &AccessError{msg: fmt.Sprintf("%v", code)})
	}
	return true, nil
}

/*List the contents of the current S3 Bucket.*/
func (s *StoreS3) List(options minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return s.client.ListObjects(context.Background(), s.bucket, options)
}
