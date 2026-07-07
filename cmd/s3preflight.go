package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	bkupcom "github.com/AustralianCyberSecurityCentre/azul-backup.git/common"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v12/gosrc/settings"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// runS3Preflight probes the configured S3 target directly, using its own minio
// client, so failures that the normal restore path hides become visible in the
// logs: a listing AccessDenied treated as "empty", a wrong endpoint/bucket/region
// combination, virtual-hosted vs path-style addressing, or IRSA not being
// assumed. It runs at the start of `restore` (the container's fixed command), so
// no command/args change is required. It is read-only and cheap - object probes
// are capped at a handful of keys and the path-style retry only runs when the
// default addressing lists nothing.
//
// Optional: set BK__S3_TRACE=true to also dump the raw STS + S3 HTTP traffic.
func runS3Preflight(st *bkupcom.RecoverySettings) {
	eb := st.ExternalBackup
	creds, authDesc := preflightCreds(eb.Region, eb.AccessKey, eb.SecretKey)

	streamBucket := fmt.Sprintf("%s%s-streams", st.BucketNamePrefix, st.BackupID)
	eventBucket := fmt.Sprintf("%s%s-events", st.BucketNamePrefix, st.BackupID)

	bedSet.Logger.Info().Msgf(
		"s3-preflight: endpoint=%q secure=%t region=%q auth=%s backup_id=%q bucket_prefix=%q buckets=[%q %q]",
		eb.Endpoint, eb.Secure, eb.Region, authDesc, st.BackupID, st.BucketNamePrefix, streamBucket, eventBucket,
	)
	preflightSanity(eb.Endpoint, eb.Region, streamBucket, eventBucket)

	client, err := minio.New(eb.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: eb.Secure,
		Region: eb.Region,
	})
	if err != nil {
		bedSet.Logger.Error().Err(err).Msg("s3-preflight: minio.New failed - endpoint must be host[:port] with no scheme")
		return
	}
	if os.Getenv("BK__S3_TRACE") == "true" {
		bedSet.Logger.Warn().Msg("s3-preflight: HTTP wire tracing ON (BK__S3_TRACE=true) - verbose, prints signed request headers")
		client.TraceOn(os.Stderr)
	}

	// Same endpoint/creds but forced path-style addressing. Used only to confirm
	// or rule out an addressing (endpoint+bucket) problem when the default client
	// lists nothing.
	pathClient, _ := minio.New(eb.Endpoint, &minio.Options{
		Creds:        creds,
		Secure:       eb.Secure,
		Region:       eb.Region,
		BucketLookup: minio.BucketLookupPath,
	})

	// Enumerate what this identity can actually see. If the expected buckets are
	// absent (or only newly-created empty ones are present) the target is wrong.
	preflightListBuckets(client, streamBucket, eventBucket)

	for _, b := range []string{streamBucket, eventBucket} {
		bedSet.Logger.Info().Msgf("s3-preflight: ---------- probing bucket %q ----------", b)
		preflightProbe(client, pathClient, b)
	}
}

// preflightListBuckets logs every bucket the identity can see, so a wrong
// account/endpoint (expected buckets absent) or wrong backup_id/bucket_prefix
// (data lives in a differently-named bucket) becomes obvious. Creation dates
// expose buckets that a previous restore auto-created empty.
func preflightListBuckets(client *minio.Client, targets ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		bedSet.Logger.Error().Err(err).Msgf("s3-preflight: ListBuckets FAILED (%s) - identity cannot enumerate buckets", minio.ToErrorResponse(err).Code)
		return
	}
	if len(buckets) == 0 {
		bedSet.Logger.Warn().Msg("s3-preflight: identity sees ZERO buckets at this endpoint/account - almost certainly wrong endpoint/account/credentials")
		return
	}
	seen := map[string]bool{}
	bedSet.Logger.Info().Msgf("s3-preflight: identity sees %d bucket(s) at this endpoint:", len(buckets))
	for _, b := range buckets {
		seen[b.Name] = true
		bedSet.Logger.Info().Msgf("s3-preflight:   bucket %q (created %s, region %q)", b.Name, b.CreationDate.Format(time.RFC3339), b.BucketRegion)
	}
	for _, t := range targets {
		if !seen[t] {
			bedSet.Logger.Warn().Msgf("s3-preflight: target bucket %q is NOT in the identity's bucket list - wrong endpoint/account, or wrong backup_id/bucket_prefix", t)
		}
	}
}

// preflightCreds replicates the credential selection in bedrock store.NewS3Store
// so the probe authenticates exactly like the real store does.
func preflightCreds(region, accessKey, secretKey string) (*credentials.Credentials, string) {
	if tokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"); tokenFile != "" {
		roleARN := os.Getenv("AWS_ROLE_ARN")
		stsEndpoint := fmt.Sprintf("https://sts.%s.amazonaws.com", region)
		if _, statErr := os.Stat(tokenFile); statErr != nil {
			bedSet.Logger.Warn().Err(statErr).Msgf("s3-preflight: IRSA token file %q is not readable", tokenFile)
		}
		creds, err := credentials.NewSTSWebIdentity(stsEndpoint, func() (*credentials.WebIdentityToken, error) {
			token, err := os.ReadFile(tokenFile)
			if err != nil {
				return nil, err
			}
			return &credentials.WebIdentityToken{Token: string(token)}, nil
		}, func(s *credentials.STSWebIdentity) {
			s.RoleARN = roleARN
		})
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("s3-preflight: failed to build IRSA credentials")
		}
		return creds, fmt.Sprintf("IRSA web-identity (tokenFile=%q role=%q sts=%q)", tokenFile, roleARN, stsEndpoint)
	}
	if accessKey == "" {
		bedSet.Logger.Warn().Msg("s3-preflight: no IRSA token file AND no static access key -> requests will be ANONYMOUS")
	}
	return credentials.NewStaticV4(accessKey, secretKey, ""), "static access/secret keys"
}

func preflightSanity(endpoint, region string, buckets ...string) {
	if strings.Contains(endpoint, "://") {
		bedSet.Logger.Warn().Msgf("s3-preflight: endpoint %q contains a scheme; minio expects host[:port] only (TLS is controlled by 'secure')", endpoint)
	}
	if strings.Contains(endpoint, "/") {
		bedSet.Logger.Warn().Msgf("s3-preflight: endpoint %q contains a '/'; only host[:port] is valid, no path", endpoint)
	}
	if region == "" {
		bedSet.Logger.Warn().Msg("s3-preflight: region is EMPTY; required for SigV4 and the sts.<region>.amazonaws.com IRSA endpoint")
	}
	for _, b := range buckets {
		if strings.Contains(b, ".") {
			bedSet.Logger.Warn().Msgf("s3-preflight: bucket %q contains '.'; virtual-hosted-style + TLS wildcard certs can break - path-style addressing may be required", b)
		}
		if strings.ToLower(b) != b {
			bedSet.Logger.Warn().Msgf("s3-preflight: bucket %q has uppercase chars; invalid for virtual-hosted-style DNS addressing", b)
		}
	}
	if strings.Contains(endpoint, "amazonaws.com") {
		bedSet.Logger.Info().Msg("s3-preflight: AWS endpoint detected; minio auto-selects virtual-hosted-style addressing")
	}
}

// preflightProbe runs the three calls whose errors the restore path hides, and
// - only if the default client sees nothing - retries the listing with forced
// path-style addressing to expose an endpoint+bucket addressing mismatch.
func preflightProbe(client, pathClient *minio.Client, bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// True region of the bucket - catches an endpoint/region mismatch (301 /
	// PermanentRedirect / AuthorizationHeaderMalformed).
	if loc, err := client.GetBucketLocation(ctx, bucket); err != nil {
		bedSet.Logger.Error().Err(err).Msgf("s3-preflight: GetBucketLocation(%q) FAILED (%s)", bucket, minio.ToErrorResponse(err).Code)
	} else {
		bedSet.Logger.Info().Msgf("s3-preflight: bucket %q true region = %q", bucket, loc)
	}

	// BucketExists=false (with no error) is exactly what makes NewS3Store silently
	// create an empty bucket so the restore finds nothing.
	if exists, err := client.BucketExists(ctx, bucket); err != nil {
		bedSet.Logger.Error().Err(err).Msgf("s3-preflight: BucketExists(%q) FAILED (%s)", bucket, minio.ToErrorResponse(err).Code)
	} else if !exists {
		bedSet.Logger.Warn().Msgf("s3-preflight: BucketExists(%q) = false - restore will CREATE an empty bucket and find nothing here", bucket)
	} else {
		bedSet.Logger.Info().Msgf("s3-preflight: BucketExists(%q) = true", bucket)
	}

	count, listErr := preflightList(ctx, client, bucket)
	if listErr != nil {
		bedSet.Logger.Error().Err(listErr).Msgf(
			"s3-preflight: ListObjects(%q) FAILED (%s) - THIS is the error the restore silently treats as 'empty'",
			bucket, minio.ToErrorResponse(listErr).Code,
		)
		return
	}
	if count > 0 {
		bedSet.Logger.Info().Msgf("s3-preflight: ListObjects(%q) OK - saw %d object(s) (probe capped at 5)", bucket, count)
		return
	}

	// Default addressing listed nothing and did not error: check whether forced
	// path-style addressing can see objects - if so, it is an addressing problem.
	bedSet.Logger.Warn().Msgf("s3-preflight: ListObjects(%q) returned 0 objects with default addressing; retrying with forced path-style", bucket)
	if pathClient == nil {
		return
	}
	pathCount, pathErr := preflightList(ctx, pathClient, bucket)
	switch {
	case pathErr != nil:
		bedSet.Logger.Error().Err(pathErr).Msgf("s3-preflight: path-style ListObjects(%q) FAILED (%s)", bucket, minio.ToErrorResponse(pathErr).Code)
	case pathCount > 0:
		bedSet.Logger.Error().Msgf(
			"s3-preflight: ADDRESSING ISSUE - path-style listed %d object(s) for %q where default listed 0; the endpoint+bucket combo needs path-style addressing",
			pathCount, bucket,
		)
	default:
		bedSet.Logger.Warn().Msgf("s3-preflight: bucket %q is empty under BOTH addressing styles - likely wrong endpoint/account/backup_id, or genuinely empty", bucket)
	}
}

// preflightList lists up to 5 objects, returning the count and the first listing
// error (which the store's List would otherwise swallow).
func preflightList(ctx context.Context, client *minio.Client, bucket string) (int, error) {
	count := 0
	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			return count, obj.Err
		}
		bedSet.Logger.Info().Msgf("s3-preflight:   object key=%q size=%d", obj.Key, obj.Size)
		count++
		if count >= 5 {
			break
		}
	}
	return count, nil
}
