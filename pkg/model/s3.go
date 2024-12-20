package model

import (
	"encoding/xml"
	"fmt"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cast"
	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type Bucket struct {
	Name       string `json:"name"`
	CreateTime string `json:"create_time"` // "2006-01-02 15:04:05" localTime
	Location   string `json:"location"`
	Tags       Tags   `json:"tags"`
	// Meta any    `json:"meta"`
}

type CreateBucketRequest struct {
	BucketName *string `json:"name" binding:"required"` // 腾讯云必须带上 appid， examplebucket-1250000000
	Tags       Tags    `json:"tags"`
}

type DeleteBucketRequest struct {
	BucketName *string `json:"name" binding:"required"`
}

type DeleteBucketResponse struct {
	Meta any `json:"meta"`
}

type ListBucketRequest struct {
	KeyWord *string `json:"keyword"`
}

type ListBucketResponse struct {
	Buckets []*Bucket `json:"buckets"`
	Total   int64     `json:"total"`
}

type ObjectPregisnRequest struct {
	Bucket *string `json:"bucket" binding:"required"`
	Key    *string `json:"key" binding:"required"`
	Expire *int64  `json:"expire"` // 默认 1 小时。 签名最多支持7天(604800秒)，控制台上最多 12小时(43200秒)
}

type ObjectPregisnResponse struct {
	Url string `json:"url"`
}

type CreateBucketLifecycleRequest struct {
	Bucket     *string
	Lifecycles []Lifecycle
}

type Lifecycle struct {
	ID     *string          `xml:"ID" json:"id" binding:"required"`
	Filter *LifecycleFilter `xml:"Filter" json:"filter"`
	// Status                         *bool                                    `xml:"Status" json:"status"`
	Expiration                     *LifecycleExpiration                     `xml:"Expiration" json:"expiration"`
	NoncurrentVersionExpiration    *LifecycleNoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration" json:"noncurrent_version_expiration"`
	Transition                     *LifecycleTransition                     `xml:"Transition" json:"transition"`
	NoncurrentVersionTransition    *LifecycleNoncurrentVersionTransition    `xml:"NoncurrentVersionTransition" json:"noncurrent_version_transition"`
	AbortIncompleteMultipartUpload *LifecycleAbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload" json:"abort_incomplete_multipart_upload"`
}

type LifecycleFilter struct {
	Prefix *string `xml:"Prefix" json:"prefix"` // "" 表示整个存储桶
}

type LifecycleExpiration struct {
	Days                      *int
	Date                      *string
	ExpiredObjectDeleteMarker *bool
}

type LifecycleNoncurrentVersionExpiration struct {
	Days *int
}

type LifecycleTransition struct {
	StorageClass *string // "STANDARD" "STANDARD_IA" "ARCHIVE"
	Days         *int
}

type LifecycleNoncurrentVersionTransition struct {
	Days         *int
	StorageClass *string // "STANDARD" "STANDARD_IA" "ARCHIVE"
}

type LifecycleAbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int
}

/*
<LifecycleConfiguration>
	<Rule>
		<ID>huggingface模型定期删除</ID>
		<Filter>
			<Prefix>hg/</Prefix>
		</Filter>
		<Status>Enabled</Status>
		<Expiration>
			<Days>5</Days>
		</Expiration>
	</Rule>
	<Rule>
		<ID>OPS_BASE</ID>
		<Filter/>
		<Status>Enabled</Status>
		<AbortIncompleteMultipartUpload>
			<DaysAfterInitiation>30</DaysAfterInitiation>
		</AbortIncompleteMultipartUpload>
	</Rule>
</LifecycleConfiguration>
*/
// to COSLifecycle
func (c *CreateBucketLifecycleRequest) ToCOSLifecycle() (*cos.BucketPutLifecycleOptions, error) {
	cosRules := make([]cos.BucketLifecycleRule, len(c.Lifecycles))

	for i, lifecycle := range c.Lifecycles {
		rule := cos.BucketLifecycleRule{
			Status: "Enabled",
		}

		if lifecycle.ID != nil {
			rule.ID = *lifecycle.ID
		} else {
			return nil, fmt.Errorf("id is required")
		}

		if lifecycle.Filter != nil && lifecycle.Filter.Prefix != nil {
			rule.Filter = &cos.BucketLifecycleFilter{
				Prefix: *lifecycle.Filter.Prefix,
			}
		}

		if lifecycle.AbortIncompleteMultipartUpload != nil {
			rule.AbortIncompleteMultipartUpload = &cos.BucketLifecycleAbortIncompleteMultipartUpload{
				DaysAfterInitiation: *lifecycle.AbortIncompleteMultipartUpload.DaysAfterInitiation,
			}
		}
		if lifecycle.NoncurrentVersionExpiration != nil {
			if lifecycle.NoncurrentVersionExpiration.Days == nil {
				return nil, fmt.Errorf("NoncurrentVersionExpiration days is required")
			}
			rule.NoncurrentVersionExpiration = &cos.BucketLifecycleNoncurrentVersion{
				NoncurrentDays: *lifecycle.NoncurrentVersionExpiration.Days,
			}
		}
		if lifecycle.NoncurrentVersionTransition != nil {
			if lifecycle.NoncurrentVersionTransition.Days == nil {
				return nil, fmt.Errorf("NoncurrentVersionTransition days is required")
			}
			if lifecycle.NoncurrentVersionTransition.StorageClass == nil {
				return nil, fmt.Errorf("StorageClass is required")
			}
			rule.NoncurrentVersionTransition = []cos.BucketLifecycleNoncurrentVersion{
				{
					NoncurrentDays: *lifecycle.NoncurrentVersionTransition.Days,
					StorageClass:   *lifecycle.NoncurrentVersionTransition.StorageClass,
				},
			}
		}

		if lifecycle.Expiration != nil {
			if lifecycle.Expiration.Days == nil {
				return nil, fmt.Errorf("days is required")
			}
			ex := &cos.BucketLifecycleExpiration{
				Days: *lifecycle.Expiration.Days,
			}
			if lifecycle.Expiration.Date != nil {
				ex.Date = *lifecycle.Expiration.Date
			}
			if lifecycle.Expiration.ExpiredObjectDeleteMarker != nil {
				ex.ExpiredObjectDeleteMarker = *lifecycle.Expiration.ExpiredObjectDeleteMarker
			}
			rule.Expiration = ex
		}
		if lifecycle.Transition != nil {
			rule.Transition = []cos.BucketLifecycleTransition{
				{
					Days:         *lifecycle.Transition.Days,
					StorageClass: *lifecycle.Transition.StorageClass,
				},
			}
		}

		cosRules[i] = rule
	}
	fmt.Println(tea.Prettify(cosRules))
	return &cos.BucketPutLifecycleOptions{
		XMLName: xml.Name{Local: "LifecycleConfiguration"},
		Rules:   cosRules,
	}, nil
}

// to aws lifecycle
func (c *CreateBucketLifecycleRequest) ToAWSS3Lifecycle() (*s3.PutBucketLifecycleInput, error) {
	input := &s3.PutBucketLifecycleInput{
		Bucket: c.Bucket,
	}
	rules := make([]*s3.Rule, len(c.Lifecycles))
	for i, lifecycle := range c.Lifecycles {
		rule := &s3.Rule{
			ID:     lifecycle.ID,
			Status: tea.String("Enabled"),
		}
		if lifecycle.Filter != nil && lifecycle.Filter.Prefix != nil {
			rule.Prefix = lifecycle.Filter.Prefix
		}
		if lifecycle.Expiration != nil {
			rule.Expiration = &s3.LifecycleExpiration{
				Days: tea.Int64(cast.ToInt64(lifecycle.Expiration.Days)),
			}
		}
		if lifecycle.Transition != nil {
			rule.Transition = &s3.Transition{
				Days: tea.Int64(cast.ToInt64(lifecycle.Transition.Days)),
			}
		}
		if lifecycle.AbortIncompleteMultipartUpload != nil {
			rule.AbortIncompleteMultipartUpload = &s3.AbortIncompleteMultipartUpload{
				DaysAfterInitiation: tea.Int64(cast.ToInt64(lifecycle.AbortIncompleteMultipartUpload.DaysAfterInitiation)),
			}
		}
		if lifecycle.NoncurrentVersionExpiration != nil {
			rule.NoncurrentVersionExpiration = &s3.NoncurrentVersionExpiration{
				NoncurrentDays: tea.Int64(cast.ToInt64(lifecycle.NoncurrentVersionExpiration.Days)),
			}
		}
		if lifecycle.NoncurrentVersionTransition != nil {
			rule.NoncurrentVersionTransition = &s3.NoncurrentVersionTransition{
				NoncurrentDays: tea.Int64(cast.ToInt64(lifecycle.NoncurrentVersionTransition.Days)),
				StorageClass:   lifecycle.NoncurrentVersionTransition.StorageClass,
			}
		}
		if lifecycle.Filter != nil && lifecycle.Filter.Prefix != nil {
			rule.Prefix = lifecycle.Filter.Prefix
		} else {
			return nil, fmt.Errorf("filter is required")
		}
		rules[i] = rule
	}
	input.SetLifecycleConfiguration(&s3.LifecycleConfiguration{
		Rules: rules,
	})
	return input, nil
}

type GetBucketLifecycleRequest struct {
	Bucket *string
}

type GetBucketLifecycleResponse struct {
	Lifecycle any
}
