package paramstore

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/knadh/koanf/maps"
)

type Config struct {
	Delimiter          string
	Path               string
	WithDecryption     bool
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSRoleARN         string
	AWSRegion          string
	WatchInterval      time.Duration
}

type ParamStore struct {
	client *ssm.Client
	config Config
	input  ssm.GetParametersByPathInput
	params []types.Parameter
	cb     func(s string) string
}

func Provider(cfg Config, cb func(s string) string) *ParamStore {
	// Load the default config
	c, err := config.LoadDefaultConfig(context.Background())

	if err != nil {
		return nil
	}

	// Initialize delimiter string
	if cfg.Delimiter == "" {
		cfg.Delimiter = "/"
	}

	// Initialize AWS region
	if cfg.AWSRegion != "" {
		c.Region = cfg.AWSRegion
	}

	// Initialize watch interval
	if cfg.WatchInterval == 0 {
		cfg.WatchInterval = 600 * time.Second
	}

	// Check if AWS access key ID and secret key are specified
	if cfg.AWSAccessKeyID != "" && cfg.AWSSecretAccessKey != "" {
		c.Credentials = credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, "")
	}

	// Check if AWS role ARN is present
	if cfg.AWSRoleARN != "" {
		stsSvc := sts.NewFromConfig(c)
		credentials := stscreds.NewAssumeRoleProvider(stsSvc, cfg.AWSRoleARN)
		c.Credentials = aws.NewCredentialsCache(credentials)
	}

	client := ssm.NewFromConfig(c)

	return &ParamStore{client: client, config: cfg, cb: cb}
}

func ProviderWithClient(cfg Config, cb func(s string) string, client *ssm.Client) *ParamStore {
	return &ParamStore{client: client, config: cfg, cb: cb}
}

func (ps *ParamStore) Read() (map[string]interface{}, error) {
	// Check if path is provided
	if ps.config.Path == "" {
		return nil, errors.New("no parameter path provided")
	}

	// Set SSM API call input
	ps.input = ssm.GetParametersByPathInput{
		Path:           aws.String(ps.config.Path),
		WithDecryption: &ps.config.WithDecryption,
	}

	// Get parameters
	var params []types.Parameter

	for {
		result, err := ps.client.GetParametersByPath(context.Background(), &ps.input)

		if err != nil {
			return nil, err
		}

		params = append(params, result.Parameters...)

		ps.input.NextToken = result.NextToken

		if result.NextToken == nil {
			break
		}

	}

	ps.params = params

	mp := make(map[string]interface{})

	for _, param := range params {
		key := *param.Name

		// Transform key if transformer is provided
		if ps.cb != nil {
			key = ps.cb(key)
		}

		if key == "" {
			return nil, errors.New("transformed key is empty")
		}

		// Set key value
		mp[key] = param.Value
	}

	return maps.Unflatten(mp, ps.config.Delimiter), nil
}

func (ps *ParamStore) ReadBytes() ([]byte, error) {
	return nil, errors.New("paramstore provider does not support ReadBytes method")
}

func (ps *ParamStore) Watch(cb func(event interface{}, err error)) error {
	go func() {
		// Start new ticker
		ticker := time.NewTicker(ps.config.WatchInterval)
		defer ticker.Stop()

	main:
		for range ticker.C {
			// Initialize slice to store parameters fetched from API
			var params []types.Parameter
			// Initialize slice to store updated parameters
			var updatedParams []types.Parameter

			// Fetch all parameters from API
			for {
				result, err := ps.client.GetParametersByPath(context.Background(), &ps.input)

				if err != nil {
					cb(nil, err)

					continue main
				}

				params = append(params, result.Parameters...)

				ps.input.NextToken = result.NextToken

				if result.NextToken == nil {
					break
				}
			}

			// Check for updates
			for _, newParam := range params {
				// Find parameter in previously saved parameters
				for _, p := range ps.params {
					if *p.ARN == *newParam.ARN && newParam.Version != p.Version {
						updatedParams = append(updatedParams, newParam)
					}
				}
			}

			if len(updatedParams) > 0 {
				// Trigger update
				cb(updatedParams, nil)
			}
		}
	}()

	return nil
}
