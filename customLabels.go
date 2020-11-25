package main

import (
	"encoding/json"
	"strings"
	"time"
	plog "github.com/prometheus/common/log"
	go_cache "github.com/patrickmn/go-cache"
	"github.com/xeipuuv/gojsonschema"
)

// Expected Json schema
const SCHEMA = `{
	"type": "object",
	"title": "Custom Tagging for consumer groups lag metrics",
	"required": ["consumer_notifiers"],
	"properties": {
		"consumer_notifiers": {
			"type": "array",
			"items": {
				"$ref": "#/definitions/notifier"
			}
		}
	},
	"definitions": {
		"notifier": {
			"type": "object",
			"required": ["when", "set"],
			"properties": {
				"when": {
					"type": "object",
					"required": ["starts_with"],
					"properties": {
						"starts_with": {
							"type": "array",
							"items": {
								"type": "string",
								"description": "Consumer group prefix",
								"pattern": "^[a-zA-Z0-9_i\\-]+$"
							}
						}
					}
				},
				"set": {
					"type": "object",
					"required": ["tags"],
					"properties": {
						"tags": {
							"type": "array",
							"items": {
								"type": "string",
								"description": "For now it is just supported owner tag",
								"pattern": "^owner:[a-zA-Z0-9_i\\-]+$"
							}
						}
					}
				}
			}
		}
	}
}`

// Custom labels for ConsumerGroups
type CustomCGLagLabels struct {
	labelByPrefix					map[string]string
	labelCache					*go_cache.Cache
	cacheExpirationInMin				time.Duration
	cachecleanupIntervalinMin			time.Duration
}

type Notifier map[string]map[string][]string

func NewCustomCGLagLabels(config string, CacheExpirationInMin, CachecleanupIntervalinMin time.Duration) (*CustomCGLagLabels, error){
	cacheExpirationInMin := CacheExpirationInMin*time.Minute
	cachecleanupIntervalinMin := CachecleanupIntervalinMin*time.Minute
	consumerNotifiers := make(map[string][]Notifier)
	schemaLoader := gojsonschema.NewStringLoader(SCHEMA)
	documentLoader := gojsonschema.NewStringLoader(config)

	// Validate Json schema
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		plog.Debugln("Error validating json schema", err)
		return nil, err
	}
	if result.Valid() {
		plog.Debugln("Valid json document")
	} else {
		plog.Debugln("The document is not valid. see errors :")
		for _, desc := range result.Errors() {
			plog.Errorln("Error:", desc)
		}
		return nil, err
	}

	// Convert json string into map
	err = json.Unmarshal([]byte(config), &consumerNotifiers)
	if err != nil {
		plog.Debugln("Error unmarshalling Json string:", err)
		return nil, err
	}

	// We use ALL the startWith strings as keys and the owner tag as values
	labelByPrefix := make(map[string]string)
	for  _, notifier:= range  consumerNotifiers["consumer_notifiers"]{
		for _, startWith := range notifier["when"]["starts_with"] {
			// If prefix already exists then warn and skip the overwrite
			if _, ok := labelByPrefix[startWith]; ok {
				plog.Warnln("starts_with prefix", startWith, "was set more than once, skipping latest occurrence.")
				continue
			}
			for _, tag  := range notifier["set"]["tags"]{
				if strings.HasPrefix(tag, "owner:"){
					labelByPrefix[startWith] = strings.Split(tag, ":")[1]
				} else {
					plog.Warnln("Tag", tag, "is not supported for the consumergroup lag metrics")
				}
			}
		}
	}

	// Create cache
	labelCache := go_cache.New(cacheExpirationInMin, cachecleanupIntervalinMin)
	return &CustomCGLagLabels{
		labelByPrefix: labelByPrefix,
		labelCache: labelCache,
		cacheExpirationInMin: cacheExpirationInMin,
		cachecleanupIntervalinMin: cachecleanupIntervalinMin,
	}, nil
}

func (c *CustomCGLagLabels) FetchLabel(groupId string) string {
	owner, found := c.labelCache.Get(groupId)
	if found {
		plog.Debugln("Cache hit for consumergroup:", groupId)
		c.labelCache.Set(groupId, owner, c.cacheExpirationInMin) // Let's renew TTL to keep a "kind"" of LRU
		return owner.(string)
	} else {
		for startWith, owner := range c.labelByPrefix {
        		if strings.HasPrefix(groupId, startWith) {
				c.labelCache.Set(groupId, owner, c.cacheExpirationInMin)
            			return owner
         		}
     		}
		return ""
	}
}
