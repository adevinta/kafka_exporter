package main

import (
	"encoding/json"
	go_cache "github.com/patrickmn/go-cache"
	"strings"
	"time"
	plog "github.com/prometheus/common/log"
)

// Custom labels for ConsumerGroups
type CustomCGLagLabels struct {
	labelByPrefix					map[string]string
	labelCache					*go_cache.Cache
	cacheExpirationInMin				time.Duration
	cachecleanupIntervalinMin			time.Duration
}

func NewCustomCGLagLabels(config string, CacheExpirationInMin, CachecleanupIntervalinMin time.Duration) (*CustomCGLagLabels, error){
	cacheExpirationInMin := CacheExpirationInMin*time.Minute
	cachecleanupIntervalinMin := CachecleanupIntervalinMin*time.Minute

	labelByOwner := make(map[string][]string)
	err := json.Unmarshal([]byte(config), &labelByOwner)
	if err != nil {
		plog.Debugf("Error unmarshalling Json string:", err)		
		return nil, err
	}

	labelByPrefix := make(map[string]string)
	for owner, startWith := range  labelByOwner{
		for _, startWith := range startWith {
			// If key already exists then warn and skip the overwrite
			if _, ok := labelByPrefix[startWith]; ok {
				plog.Warnln("startWith key", startWith, "was set twice, skipping latest occurrence")
				continue
			}
			labelByPrefix[startWith] = owner
		}
	}
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
		plog.Debugf("Cache hit for consumergroup:", groupId)
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
