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
	labelByPrefix			map[string]string
	labelCache			*go_cache.Cache
}

func NewCustomCGLagLabels(config string) (*CustomCGLagLabels, error){
	labelByOwner := make(map[string][]string)
	err := json.Unmarshal([]byte(config), &labelByOwner)
	if err != nil {
		return nil, err
	}

	labelByPrefix := make(map[string]string)
	for owner, startWith := range  labelByOwner{
		for _, startWith := range startWith {
			labelByPrefix[startWith] = owner
		}
	}
	labelCache := go_cache.New(1440*time.Minute, 60*time.Minute)
	return &CustomCGLagLabels{labelByPrefix: labelByPrefix, labelCache: labelCache}, nil
}

func (c *CustomCGLagLabels) FetchLabel(groupId string) string {
	var owner, found = c.labelCache.Get(groupId)
	if found {
		plog.Debugf("Cache hit for consumergroup: \"%s\"", groupId)
		return owner.(string)
	} else {
		for startWith, owner := range c.labelByPrefix {
        		if strings.HasPrefix(groupId, startWith) {
				c.labelCache.Set(groupId, owner, 1440*time.Minute)
            			return owner
         		}
     		}
		return ""
	}
}
