package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)



const (
	INCORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS = `{"consumer_notifiers": [[{"when":{ "starts_with":["string1", "string2"]}, "set": {"tags":["owner:fotocasa"]}}]}`
	CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS = `{
		"consumer_notifiers": [
			{
				"when": {
					"starts_with": [
						"string1",
						"string2"
					]
				},
				"set": {
					"tags":[
						"owner:fotocasa"
					]
				}
			},
			{
				"when": {
					"starts_with": [
						"string3",
						"string4"
					]
				},
				"set": {
					"tags":[
						"owner:mads"
					]
				}

			}
		]
	}`
)

func TestNewCustomCGLagLabels_wrong_config(t *testing.T){
	_, err := NewCustomCGLagLabels(INCORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS, 1, 1)

	if err == nil {
		t.Errorf("Json string should have failed due to the wrong string json format")
	}
}

func TestNewCustomCGLagLabels_correct_config(t *testing.T){
	customLabels, err := NewCustomCGLagLabels(CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS, 1, 1)

	// Assert No errors from json string to map
	if err != nil {
		t.Errorf("Json string should have not failed due to the correct string json format")
	}

	// Assert labelByPrefix are correct
	assert.Equal(t, customLabels.labelByPrefix["string1"], "fotocasa")
	assert.Equal(t, customLabels.labelByPrefix["string3"], "mads")
}

func TestFetchLabel_success(t *testing.T){
	customLabels, _ := NewCustomCGLagLabels(CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS, 1, 1)

	// Value should not be in Cache, then Fetch it, and finally check the value is cached
	_, found := customLabels.labelCache.Get("string1")
	assert.Equal(t, found, false)
	assert.Equal(t, customLabels.FetchLabel("string1"), "fotocasa")
	_, found = customLabels.labelCache.Get("string1")
	assert.Equal(t, found, true)

}

