package main

import (
	"testing"
	"os"
	"github.com/stretchr/testify/assert"
)



const (
	INCORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS = `{"team1": [["start-string1"], "team2": []}`
	CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS = `{"team1": ["st-string1", "start-string2"], "team2": ["this-start-string3"]}`
)

func TestNewCustomCGLagLabels_wrong_config(t *testing.T){
	os.Setenv("CONSUMERGROUP_LAG_CUSTOM_LABELS", INCORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS)
	customLabelsEnv, _ := os.LookupEnv("CONSUMERGROUP_LAG_CUSTOM_LABELS")
	_, err := NewCustomCGLagLabels(customLabelsEnv)

	if err == nil {
		t.Errorf("Json string should have failed due to the wrong string json format")
	}
}

func TestNewCustomCGLagLabels_correct_config(t *testing.T){
	os.Setenv("CONSUMERGROUP_LAG_CUSTOM_LABELS", CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS)
	customLabelsEnv, _ := os.LookupEnv("CONSUMERGROUP_LAG_CUSTOM_LABELS")
	customLabels, err := NewCustomCGLagLabels(customLabelsEnv)

	// Assert No errors from json string to map
	if err != nil {
		t.Errorf("Json string should have not failed due to the correct string json format")
	}

	// Assert labelByPrefix are correct
	assert.Equal(t, customLabels.labelByPrefix["st-string1"], "team1")
	assert.Equal(t, customLabels.labelByPrefix["this-start-string3"], "team2")
}

func TestFetchLabel_success(t *testing.T){
	os.Setenv("CONSUMERGROUP_LAG_CUSTOM_LABELS", CORRECT_CONSUMERGROUP_LAG_CUSTOM_LABELS)
	customLabelsEnv, _ := os.LookupEnv("CONSUMERGROUP_LAG_CUSTOM_LABELS")
	customLabels, _ := NewCustomCGLagLabels(customLabelsEnv)

	// Value should not be in Cache, then Fetch it, and finally check the value is cached
	_, found := customLabels.labelCache.Get("st-string1-consumergroup-name")
	assert.Equal(t, found, false)
	assert.Equal(t, customLabels.FetchLabel("st-string1-consumergroup-name"), "team1")
	_, found = customLabels.labelCache.Get("st-string1-consumergroup-name")
	assert.Equal(t, found, true)

}

