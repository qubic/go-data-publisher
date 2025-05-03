package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet_create(t *testing.T) {
	set := ToSet([]string{"one", "two"})
	assert.True(t, set["one"])
	assert.True(t, set["two"])
	assert.False(t, set["foo"])
}

func TestSet_difference(t *testing.T) {
	set1 := ToSet([]string{"three", "four", "five", "one", "two"})
	set2 := ToSet([]string{"three", "four", "five", "six"})

	result := Difference(set1, set2)

	assert.True(t, result["one"])
	assert.True(t, result["two"])
	assert.False(t, result["three"])
	assert.False(t, result["four"])
	assert.False(t, result["five"])
	assert.True(t, result["six"])
	assert.False(t, result["foo"])
}

func TestSet_difference_givenEmptySet(t *testing.T) {
	set1 := ToSet([]string{})
	set2 := ToSet([]string{"one", "two", "three"})

	result1 := Difference(set1, set2)

	assert.True(t, result1["one"])
	assert.True(t, result1["two"])
	assert.True(t, result1["three"])

	result2 := Difference(set2, set1)

	assert.True(t, result2["one"])
	assert.True(t, result2["two"])
	assert.True(t, result2["three"])

	result3 := Difference(set1, set1)
	assert.Empty(t, result3)
}

func TestSet_addToSet(t *testing.T) {

	set := NewSet()
	AddToSet(set, "one")
	AddToSet(set, "two")

	assert.True(t, set["one"])
	assert.True(t, set["two"])
	assert.False(t, set["foo"])

}
