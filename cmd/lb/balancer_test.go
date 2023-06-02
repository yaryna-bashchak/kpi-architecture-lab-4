package main

import (
	. "gopkg.in/check.v1"
	
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func TestBalancer(t *testing.T) {
	TestingT(t)
}

func (s *MySuite) TestScheme(c *C) {
	testCases := []struct {
		name     string
		https    bool
		expected string
	}{
		{"HTTP", false, "http"},
		{"HTTPS", true, "https"},
		{"Reset to HTTP", false, "http"},
	}

	for _, tc := range testCases {
		c.Log(tc.name)
		*https = tc.https
		c.Check(scheme(), Equals, tc.expected)
	}
}
