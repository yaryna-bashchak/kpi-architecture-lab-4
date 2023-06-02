package main

import (
	"net/http"
	"net/http/httptest"

	"github.com/jarcoal/httpmock"
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

func (s *MySuite) TestFindMinServer(c *C) {
	c.Assert(FindMinServer(), Equals, -1)

	serversPool = []*Server{
		{URL: "Server1", ConnCnt: 22, Healthy: true},
		{URL: "Server2", ConnCnt: 17, Healthy: true},
		{URL: "Server3", ConnCnt: 35, Healthy: true},
	}
	c.Assert(FindMinServer(), Equals, 1)
	
	serversPool = []*Server{
		{URL: "Server1", ConnCnt: 10, Healthy: true},
		{URL: "Server2", ConnCnt: 10, Healthy: true},
		{URL: "Server3", ConnCnt: 10, Healthy: true},
	}
	c.Assert(FindMinServer(), Equals, 0)

	serversPool = []*Server{
		{URL: "Server1", ConnCnt: 1, Healthy: false},
		{URL: "Server2", ConnCnt: 15, Healthy: true},
		{URL: "Server3", ConnCnt: 12, Healthy: true},
	}
	c.Assert(FindMinServer(), Equals, 2)

	serversPool = []*Server{
		{URL: "Server1", ConnCnt: 0, Healthy: false},
		{URL: "Server2", ConnCnt: 10, Healthy: true},
		{URL: "Server3", ConnCnt: 0, Healthy: false},
	}
	c.Assert(FindMinServer(), Equals, 1)

	serversPool = []*Server{
		{URL: "Server1", ConnCnt: 15, Healthy: false},
		{URL: "Server2", ConnCnt: 12, Healthy: false},
		{URL: "Server3", ConnCnt: 17, Healthy: false},
	}
	c.Assert(FindMinServer(), Equals, -1)

func (s *MySuite) TestServerHealth_Healthy(c *C) {
	mockURL := "http://example.com/Health"

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	server := &Server{
		URL: "example.com",
	}

	httpmock.RegisterResponder(http.MethodGet, mockURL, httpmock.NewStringResponder(http.StatusOK, ""))
	
	healthyCheckResult := Health(server)
	c.Check(healthyCheckResult, Equals, true)
	c.Check(server.Healthy, Equals, true)
}

func (s *MySuite) TestServerHealth_Unhealthy(c *C) {
	mockURL := "http://example.com/Health"

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	server := &Server{
		URL: "example.com",
		Healthy: false,
	}

	httpmock.RegisterResponder(http.MethodGet, mockURL, httpmock.NewStringResponder(http.StatusInternalServerError, ""))

	unhealthyCheckResult := Health(server)
	c.Check(unhealthyCheckResult, Equals, false)
	c.Check(server.Healthy, Equals, false)
}
}