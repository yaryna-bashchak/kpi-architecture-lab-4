package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
	. "gopkg.in/check.v1"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

type BalancerIntegrationSuite struct{}

var _ = Suite(&BalancerIntegrationSuite{})

func TestBalancer(t *testing.T) {
	TestingT(t)
}

func (s *BalancerIntegrationSuite) Test(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_ENV"); !exists {
		c.Skip("Integration test is not enabled")
	}

	var serverId int
	for i := 1; i <= 25; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		c.Assert(err, IsNil)

		serverId = ((i - 1) % 3) + 1
		expectedServer := fmt.Sprintf("server%d:8080", serverId)
		c.Assert(resp.Header.Get("lb-from"), Equals, expectedServer)
		c.Logf("response from [%s]", resp.Header.Get("lb-from"))
	}
}

func (s *BalancerIntegrationSuite) BenchmarkBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_ENV"); !exists {
		c.Skip("Integration test is not enabled")
	}

	for i := 0; i < c.N; i++ {
		response, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		c.Assert(err, IsNil, Commentf("Failed on iteration: %d", i))
		response.Body.Close()
	}
}
