package integration

import (
	"encoding/json"
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

type ResponseBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

	apiURL := fmt.Sprintf("%s/api/v1/some-data?key=code-quartet", baseAddress)
	dbResponse, error := client.Get(apiURL)

	if error != nil {
		c.Error(error)
		return
	}

	var responseData ResponseBody
	decodeError := json.NewDecoder(dbResponse.Body).Decode(&responseData)

	if decodeError != nil {
		c.Error(decodeError)
		return
	}

	c.Check(responseData.Key, Equals, "code-quartet")

	if responseData.Value == "" {
		c.Error(decodeError)
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
