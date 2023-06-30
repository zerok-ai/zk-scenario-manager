package repository_test

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type ServiceTestSuite struct {
	suite.Suite
	//service  ScenarioService
	//repoMock *mocks.ScenarioRepo
}

func TestServiceSuite(t *testing.T) {
	suite.Run(t, &ServiceTestSuite{})
}

// runs before execution of suite
func (s *ServiceTestSuite) SetupSuite() {
	//r := mocks.NewScenarioRepo()
	//s.repoMock = r
	//s.service = NewScenarioService(r)
}
