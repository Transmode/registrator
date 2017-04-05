//go:generate go-extpoints . AdapterFactory
package bridge

import (
	"net/url"

	dockerapi "github.com/fsouza/go-dockerclient"
)

type AdapterFactory interface {
	New(uri *url.URL) RegistryAdapter
}

type RegistryAdapter interface {
	Ping() error
	Register(service *Service) error
	RegisterSwarmService(service *ServiceSwarm) error
	Deregister(service *Service) error
	Refresh(service *Service) error
	Services() ([]*Service, error)
}

type Config struct {
	HostIp          string
	Internal        bool
	UseIpFromLabel  string
	ForceTags       string
	RefreshTtl      int
	RefreshInterval int
	DeregisterCheck string
	Cleanup         bool
}

type Service struct {
	ID    string
	Name  string
	Port  int
	IP    string
	Tags  []string
	Attrs map[string]string
	TTL   int

	Origin ServicePort
}

type ServiceSwarm struct {
	ID    string
	Name  string
	Port  int
	IP    string
	Tags  []string
	Attrs map[string]string
	TTL   int

	Origin ServicePortSwarm
}

type DeadContainer struct {
	TTL      int
	Services []*Service
}

type ServicePort struct {
	HostPort          string
	HostIP            string
	ExposedPort       string
	ExposedIP         string
	PortType          string
	ContainerHostname string
	ContainerID       string
	ContainerName     string
	container         *dockerapi.Container
}

type ServicePortSwarm struct {
	Port        string
	PortType    string
	ID          string
	Name        string
	serviceTags string
	IP          string
	Env         []string
	Labels      []string
}
