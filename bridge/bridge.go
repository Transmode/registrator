package bridge

import (
	"errors"
	"log"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	dockerapi "github.com/fsouza/go-dockerclient"
)

var serviceIDPattern = regexp.MustCompile(`^(.+?):([a-zA-Z0-9][a-zA-Z0-9_.-]+):[0-9]+(?::udp)?$`)

type Bridge struct {
	sync.Mutex
	registry       RegistryAdapter
	docker         *dockerapi.Client
	services       map[string][]*Service
	servicesSwarm  map[string][]*ServiceSwarm
	deadContainers map[string]*DeadContainer
	config         Config
}

func New(docker *dockerapi.Client, adapterUri string, config Config) (*Bridge, error) {
	uri, err := url.Parse(adapterUri)
	if err != nil {
		return nil, errors.New("bad adapter uri: " + adapterUri)
	}
	factory, found := AdapterFactories.Lookup(uri.Scheme)
	if !found {
		return nil, errors.New("unrecognized adapter: " + adapterUri)
	}

	log.Println("Using", uri.Scheme, "adapter:", uri)
	return &Bridge{
		docker:         docker,
		config:         config,
		registry:       factory.New(uri),
		services:       make(map[string][]*Service),
		servicesSwarm:  make(map[string][]*ServiceSwarm),
		deadContainers: make(map[string]*DeadContainer),
	}, nil
}

func (b *Bridge) Ping() error {
	return b.registry.Ping()
}

func (b *Bridge) Add(containerId string) {
	b.Lock()
	defer b.Unlock()
	b.add(containerId, false)
}

func (b *Bridge) Remove(containerId string) {
	b.remove(containerId, true)
}

func (b *Bridge) RemoveOnExit(containerId string) {
	b.remove(containerId, b.shouldRemove(containerId))
}

func (b *Bridge) RemoveOnExitSwarm(serviceId string) {
	b.removeSwarm(serviceId, true)
}

func (b *Bridge) Refresh() {
	b.Lock()
	defer b.Unlock()

	for containerId, deadContainer := range b.deadContainers {
		deadContainer.TTL -= b.config.RefreshInterval
		if deadContainer.TTL <= 0 {
			delete(b.deadContainers, containerId)
		}
	}

	for containerId, services := range b.services {
		for _, service := range services {
			err := b.registry.Refresh(service)
			if err != nil {
				log.Println("refresh failed:", service.ID, err)
				continue
			}
			log.Println("refreshed:", containerId[:12], service.ID)
		}
	}
}

func (b *Bridge) Sync(quiet bool) {
	b.Lock()
	defer b.Unlock()

	containers, err := b.docker.ListContainers(dockerapi.ListContainersOptions{})
	if err != nil && quiet {
		log.Println("error listing containers, skipping sync")
		return
	} else if err != nil && !quiet {
		log.Fatal(err)
	}

	log.Printf("Syncing services on %d containers", len(containers))

	// NOTE: This assumes reregistering will do the right thing, i.e. nothing..
	for _, listing := range containers {
		services := b.services[listing.ID]
		if services == nil {
			b.add(listing.ID, quiet)
		} else {
			for _, service := range services {
				err := b.registry.Register(service)
				if err != nil {
					log.Println("sync register failed:", service, err)
				}
			}
		}
	}

	swarm_services, err := b.docker.ListServices(dockerapi.ListServicesOptions{})

	if err != nil && quiet {
		log.Println("error listing swarm services, skipping sync")
		return
	} else if err != nil && !quiet {
		log.Fatal(err)
	}

	log.Printf("Syncing services on %d swarm services", len(swarm_services))

	// NOTE: This assumes reregistering will do the right thing, i.e. nothing..
	for _, listing := range swarm_services {
		services := b.services[listing.ID]
		if services == nil {
			b.addService(listing.ID, quiet)
		} else {
			for _, service := range services {
				err := b.registry.Register(service)
				if err != nil {
					log.Println("sync register failed:", service, err)
				}
			}
		}
	}

	// Clean up services that were registered previously, but aren't
	// acknowledged within registrator
	if b.config.Cleanup {
		// Remove services if its corresponding container is not running
		log.Println("Listing non-exited containers")
		filters := map[string][]string{"status": {"created", "restarting", "running", "paused"}}
		nonExitedContainers, err := b.docker.ListContainers(dockerapi.ListContainersOptions{Filters: filters})
		if err != nil {
			log.Println("error listing nonExitedContainers, skipping sync", err)
			return
		}
		for listingId, _ := range b.services {
			found := false
			for _, container := range nonExitedContainers {
				if listingId == container.ID {
					found = true
					break
				}
			}
			// This is a container that does not exist
			if !found {
				log.Printf("stale: Removing service %s because it does not exist", listingId)
				go b.RemoveOnExit(listingId)
			}
		}

		log.Println("Cleaning up dangling services")
		extServices, err := b.registry.Services()
		if err != nil {
			log.Println("cleanup failed:", err)
			return
		}

		log.Println("Listing non-exited services")
		nonExitedServices, err := b.docker.ListServices(dockerapi.ListServicesOptions{})
		if err != nil {
			log.Println("error listing nonExitedServices, skipping sync", err)
			return
		}
		for listingId, _ := range b.servicesSwarm {
			found := false
			for _, service := range nonExitedServices {
				if listingId == service.ID {
					found = true
					break
				}
			}
			// This is a service that does not exist
			if !found {
				log.Printf("stale: Removing swarm service %s because it does not exist", listingId)
				go b.RemoveOnExitSwarm(listingId)
			}
		}

	Outer:
		for _, extService := range extServices {
			matches := serviceIDPattern.FindStringSubmatch(extService.ID)
			if len(matches) != 3 {
				// There's no way this was registered by us, so leave it
				continue
			}
			serviceHostname := matches[1]
			if serviceHostname != Hostname {
				// ignore because registered on a different host
				continue
			}
			serviceContainerName := matches[2]
			for _, listing := range b.services {
				for _, service := range listing {
					if service.Name == extService.Name && serviceContainerName == service.Origin.container.Name[1:] {
						continue Outer
					}
				}
			}
			log.Println("dangling:", extService.ID)
			err := b.registry.Deregister(extService)
			if err != nil {
				log.Println("deregister failed:", extService.ID, err)
				continue
			}
			log.Println(extService.ID, "removed")
		}
	}
}

func (b *Bridge) add(containerId string, quiet bool) {
	if d := b.deadContainers[containerId]; d != nil {
		b.services[containerId] = d.Services
		delete(b.deadContainers, containerId)
	}

	if b.services[containerId] != nil {
		log.Println("container, ", containerId[:12], ", already exists, ignoring")
		// Alternatively, remove and readd or resubmit.
		return
	}

	container, err := b.docker.InspectContainer(containerId)
	if err != nil {
		log.Println("unable to inspect container:", containerId[:12], err)
		return
	}

	ports := make(map[string]ServicePort)

	// Extract configured host port mappings, relevant when using --net=host
	for port, _ := range container.Config.ExposedPorts {
		published := []dockerapi.PortBinding{{"0.0.0.0", port.Port()}}
		ports[string(port)] = servicePort(container, port, published)
	}

	// Extract runtime port mappings, relevant when using --net=bridge
	for port, published := range container.NetworkSettings.Ports {
		ports[string(port)] = servicePort(container, port, published)
	}

	if len(ports) == 0 && !quiet {
		log.Println("ignored:", container.ID[:12], "no published ports")
		return
	}

	servicePorts := make(map[string]ServicePort)
	for key, port := range ports {
		if b.config.Internal != true && port.HostPort == "" {
			if !quiet {
				log.Println("ignored:", container.ID[:12], "port", port.ExposedPort, "not published on host")
			}
			continue
		}
		servicePorts[key] = port
	}

	isGroup := len(servicePorts) > 1
	for _, port := range servicePorts {
		service := b.newService(port, isGroup)
		if service == nil {
			if !quiet {
				log.Println("ignored:", container.ID[:12], "service on port", port.ExposedPort)
			}
			continue
		}
		err := b.registry.Register(service)
		if err != nil {
			log.Println("register failed:", service, err)
			continue
		}
		b.services[container.ID] = append(b.services[container.ID], service)
		log.Println("added:", container.ID[:12], service.ID)
	}
}

func (b *Bridge) addService(serviceId string, quiet bool) {
	if d := b.deadContainers[serviceId]; d != nil {
		b.services[serviceId] = d.Services
		delete(b.deadContainers, serviceId)
	}

	if b.services[serviceId] != nil {
		log.Println("service, ", serviceId[:12], ", already exists, ignoring")
		// Alternatively, remove and readd or resubmit.
		return
	}

	service, err := b.docker.InspectService(serviceId)
	if err != nil {
		log.Println("unable to inspect service:", serviceId[:12], err)
		return
	} else {
		log.Println("service with ID ", serviceId, ", ", service)
	}

	ports := make(map[string]ServicePortSwarm)
	log.Println("Endpoint ports for service, ", service.Endpoint.Ports)

	// Extract configured host port mappings, relevant when using overlay
	for key, port := range service.Endpoint.Ports {
		log.Println("parsing port", key, port)
		log.Println("Protocol:", port.Protocol)
		log.Println("TargetPort", port.TargetPort)
		log.Println("PublishedPort", port.PublishedPort)
		log.Println("PublishMode", port.PublishMode)
		//published := []dockerapi.PortBinding{ {"0.0.0.0", port.PublishedPort()}, }
		ports[string(key)] = servicePortFromService(serviceId, service.Spec.Name, port.PublishedPort, port.TargetPort, service.Spec.TaskTemplate.ContainerSpec.Env, service.Spec.TaskTemplate.ContainerSpec.Labels)
		log.Println("Service port:", ports[string(key)])
	}

	if len(ports) == 0 && !quiet {
		log.Println("ignored:", service.ID[:12], "no published ports")
		return
	}

	servicePorts := make(map[string]ServicePortSwarm)
	for key, port := range ports {
		if b.config.Internal != true && port.Port == "" {
			// if !quiet {
			// 	log.Println("ignored:", service.ID[:12], "port", port.ExposedPort, "not published on host")
			// }
			continue
		}
		servicePorts[key] = port
	}

	isGroup := len(servicePorts) > 1
	for _, port := range servicePorts {
		service := b.newServiceSwarm(port, isGroup)
		if service == nil {
			if !quiet {
				log.Println("ignored:", service.ID[:12], "service on port", port.Port)
			}
			continue
		}
		err := b.registry.RegisterSwarmService(service)
		if err != nil {
			log.Println("register failed:", service, err)
			continue
		}
		b.servicesSwarm[service.ID] = append(b.servicesSwarm[service.ID], service)
		log.Println("added:", service.ID[:12], service.ID)
	}
}

func (b *Bridge) newService(port ServicePort, isgroup bool) *Service {
	container := port.container
	defaultName := strings.Split(path.Base(container.Config.Image), ":")[0]

	// not sure about this logic. kind of want to remove it.
	hostname := Hostname
	if hostname == "" {
		hostname = port.HostIP
	}
	if port.HostIP == "0.0.0.0" {
		ip, err := net.ResolveIPAddr("ip", hostname)
		if err == nil {
			port.HostIP = ip.String()
		}
	}

	if b.config.HostIp != "" {
		port.HostIP = b.config.HostIp
	}

	metadata, metadataFromPort := serviceMetaData(container.Config, port.ExposedPort)

	ignore := mapDefault(metadata, "ignore", "")
	if ignore != "" {
		return nil
	}

	service := new(Service)
	service.Origin = port
	service.ID = hostname + ":" + container.Name[1:] + ":" + port.ExposedPort
	service.Name = mapDefault(metadata, "name", defaultName)
	if isgroup && !metadataFromPort["name"] {
		service.Name += "-" + port.ExposedPort
	}
	var p int

	if b.config.Internal == true {
		service.IP = port.ExposedIP
		p, _ = strconv.Atoi(port.ExposedPort)
	} else {
		service.IP = port.HostIP
		p, _ = strconv.Atoi(port.HostPort)
	}
	service.Port = p

	if b.config.UseIpFromLabel != "" {
		containerIp := container.Config.Labels[b.config.UseIpFromLabel]
		if containerIp != "" {
			slashIndex := strings.LastIndex(containerIp, "/")
			if slashIndex > -1 {
				service.IP = containerIp[:slashIndex]
			} else {
				service.IP = containerIp
			}
			log.Println("using container IP " + service.IP + " from label '" +
				b.config.UseIpFromLabel + "'")
		} else {
			log.Println("Label '" + b.config.UseIpFromLabel +
				"' not found in container configuration")
		}
	}

	// NetworkMode can point to another container (kuberenetes pods)
	networkMode := container.HostConfig.NetworkMode
	if networkMode != "" {
		if strings.HasPrefix(networkMode, "container:") {
			networkContainerId := strings.Split(networkMode, ":")[1]
			log.Println(service.Name + ": detected container NetworkMode, linked to: " + networkContainerId[:12])
			networkContainer, err := b.docker.InspectContainer(networkContainerId)
			if err != nil {
				log.Println("unable to inspect network container:", networkContainerId[:12], err)
			} else {
				service.IP = networkContainer.NetworkSettings.IPAddress
				log.Println(service.Name + ": using network container IP " + service.IP)
			}
		}
	}

	if port.PortType == "udp" {
		service.Tags = combineTags(
			mapDefault(metadata, "tags", ""), b.config.ForceTags, "udp")
		service.ID = service.ID + ":udp"
	} else {
		service.Tags = combineTags(
			mapDefault(metadata, "tags", ""), b.config.ForceTags)
	}

	id := mapDefault(metadata, "id", "")
	if id != "" {
		service.ID = id
	}

	delete(metadata, "id")
	delete(metadata, "tags")
	delete(metadata, "name")
	service.Attrs = metadata
	service.TTL = b.config.RefreshTtl

	return service
}

func (b *Bridge) newServiceSwarm(port ServicePortSwarm, isgroup bool) *ServiceSwarm {
	//container := port.container
	//defaultName := strings.Split(path.Base(container.Config.Image), ":")[0]
	defaultName := port.Name

	// not sure about this logic. kind of want to remove it.
	hostname := Hostname
	if hostname == "" {
		hostname = port.IP
	}
	if port.IP == "0.0.0.0" {
		ip, err := net.ResolveIPAddr("ip", hostname)
		if err == nil {
			port.IP = ip.String()
		}
	}

	if b.config.HostIp != "" {
		port.IP = b.config.HostIp
	}

	metadata, metadataFromPort := serviceMetaDataSwarm(port)
	log.Println("metadata:", metadata)
	log.Println("metadatafromport", metadataFromPort)

	//ignore := mapDefault(metadata, "ignore", "")
	//if ignore != "" {
	//	return nil
	//}

	service := new(ServiceSwarm)
	service.Origin = port
	service.Name = mapDefault(metadata, "name", defaultName)
	service.ID = hostname + ":" + service.Name[1:] + ":" + port.Port

	//if isgroup && !metadataFromPort["name"] {
	//	service.Name += "-" + port.ExposedPort
	//}
	var p int

	if b.config.Internal == true {
		//service.IP = port.ExposedIP
		//p, _ = strconv.Atoi(port.ExposedPort)
	} else {
		service.IP = port.IP
		p, _ = strconv.Atoi(port.Port)
	}
	service.Port = p

	// if b.config.UseIpFromLabel != "" {
	// 	containerIp := container.Config.Labels[b.config.UseIpFromLabel]
	// 	if containerIp != "" {
	// 		slashIndex := strings.LastIndex(containerIp, "/")
	// 		if slashIndex > -1 {
	// 			service.IP = containerIp[:slashIndex]
	// 		} else {
	// 			service.IP = containerIp
	// 		}
	// 		log.Println("using container IP " + service.IP + " from label '" +
	// 			b.config.UseIpFromLabel + "'")
	// 	} else {
	// 		log.Println("Label '" + b.config.UseIpFromLabel +
	// 			"' not found in container configuration")
	// 	}
	// }
	//
	// // NetworkMode can point to another container (kuberenetes pods)
	// networkMode := container.HostConfig.NetworkMode
	// if networkMode != "" {
	// 	if strings.HasPrefix(networkMode, "container:") {
	// 		networkContainerId := strings.Split(networkMode, ":")[1]
	// 		log.Println(service.Name + ": detected container NetworkMode, linked to: " + networkContainerId[:12])
	// 		networkContainer, err := b.docker.InspectContainer(networkContainerId)
	// 		if err != nil {
	// 			log.Println("unable to inspect network container:", networkContainerId[:12], err)
	// 		} else {
	// 			service.IP = networkContainer.NetworkSettings.IPAddress
	// 			log.Println(service.Name + ": using network container IP " + service.IP)
	// 		}
	// 	}
	// }

	if port.PortType == "udp" {
		service.Tags = combineTags(
			mapDefault(metadata, "tags", ""), b.config.ForceTags, "udp")
		service.ID = service.ID + ":udp"
	} else {
		service.Tags = combineTags(
			mapDefault(metadata, "tags", ""), b.config.ForceTags)
	}

	//id := mapDefault(metadata, "id", "")
	id := port.ID
	if id != "" {
		service.ID = id
	}

	//delete(metadata, "id")
	//delete(metadata, "tags")
	//delete(metadata, "name")
	service.Attrs = metadata
	service.TTL = b.config.RefreshTtl

	return service
}

func (b *Bridge) remove(containerId string, deregister bool) {
	b.Lock()
	defer b.Unlock()

	if deregister {
		deregisterAll := func(services []*Service) {
			for _, service := range services {
				err := b.registry.Deregister(service)
				if err != nil {
					log.Println("deregister failed:", service.ID, err)
					continue
				}
				log.Println("removed:", containerId[:12], service.ID)
			}
		}
		deregisterAll(b.services[containerId])
		if d := b.deadContainers[containerId]; d != nil {
			deregisterAll(d.Services)
			delete(b.deadContainers, containerId)
		}
	} else if b.config.RefreshTtl != 0 && b.services[containerId] != nil {
		// need to stop the refreshing, but can't delete it yet
		b.deadContainers[containerId] = &DeadContainer{b.config.RefreshTtl, b.services[containerId]}
	}
	delete(b.services, containerId)
}

func (b *Bridge) removeSwarm(serviceId string, deregister bool) {
	b.Lock()
	defer b.Unlock()

	if deregister {
		deregisterAll := func(services []*ServiceSwarm) {
			for _, service := range services {
				err := b.registry.DeregisterSwarm(service)
				if err != nil {
					log.Println("deregister failed:", service.ID, err)
					continue
				}
				log.Println("removed:", serviceId, service.ID)
			}
		}
		deregisterAll(b.servicesSwarm[serviceId])
		// if d := b.deadContainers[serviceId]; d != nil {
		// 	deregisterAll(d.Services)
		// 	delete(b.deadContainers, serviceId)
		// }
	}
	// else if b.config.RefreshTtl != 0 && b.services[containerId] != nil {
	// 	// need to stop the refreshing, but can't delete it yet
	// 	b.deadContainers[serviceId] = &DeadContainer{b.config.RefreshTtl, b.services[serviceId]}
	// }
	delete(b.servicesSwarm, serviceId)
}

// bit set on ExitCode if it represents an exit via a signal
var dockerSignaledBit = 128

func (b *Bridge) shouldRemove(containerId string) bool {
	if b.config.DeregisterCheck == "always" {
		return true
	}
	container, err := b.docker.InspectContainer(containerId)
	if _, ok := err.(*dockerapi.NoSuchContainer); ok {
		// the container has already been removed from Docker
		// e.g. probabably run with "--rm" to remove immediately
		// so its exit code is not accessible
		log.Printf("registrator: container %v was removed, could not fetch exit code", containerId[:12])
		return true
	}

	switch {
	case err != nil:
		log.Printf("registrator: error fetching status for container %v on \"die\" event: %v\n", containerId[:12], err)
		return false
	case container.State.Running:
		log.Printf("registrator: not removing container %v, still running", containerId[:12])
		return false
	case container.State.ExitCode == 0:
		return true
	case container.State.ExitCode&dockerSignaledBit == dockerSignaledBit:
		return true
	}
	return false
}

var Hostname string

func init() {
	// It's ok for Hostname to ultimately be an empty string
	// An empty string will fall back to trying to make a best guess
	Hostname, _ = os.Hostname()
}
