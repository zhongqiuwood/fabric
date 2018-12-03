package config

import (
	"net"
)

// GetLocalIP returns the non loopback local IP of the host
// TODO: this should be a singleton service (the result can be reused) and should use more way to
// obtain the real "external" IP for server
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback then display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
