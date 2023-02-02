package svcdiscover

import (
	"fmt"
	"net"
)

// GetOutboundIP get preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, nil
}

func GetLocalIPv4() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, address := range addrs {
		// check loopback address
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.To4(), nil
			}
		}
	}
	return nil, fmt.Errorf("no found ip")
}

// GetSpareTCPPort find a spare TCP port
func GetSpareTCPPort(ip string, portBegin int) (port int) {
LOOP:
	for port = portBegin; ; port++ {
		addr := fmt.Sprintf("%s:%d", ip, port)
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			ln.Close()
			break LOOP
		}
	}
	return
}
