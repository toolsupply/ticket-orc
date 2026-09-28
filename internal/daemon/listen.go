package daemon

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	// DefaultListenAddress is the local bind address used when none is set.
	DefaultListenAddress = "127.0.0.1"
	// DefaultListenPort requests an OS-assigned port for each daemon instance.
	DefaultListenPort = 0
)

// NormalizeListenAddress validates a configured daemon listen address and
// returns its canonical IP literal. Host names are rejected rather than
// resolved so the bind address is deterministic.
func NormalizeListenAddress(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return DefaultListenAddress, nil
	}
	ip := net.ParseIP(value)
	if ip == nil {
		return "", fmt.Errorf("daemon listen address must be an IP literal")
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		return ipv4.String(), nil
	}
	return ip.String(), nil
}

func clientAddressForListener(address string) string {
	ip := net.ParseIP(address)
	if ip == nil {
		return DefaultListenAddress
	}
	if ip.IsUnspecified() {
		if ip.To4() != nil {
			return DefaultListenAddress
		}
		return "::1"
	}
	return ip.String()
}

// ValidateListenPort validates a configured TCP port. Zero retains the
// existing OS-assigned ephemeral-port behavior; privileged ports are rejected
// because a foreground user process should not require elevated privileges.
func ValidateListenPort(port int) error {
	if port < 0 || port > 65535 {
		return fmt.Errorf("daemon listen port must be between 0 and 65535")
	}
	if port > 0 && port < 1024 {
		return fmt.Errorf("daemon listen port %d is privileged; choose a port from 1024 to 65535", port)
	}
	return nil
}

func listenNetworkAddress(address string, port int) string {
	return net.JoinHostPort(address, strconv.Itoa(port))
}
