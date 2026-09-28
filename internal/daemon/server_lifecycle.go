package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// NewServer validates configuration and creates an unstarted server.
func NewServer(config Config) (*Server, error) {
	if strings.TrimSpace(config.StateDir) == "" {
		return nil, fmt.Errorf("daemon state directory must not be empty")
	}
	if err := ValidateEndpointKey(config.EndpointKey); err != nil {
		return nil, err
	}
	listenAddress, err := NormalizeListenAddress(config.ListenAddress)
	if err != nil {
		return nil, err
	}
	if err := ValidateListenPort(config.Port); err != nil {
		return nil, err
	}
	config.ListenAddress = listenAddress
	if config.Protocol == 0 {
		config.Protocol = endpointProtocol
	}
	if config.Protocol < 1 {
		return nil, fmt.Errorf("daemon protocol must be positive")
	}
	if config.PID == 0 {
		config.PID = os.Getpid()
	}
	if config.PID < 1 {
		return nil, fmt.Errorf("daemon PID must be positive")
	}
	if config.Version == "" {
		config.Version = "dev"
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if config.ReadHeaderTimeout < 0 {
		return nil, fmt.Errorf("daemon read header timeout must not be negative")
	}
	if config.EventHeartbeat == 0 {
		config.EventHeartbeat = defaultEventHeartbeat
	}
	if config.EventHeartbeat < 0 {
		return nil, fmt.Errorf("daemon event heartbeat must not be negative")
	}
	return &Server{config: config, done: make(chan error, 1), shutdownDone: make(chan struct{}), events: newEventBroker()}, nil
}

// Start binds the configured IP address and port and atomically publishes the
// endpoint before serving requests. A context cancellation shuts the server
// down through the same bounded path as an explicit Shutdown call.
// Once setup begins, Start is one-shot: it retains and returns the first setup
// failure on later calls. A successful start cannot be repeated.
func (s *Server) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("daemon context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("daemon server has already been started")
	}
	s.mu.Unlock()
	s.startOnce.Do(func() {
		_, err := prepareRuntimeDir(s.config.StateDir)
		if err != nil {
			s.startErr = err
			return
		}
		listener, err := net.Listen("tcp", listenNetworkAddress(s.config.ListenAddress, s.config.Port))
		if err != nil {
			s.startErr = fmt.Errorf("listen daemon at %s: %w", listenNetworkAddress(s.config.ListenAddress, s.config.Port), err)
			return
		}
		addr, ok := listener.Addr().(*net.TCPAddr)
		configuredIP := net.ParseIP(s.config.ListenAddress)
		if !ok || addr.IP == nil || configuredIP == nil || !addr.IP.Equal(configuredIP) {
			_ = listener.Close()
			s.startErr = fmt.Errorf("daemon listener address does not match configured IP")
			return
		}
		s.startedAt = time.Now().UTC()
		s.endpoint = Endpoint{
			Version:       endpointVersion,
			Protocol:      s.config.Protocol,
			PID:           s.config.PID,
			URL:           "http://" + net.JoinHostPort(clientAddressForListener(s.config.ListenAddress), strconv.Itoa(addr.Port)),
			ListenAddress: addr.IP.String(),
			Port:          addr.Port,
			EndpointKey:   s.config.EndpointKey,
		}
		s.endpointPath = endpointPath(s.config.StateDir)
		lifecycle, lifecycleCancel := context.WithCancel(ctx)
		s.listener = listener
		s.lifecycle = lifecycle
		s.lifecycleCancel = lifecycleCancel
		s.http = &http.Server{
			Handler:           s.handler(),
			ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		}
		// Initialize the listener and complete control-plane setup before making
		// this endpoint discoverable to clients.
		if err := publishEndpoint(s.endpointPath, s.endpoint); err != nil {
			lifecycleCancel()
			_ = listener.Close()
			s.startErr = err
			return
		}
		s.mu.Lock()
		s.started = true
		s.mu.Unlock()
		go s.serve()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = s.Shutdown(shutdownCtx)
		}()
	})
	if s.startErr != nil {
		return s.startErr
	}
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return fmt.Errorf("daemon server has already been started")
	}
	return nil
}

func (s *Server) serve() {
	err := s.http.Serve(s.listener)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	s.done <- err
	close(s.done)
}

// Done returns a channel that receives the HTTP serve result and then closes.
func (s *Server) Done() <-chan error { return s.done }

// Endpoint returns a copy of the published discovery metadata. The token is
// included because this method is intended for a local daemon client, not API
// status serialization.
func (s *Server) Endpoint() Endpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpoint
}

// Shutdown stops accepting requests, removes this server's endpoint metadata,
// and is safe to call repeatedly.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("daemon shutdown context must not be nil")
	}
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	if s.stopped {
		shutdownDone := s.shutdownDone
		s.mu.Unlock()
		select {
		case <-shutdownDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.stopped = true
	if s.lifecycleCancel != nil {
		s.lifecycleCancel()
	}
	s.events.close()
	httpServer := s.http
	endpointPath := s.endpointPath
	s.mu.Unlock()
	err := httpServer.Shutdown(ctx)
	if removeErr := os.Remove(endpointPath); removeErr != nil && !os.IsNotExist(removeErr) {
		err = errors.Join(err, fmt.Errorf("remove daemon endpoint: %w", removeErr))
	}
	close(s.shutdownDone)
	return err
}

// MutationContext returns a daemon-owned bounded context for a validated
// state-changing operation. The request context is checked for nil but its
// cancellation is intentionally ignored; daemon shutdown still cancels the
// returned context immediately.
func (s *Server) MutationContext(request context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if request == nil {
		return nil, nil, fmt.Errorf("mutation request context must not be nil")
	}
	if timeout <= 0 {
		return nil, nil, fmt.Errorf("mutation timeout must be positive")
	}
	s.mu.Lock()
	base := s.lifecycle
	started := s.started && !s.stopped
	s.mu.Unlock()
	if !started || base == nil {
		return nil, nil, fmt.Errorf("daemon server is not running")
	}
	mutation, cancel := context.WithTimeout(base, timeout)
	return mutation, cancel, nil
}

// Close performs a bounded shutdown for callers that do not already own a
// shutdown context.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	return s.Shutdown(ctx)
}

// SetRepositoryGateway installs the typed read-only repository gateway before
// the server starts. The gateway is intentionally separate from worker
// control and accepts only configured Orc repository keys.
func (s *Server) SetRepositoryGateway(gateway RepositoryGateway) error {
	if s == nil {
		return fmt.Errorf("daemon server is unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("repository gateway cannot change after daemon start")
	}
	s.config.RepositoryGateway = gateway
	return nil
}
