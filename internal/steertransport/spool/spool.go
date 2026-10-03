// Package spool implements a harness-neutral, filesystem-backed steering
// transport for externally-owned interactive sessions.
package spool

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
)

const (
	Kind            = "spool"
	ProtocolVersion = 1
	MaxMessageBytes = 64 * 1024
)

type Transport struct{}

type Manifest struct {
	Protocol       int    `json:"protocol"`
	RegistrationID string `json:"registration_id"`
	IncarnationID  string `json:"incarnation_id"`
	Harness        string `json:"harness"`
	SessionID      string `json:"session_id"`
}

// WireMessage is the strict protocol-v1 message format consumed by a harness
// extension. Message identity is copied from the current registration.
type WireMessage struct {
	Protocol       int                        `json:"protocol"`
	MessageID      string                     `json:"message_id"`
	Kind           steertransport.MessageKind `json:"kind"`
	RegistrationID string                     `json:"registration_id"`
	IncarnationID  string                     `json:"incarnation_id"`
	Harness        string                     `json:"harness"`
	SessionID      string                     `json:"session_id"`
	Message        string                     `json:"message"`
}

func New() *Transport { return &Transport{} }

func (*Transport) Kind() string { return Kind }

func (*Transport) Prepare(_ context.Context, localRoot string, registration state.SteerRegistration) (steertransport.Endpoint, error) {
	paths, err := endpointPaths(localRoot, registration)
	if err != nil {
		return steertransport.Endpoint{}, err
	}
	if err := ensureDirectory(paths.base, localRoot); err != nil {
		return steertransport.Endpoint{}, err
	}
	for _, dir := range []string{paths.pending, paths.control, paths.rejected} {
		if err := ensureDirectory(dir, localRoot); err != nil {
			return steertransport.Endpoint{}, err
		}
	}
	manifest := manifestFor(registration)
	data, err := json.Marshal(manifest)
	if err != nil {
		return steertransport.Endpoint{}, fmt.Errorf("encode spool manifest: %w", err)
	}
	manifestPath := filepath.Join(paths.base, "registration.json")
	if err := publishImmutable(manifestPath, data); err != nil {
		return steertransport.Endpoint{}, fmt.Errorf("publish spool manifest: %w", err)
	}
	return endpoint(paths), nil
}

func (*Transport) Verify(_ context.Context, localRoot string, registration state.SteerRegistration) (steertransport.Endpoint, error) {
	paths, err := endpointPaths(localRoot, registration)
	if err != nil {
		return steertransport.Endpoint{}, err
	}
	for _, dir := range []string{filepath.Join(localRoot, "steer-spool"), filepath.Dir(paths.registration), paths.base, paths.pending, paths.control, paths.rejected} {
		if err := verifyPrivateDirectory(dir); err != nil {
			return steertransport.Endpoint{}, fmt.Errorf("verify spool endpoint directory: %w", err)
		}
	}
	if err := verifyManifest(paths, registration); err != nil {
		return steertransport.Endpoint{}, fmt.Errorf("verify spool endpoint manifest: %w", err)
	}
	return endpoint(paths), nil
}

func (*Transport) Deliver(_ context.Context, localRoot string, registration state.SteerRegistration, message steertransport.Message) error {
	paths, err := endpointPaths(localRoot, registration)
	if err != nil {
		return steertransport.Rejected(err)
	}
	if err := validateMessage(message, registration); err != nil {
		return steertransport.Rejected(err)
	}
	for _, dir := range []string{paths.base, paths.pending, paths.control, paths.rejected} {
		if err := inspectDirectory(dir); err != nil {
			return steertransport.Rejected(err)
		}
	}
	for _, dir := range []string{filepath.Dir(paths.registration), paths.registration} {
		if err := inspectDirectory(dir); err != nil {
			return steertransport.Rejected(err)
		}
	}
	if err := verifyManifest(paths, registration); err != nil {
		return steertransport.Rejected(err)
	}
	var retireErr error
	if message.Kind == steertransport.MessageSteer || message.Kind == steertransport.MessageStop {
		retireErr = retirePending(paths)
		if retireErr != nil && message.Kind == steertransport.MessageSteer {
			return steertransport.Rejected(retireErr)
		}
	}
	messageID, err := newMessageID()
	if err != nil {
		return fmt.Errorf("generate spool message ID: %w", err)
	}
	wire := WireMessage{
		Protocol: ProtocolVersion, MessageID: messageID, Kind: message.Kind,
		RegistrationID: registration.RegistrationID, IncarnationID: registration.IncarnationID,
		Harness: registration.Harness, SessionID: registration.SessionID, Message: message.Text,
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return steertransport.Rejected(fmt.Errorf("encode spool message: %w", err))
	}
	if len(data) > MaxMessageBytes {
		return steertransport.Rejected(fmt.Errorf("spool message exceeds %d bytes", MaxMessageBytes))
	}
	lane := paths.pending
	if message.Kind == steertransport.MessageStop {
		lane = paths.control
	}
	if err := publish(lane, wire.MessageID+".json", data); err != nil {
		if retireErr != nil {
			return fmt.Errorf("retire stale pending messages: %v; publish control message: %w", retireErr, err)
		}
		return err
	}
	if retireErr != nil {
		return fmt.Errorf("control message published but stale pending cleanup failed: %w", retireErr)
	}
	return nil
}

func (*Transport) Retire(_ context.Context, localRoot string, registration state.SteerRegistration) error {
	paths, err := endpointPaths(localRoot, registration)
	if err != nil {
		return err
	}
	if err := inspectDirectory(paths.registration); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := inspectDirectory(paths.base); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, dir := range []string{filepath.Dir(paths.registration), paths.registration} {
		if err := inspectDirectory(dir); err != nil {
			return err
		}
	}
	if err := verifyManifest(paths, registration); err != nil {
		return err
	}
	retirementID, err := newMessageID()
	if err != nil {
		return fmt.Errorf("generate spool retirement ID: %w", err)
	}
	retired := filepath.Join(paths.registration, ".retired-"+registration.IncarnationID+"-"+retirementID)
	if err := os.Rename(paths.base, retired); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("deactivate spool endpoint: %w", err)
	}
	if err := os.RemoveAll(retired); err != nil {
		return fmt.Errorf("remove retired spool endpoint: %w", err)
	}
	return nil
}

// Reconcile removes spool incarnations that are not owned by the current
// registration snapshot. Call it while holding the registration-store lock so
// Join cannot expose a prepared-but-not-yet-committed endpoint to cleanup.
func (*Transport) Reconcile(ctx context.Context, localRoot string, registrations []state.SteerRegistration) error {
	if ctx == nil {
		return errors.New("spool reconciliation context must not be nil")
	}
	if strings.TrimSpace(localRoot) == "" || !filepath.IsAbs(localRoot) || filepath.Clean(localRoot) != localRoot {
		return errors.New("spool local root must be an absolute canonical path")
	}
	rootInfo, err := os.Lstat(localRoot)
	if err != nil {
		return fmt.Errorf("inspect spool local root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return errors.New("spool local root must be a real directory")
	}
	spoolRoot := filepath.Join(localRoot, "steer-spool")
	if _, err := os.Lstat(spoolRoot); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect spool root: %w", err)
	}
	if err := verifyPrivateDirectory(spoolRoot); err != nil {
		return fmt.Errorf("verify spool root: %w", err)
	}

	active := make(map[string]struct{}, len(registrations))
	for _, registration := range registrations {
		if registration.Transport.Kind != Kind {
			continue
		}
		if _, err := endpointPaths(localRoot, registration); err != nil {
			return fmt.Errorf("validate current spool registration: %w", err)
		}
		active[registration.RegistrationID+"\x00"+registration.IncarnationID] = struct{}{}
	}

	registrationDirs, err := os.ReadDir(spoolRoot)
	if err != nil {
		return fmt.Errorf("read spool root: %w", err)
	}
	for _, registrationDir := range registrationDirs {
		if err := ctx.Err(); err != nil {
			return err
		}
		registrationPath := filepath.Join(spoolRoot, registrationDir.Name())
		if !validID(registrationDir.Name()) || !registrationDir.IsDir() {
			return fmt.Errorf("unexpected spool registration entry: %s", registrationPath)
		}
		if err := verifyPrivateDirectory(registrationPath); err != nil {
			return fmt.Errorf("verify spool registration directory: %w", err)
		}
		incarnations, err := os.ReadDir(registrationPath)
		if err != nil {
			return fmt.Errorf("read spool registration directory: %w", err)
		}
		for _, incarnation := range incarnations {
			if err := ctx.Err(); err != nil {
				return err
			}
			path := filepath.Join(registrationPath, incarnation.Name())
			if !incarnation.IsDir() || (!validID(incarnation.Name()) && !strings.HasPrefix(incarnation.Name(), ".retired-")) {
				return fmt.Errorf("unexpected spool incarnation entry: %s", path)
			}
			if err := verifyPrivateDirectory(path); err != nil {
				return fmt.Errorf("verify spool incarnation directory: %w", err)
			}
			if _, ok := active[registrationDir.Name()+"\x00"+incarnation.Name()]; ok {
				continue
			}
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove stale spool incarnation: %w", err)
			}
		}
		remaining, err := os.ReadDir(registrationPath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("recheck spool registration directory: %w", err)
		}
		if len(remaining) == 0 {
			if err := os.Remove(registrationPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove empty spool registration directory: %w", err)
			}
		}
	}
	return nil
}

// DecodeMessage strictly decodes a protocol-v1 spool message and verifies that
// it belongs to the supplied registration. Consumers should check the control
// lane before the pending lane; this decoder does not choose scheduling policy.
func DecodeMessage(data []byte, registration state.SteerRegistration) (WireMessage, error) {
	var message WireMessage
	if len(data) == 0 || len(data) > MaxMessageBytes {
		return message, fmt.Errorf("spool message size must be between 1 and %d bytes", MaxMessageBytes)
	}
	if err := decodeStrict(data, &message); err != nil {
		return WireMessage{}, fmt.Errorf("decode spool message: %w", err)
	}
	if !validID(registration.RegistrationID) || !validID(registration.IncarnationID) ||
		!validHarness(registration.Harness) || !validSession(registration.SessionID) ||
		!validID(message.MessageID) || !validID(message.RegistrationID) || !validID(message.IncarnationID) ||
		message.Protocol != ProtocolVersion ||
		(message.Kind != steertransport.MessageSteer && message.Kind != steertransport.MessageStop) ||
		message.RegistrationID != registration.RegistrationID || message.IncarnationID != registration.IncarnationID ||
		message.Harness != registration.Harness || message.SessionID != registration.SessionID ||
		message.Message == "" || strings.ContainsRune(message.Message, '\x00') {
		return WireMessage{}, errors.New("spool message has invalid or mismatched identity, protocol, kind, or content")
	}
	return message, nil
}

func manifestFor(registration state.SteerRegistration) Manifest {
	return Manifest{Protocol: ProtocolVersion, RegistrationID: registration.RegistrationID,
		IncarnationID: registration.IncarnationID, Harness: registration.Harness, SessionID: registration.SessionID}
}

func validateMessage(message steertransport.Message, registration state.SteerRegistration) error {
	if message.Kind != steertransport.MessageSteer && message.Kind != steertransport.MessageStop {
		return fmt.Errorf("unsupported spool message kind %q", message.Kind)
	}
	if message.Text == "" || strings.ContainsRune(message.Text, '\x00') || len(message.Text) > MaxMessageBytes {
		return errors.New("spool message must be non-empty, contain no NUL, and fit the protocol size limit")
	}
	if !validID(registration.RegistrationID) || !validID(registration.IncarnationID) ||
		!validHarness(registration.Harness) || !validSession(registration.SessionID) {
		return errors.New("spool registration identity is invalid")
	}
	return nil
}

func endpointPaths(localRoot string, registration state.SteerRegistration) (paths, error) {
	if strings.TrimSpace(localRoot) == "" || !filepath.IsAbs(localRoot) || filepath.Clean(localRoot) != localRoot {
		return paths{}, errors.New("spool local root must be an absolute canonical path")
	}
	if registration.Transport.Kind != Kind || len(registration.Transport.Params) != 0 {
		return paths{}, errors.New("spool transport accepts no parameters")
	}
	if !validID(registration.RegistrationID) || !validID(registration.IncarnationID) ||
		!validHarness(registration.Harness) || !validSession(registration.SessionID) {
		return paths{}, errors.New("spool registration identity is invalid")
	}
	rootInfo, err := os.Lstat(localRoot)
	if err != nil {
		return paths{}, fmt.Errorf("inspect spool local root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return paths{}, errors.New("spool local root must be a real directory")
	}
	registrationDir := filepath.Join(localRoot, "steer-spool", registration.RegistrationID)
	base := filepath.Join(registrationDir, registration.IncarnationID)
	for _, component := range []string{filepath.Join(localRoot, "steer-spool"), registrationDir, base} {
		info, err := os.Lstat(component)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return paths{}, fmt.Errorf("inspect spool path component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return paths{}, fmt.Errorf("spool path component must be a real directory: %s", component)
		}
	}
	return paths{registration: registrationDir, base: base,
		pending: filepath.Join(base, "pending"), control: filepath.Join(base, "control"),
		rejected: filepath.Join(base, "rejected")}, nil
}

type paths struct {
	registration string
	base         string
	pending      string
	control      string
	rejected     string
}

func endpoint(paths paths) steertransport.Endpoint {
	return steertransport.Endpoint{Kind: Kind, Protocol: ProtocolVersion, Root: paths.base,
		Pending: paths.pending, Control: paths.control, Rejected: paths.rejected}
}

func ensureDirectory(path, root string) error {
	if path == root {
		info, err := os.Lstat(root)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("spool local root must be a real directory")
		}
		return nil
	}
	parent := filepath.Dir(path)
	if err := ensureDirectory(parent, root); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("create spool directory %s: %w", path, err)
	}
	return inspectDirectory(path)
}

func inspectDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("spool path component must be a real directory: %s", path)
	}
	if err := securePrivateDirectory(path); err != nil {
		return fmt.Errorf("secure spool directory %s: %w", path, err)
	}
	info, err = os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !privateDirectoryMode(info) {
		return fmt.Errorf("spool directory is not private: %s", path)
	}
	return nil
}

func verifyPrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("spool path component must be a real directory: %s", path)
	}
	if !privateDirectoryMode(info) {
		return fmt.Errorf("spool directory is not private: %s", path)
	}
	return nil
}

func verifyManifest(paths paths, registration state.SteerRegistration) error {
	path := filepath.Join(paths.base, "registration.json")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 4096 || !privateFileMode(info) {
		return errors.New("spool manifest must be a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var got Manifest
	if err := decodeStrict(data, &got); err != nil {
		return fmt.Errorf("decode spool manifest: %w", err)
	}
	want := manifestFor(registration)
	if got != want {
		return errors.New("spool manifest does not match registration")
	}
	return nil
}

func publishImmutable(path string, data []byte) error {
	if _, err := os.Lstat(path); err == nil {
		return verifyManifest(paths{base: filepath.Dir(path)}, manifestFromBytes(data))
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect spool manifest: %w", err)
	}
	if err := publish(filepath.Dir(path), filepath.Base(path), data); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyManifest(paths{base: filepath.Dir(path)}, manifestFromBytes(data))
		}
		return err
	}
	return nil
}

func manifestFromBytes(data []byte) state.SteerRegistration {
	var manifest Manifest
	if decodeStrict(data, &manifest) != nil {
		return state.SteerRegistration{}
	}
	return state.SteerRegistration{RegistrationID: manifest.RegistrationID, IncarnationID: manifest.IncarnationID,
		Harness: manifest.Harness, SessionID: manifest.SessionID, Transport: state.SteerTransportRoute{Kind: Kind}}
}

func publish(dir, name string, data []byte) error {
	return publishWithRename(dir, name, data, os.Rename)
}

func publishWithRename(dir, name string, data []byte, rename func(string, string) error) error {
	tmp, err := os.CreateTemp(dir, ".spool-*.tmp")
	if err != nil {
		return fmt.Errorf("create spool temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secure spool temporary file: %w", err)
	}
	written, writeErr := tmp.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return fmt.Errorf("write spool temporary file: %w", writeErr)
	}
	target := filepath.Join(dir, name)
	if _, err := os.Lstat(target); err == nil {
		return fmt.Errorf("spool message already exists: %s: %w", name, os.ErrExist)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect spool target: %w", err)
	}
	if err := rename(tmpPath, target); err != nil {
		return fmt.Errorf("publish spool message (visibility uncertain): %w", err)
	}
	return nil
}

func retirePending(paths paths) error {
	entries, err := os.ReadDir(paths.pending)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		path := filepath.Join(paths.pending, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || !privateFileMode(info) {
			return fmt.Errorf("pending spool entry must be a regular file: %s", path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("retire stale spool message %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("spool JSON is not valid UTF-8")
	}
	if err := rejectDuplicateObjectKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func rejectDuplicateObjectKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("spool JSON value must be an object")
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("spool JSON object key must be a string")
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate spool JSON field %q", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func validID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func validHarness(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 64 || value == "." || value == ".." {
		return false
	}
	for index, char := range value {
		valid := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' ||
			index > 0 && (char == '.' || char == '_' || char == '-')
		if !valid {
			return false
		}
	}
	return true
}

func validSession(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 4096 && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func newMessageID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
