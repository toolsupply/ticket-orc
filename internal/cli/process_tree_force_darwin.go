package cli

import (
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

var readDarwinProcessGroup = darwinProcessGroupSnapshot

func forceExitedProcessGroup(pgid int) error {
	err := signalProcessGroupID(pgid, syscall.SIGKILL)
	if !errors.Is(err, syscall.EPERM) {
		return err
	}
	// XNU killpg1 skips SZOMB members and returns EPERM when none can be
	// signalled, even for our own zombie-only group. Inspect only this ambiguous
	// post-exit result; never suppress a denied request to a live member.
	// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sig.c
	data, inspectErr := readDarwinProcessGroup(pgid)
	if inspectErr != nil {
		return errors.Join(err, fmt.Errorf("inspect post-exit group %d: %w", pgid, inspectErr))
	}
	if darwinZombieOnlyGroup(data, pgid) {
		// This removes a redundant request error, not boundary uncertainty.
		// Members may have escaped the group; no full-boundary proof is gained.
		return nil
	}
	return err
}

// Darwin's 64-bit kinfo_proc ABI (amd64 and arm64): 648-byte records, with
// extern_proc.p_stat at byte 36 and p_pid at byte 40. KERN_PROC_PGRP includes
// both live and zombie members. Require the exact unreaped leader as well as
// valid, entirely zombie records; missing or unfamiliar data is not evidence.
// See XNU bsd/sys/{proc,sysctl}.h and bsd/kern/kern_sysctl.c.
const darwinKinfoProcSize = 648

func darwinZombieOnlyGroup(data []byte, leader int) bool {
	if len(data) == 0 || len(data)%darwinKinfoProcSize != 0 {
		return false
	}
	foundLeader := false
	for offset := 0; offset < len(data); offset += darwinKinfoProcSize {
		record := data[offset : offset+darwinKinfoProcSize]
		pid := int(int32(binary.LittleEndian.Uint32(record[40:44])))
		if record[36] != 5 /* SZOMB */ || pid <= 0 {
			return false
		}
		foundLeader = foundLeader || pid == leader
	}
	return foundLeader
}

func darwinProcessGroupSnapshot(pgid int) ([]byte, error) {
	mib := [4]int32{1, 14, 2, int32(pgid)} // CTL_KERN, KERN_PROC, KERN_PROC_PGRP
	var size uintptr
	_, _, errno := syscall.Syscall6(syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), 0,
		uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return nil, errno
	}
	if size == 0 {
		return nil, nil
	}
	data := make([]byte, size)
	_, _, errno = syscall.Syscall6(syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), uintptr(unsafe.Pointer(&data[0])),
		uintptr(unsafe.Pointer(&size)), 0, 0)
	// A growing group can exhaust the sized buffer. Do not retry or accept a
	// partial snapshot; retain EPERM and the inspection error instead.
	if errno != 0 {
		return nil, errno
	}
	if size > uintptr(len(data)) {
		return nil, fmt.Errorf("process group snapshot exceeds buffer")
	}
	return data[:size], nil
}
