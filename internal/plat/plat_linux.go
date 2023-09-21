//go:build linux
// +build linux

package plat

import "syscall"

func GetFsBlockSize(filename string) int64 {
	var stat syscall.Statfs_t
	err := syscall.Statfs(filename, &stat)
	if err != nil {
		panic(err)
	}
	return stat.Bsize
}
