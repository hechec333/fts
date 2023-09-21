//go:build windows

package plat

import (
	"syscall"
	"unsafe"
)

func GetFsBlockSize(filename string) int64 {
	// kernel32, err := syscall.LoadLibrary("kernel32.dll")
	// if err != nil {
	// 	panic(err)
	// }
	// defer syscall.FreeLibrary(kernel32)
	// getDiskFreeSpace, err := syscall.GetProcAddress(kernel32, "GetDiskFreeSpaceW")
	// if err != nil {
	// 	panic(err)
	// }
	// u16, err := syscall.UTF16PtrFromString(filepath.VolumeName(filename))
	// if err != nil {
	// 	panic(err)
	// }
	// var sectorsPerCluster, bytesPerSector uint32
	// _, _, err = syscall.SyscallN(uintptr(getDiskFreeSpace), 5,
	// 	uintptr(unsafe.Pointer(u16)),
	// 	uintptr(unsafe.Pointer(&sectorsPerCluster)),
	// 	uintptr(unsafe.Pointer(&bytesPerSector)),
	// 	0, 0, 0)

	kernel32, _ := syscall.LoadLibrary("kernel32.dll")
	getDiskFreeSpace, _ := syscall.GetProcAddress(kernel32, "GetDiskFreeSpaceW")

	var sectorsPerCluster, bytesPerSector uint32
	_, _, _ = syscall.Syscall6(uintptr(getDiskFreeSpace), 5,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr("C:\\"))),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		0, 0, 0)
	// if err != nil {
	// 	panic(err)
	// }
	return int64(sectorsPerCluster * bytesPerSector)
}
