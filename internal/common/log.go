package common

import "log"

var debug = true

func INFO(format string, args ...any) {
	log.Printf("[INFO] "+format, args...)
}
func WARN(format string, args ...any) {
	log.Printf("[WARN] "+format, args...)
}
func FAIL(format string, args ...any) {
	log.Printf("[FAIL] "+format, args...)
}

func DINFO(format string, args ...any) {
	if debug {
		log.Printf("[INFO] "+format, args...)
	}
}
func DWARN(format string, args ...any) {
	if debug {
		log.Printf("[WARN] "+format, args...)
	}
}
func DFAIL(format string, args ...any) {
	if debug {
		log.Printf("[FAIL] "+format, args...)
	}
}
