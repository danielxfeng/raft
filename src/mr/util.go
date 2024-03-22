package mr

import (
	"log"
	"os"
)

// Debug Debug mode switch.
const Debug = false

// DPrintf a log printer.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// SetupLogging set the output of the logging.
func SetupLogging() {
	if Debug {
		file, err := os.OpenFile("logfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Util.SetupLogging, Error when creating log file, %v", err)
			return
		}
		log.SetOutput(file)
	}
}
