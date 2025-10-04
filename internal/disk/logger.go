package disk

import (
	"log"
	"strconv"

	"github.com/d-Rickyy-b/certstream-server-go/internal/certstream"
	"github.com/d-Rickyy-b/certstream-server-go/internal/disk/filerotate"
)

var (
	CertStreamEntryChan chan certstream.Entry
)

type DiskLog string

const (
	DISK_LOG_FULL         DiskLog = "FULL"
	DISK_LOG_LITE         DiskLog = "LITE"
	DISK_LOG_DOMAINS_ONLY DiskLog = "DOMAINS_ONLY"
)

func StartLogger(logDirectory string, logType DiskLog, rotation string) {

	if CertStreamEntryChan == nil {
		CertStreamEntryChan = make(chan certstream.Entry, 10_000)
	}

	go logEntries(logDirectory, logType, rotation)
}

func logEntries(logDirectory string, logType DiskLog, rotation string) {
	var logFile *filerotate.RotatableFile
	var err error

	switch rotation {
	case "HOURLY":
		logFile, err = filerotate.New(logDirectory, filerotate.ROTATE_HOURLY)
	case "DAILY":
		fallthrough
	default:
		logFile, err = filerotate.New(logDirectory, filerotate.ROTATE_DAILY)
	}

	if err != nil {
		log.Panic(err)
	}

	for {
		entry, ok := <-CertStreamEntryChan

		if !ok {
			break
		}

		switch logType {
		case DISK_LOG_DOMAINS_ONLY:
			for _, domain := range entry.Data.LeafCert.AllDomains {
				logFile.Write([]byte(
					"\"" + domain + "\"," +
						strconv.FormatInt(entry.Data.LeafCert.NotBefore, 10) + "," +
						strconv.FormatInt(entry.Data.LeafCert.NotAfter, 10) + "," +
						"\"" + *entry.Data.LeafCert.Issuer.O + "\"\n"))
			}
		case DISK_LOG_LITE:
			logFile.Write(entry.JSONLite())
		case DISK_LOG_FULL:
			fallthrough
		default:
			logFile.Write(entry.JSON())
		}
	}

	logFile.Close()
}
