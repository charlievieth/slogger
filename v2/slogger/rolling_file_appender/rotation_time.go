package rolling_file_appender

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

type RotationTime struct {
	Time     time.Time
	Serial   int
	Filename string
}

type RotationTimeSlice []*RotationTime

func (self RotationTimeSlice) Len() int {
	return len(self)
}

// // WARN: dev only
func (self RotationTimeSlice) String() string {
	names := make([]string, 0, len(self))
	for _, s := range self {
		names = append(names, filepath.Base(s.Filename))
	}
	return fmt.Sprintf("%q", names)
}

func (self RotationTimeSlice) Less(i, j int) bool {
	r1 := self[i]
	r2 := self[j]
	if r1.Time.Before(r2.Time) {
		return true
	}
	return r1.Time.Equal(r2.Time) && r1.Serial < r2.Serial
}

func (self RotationTimeSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

var rotatedTimeRegExp = regexp.MustCompile(`\.(\d+-\d\d-\d\dT\d\d-\d\d-\d\d)(-(\d+))?(\.gz)?$`)

func extractRotationTimeFromFilename(filename string) (*RotationTime, error) {
	match := rotatedTimeRegExp.FindStringSubmatch(filename)

	if match == nil {
		return nil, fmt.Errorf("Filename does not match rotation time format: %s", filename)
	}

	rotatedTime, err := time.Parse("2006-01-02T15-04-05", match[1])
	if err != nil {
		return nil, fmt.Errorf(
			"Time %s in filename %s did not parse: %v",
			match[1],
			filename,
			err,
		)
	}

	serial := 0
	if match[3] != "" {
		serial, err = strconv.Atoi(match[3])

		if err != nil {
			return nil, fmt.Errorf(
				"Could not parse serial number in filename %s: %v",
				filename,
				err,
			)
		}
	}

	return &RotationTime{rotatedTime, serial, filename}, nil
}
