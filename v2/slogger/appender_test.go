package slogger

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// WARN: DELETE ME
func convertOffsetToString(offset int) string {
	if offset == 0 {
		return "+0000"
	}
	var sign string
	if offset > 0 {
		sign = "+"
	} else {
		sign = "-"
		offset *= -1
	}
	hoursOffset := float32(offset) / 3600.0
	var leadingZero string
	if hoursOffset > -9 && hoursOffset < 9 {
		leadingZero = "0"
	}
	// TODO: "%.0f" is the same as an int - only difference is rounding
	return fmt.Sprintf("%s%s%.0f", sign, leadingZero, hoursOffset*100.0)
}

// WARN: DELETE ME
// Reference implementation
func timestampWithTimezoneReference(t time.Time) string {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	millisec := t.Nanosecond() / 1000000
	_, offset := t.Zone() // offset in seconds

	// TODO: this is close to RFC3339Nano - we should use time.Format()
	return fmt.Sprintf("[%.4d-%.2d-%.2dT%.2d:%.2d:%.2d.%.3d%s]",
		year, month, day,
		hour, min, sec,
		millisec,
		convertOffsetToString(offset),
	)
}

// [2024-06-08T22:18:42.194+0000]
func timestampWithTimezone(t time.Time) string {
	return t.Format("[2006-01-02T15:04:05.000-0700]")
}

func BenchmarkFormatTimestamp(b *testing.B) {
	now := time.Now()
	for i := 0; i < b.N; i++ {
		// timestampWithTimezoneReference(now)
		timestampWithTimezone(now)
	}
}

type TimeStampTest struct {
	// time time.Time
	loc  string
	want string
}

var timeStampTests = []TimeStampTest{
	{"Local", "[2024-11-07T09:10:11.000-0500]"},
	{"UTC", "[2024-11-07T09:10:11.000+0000]"},
	{"Etc/GMT", "[2024-11-07T09:10:11.000+0000]"},
	{"America/New_York", "[2024-11-07T09:10:11.000-0500]"},
	{"America/St_Johns", "[2024-11-07T09:10:11.000-0330]"},
	{"Asia/Baku", "[2024-11-07T09:10:11.000+0400]"},
	{"Asia/Beirut", "[2024-11-07T09:10:11.000+0200]"},
	{"Asia/Kathmandu", "[2024-11-07T09:10:11.000+0545]"},
	{"Asia/Shanghai", "[2024-11-07T09:10:11.000+0800]"},
	{"Europe/Warsaw", "[2024-11-07T09:10:11.000+0100]"},
	{"Pacific/Easter", "[2024-11-07T09:10:11.000-0500]"},
	{"Pacific/Fiji", "[2024-11-07T09:10:11.000+1200]"},
}

func TestFormatTimestampXX(t *testing.T) {
	for _, test := range timeStampTests {
		t.Run(strings.ReplaceAll(test.loc, "/", "_"), func(t *testing.T) {
			loc, err := time.LoadLocation(test.loc)
			if err != nil {
				t.Fatal(err)
			}
			tt := time.Date(2024, 11, 7, 9, 10, 11, 12345, loc)
			got := timestampWithTimezone(tt)
			if got != test.want {
				t.Errorf("timestampWithTimezone(%q) = %q; want: %q", tt, got, test.want)
			}
		})
	}
}

// func TestFormatTimestamp(t *testing.T) {
// 	test := func(t *testing.T, ts time.Time) {
// 		got := timestampWithTimezone(ts)
// 		want := timestampWithTimezoneReference(ts)
// 		if got != want {
// 			t.Errorf("timestampWithTimezone(%s) = %q; want: %q",
// 				ts, got, want)
// 		}
// 	}
// 	zones := []string{
// 		"Local",
// 		"UTC",
// 		"Etc/GMT",
// 		"America/New_York",
// 		"America/St_Johns",
// 		"Asia/Baku",
// 		"Asia/Beirut",
// 		"Asia/Kathmandu",
// 		"Asia/Shanghai",
// 		"Europe/Warsaw",
// 		"Pacific/Easter",
// 		"Pacific/Fiji",
// 	}
// 	for _, name := range zones {
// 		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
// 			loc, err := time.LoadLocation(name)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			test(t, time.Now().In(loc))
// 		})
// 	}
// }

func TestFormatLog(t *testing.T) {
	formatLogOld := func(log *Log, timePart string) string {

		errorCodeStr := ""
		if log.ErrorCode != NoErrorCode {
			errorCodeStr += fmt.Sprintf("[%v] ", log.ErrorCode)
		}

		return fmt.Sprintf("%v [%v.%v] [%v:%v:%d] %v%v\n",
			timePart, log.Prefix, log.Level.Type(),
			log.Filename, log.FuncName, log.Line,
			errorCodeStr,
			log.Message())
	}

	reference := func(log *Log) string {
		year, month, day := log.Timestamp.Date()
		hour, min, sec := log.Timestamp.Clock()
		millisec := log.Timestamp.Nanosecond() / 1000000

		return formatLogOld(log, fmt.Sprintf("[%.4d/%.2d/%.2d %.2d:%.2d:%.2d.%.3d]",
			year, month, day,
			hour, min, sec,
			millisec,
		))
	}

	fmtVerbs := make([]string, 10)
	for i := range fmtVerbs {
		fmtVerbs[i] = "%d"
	}

	rr := rand.New(rand.NewSource(time.Now().UnixNano()))
	genRandomLog := func() *Log {
		args := make([]interface{}, rr.Intn(len(fmtVerbs)))
		for i := range args {
			args[i] = rr.Int31n(256)
		}
		var ts time.Time
		if rr.Float64() > 0.01 {
			ts = time.Now().Add(time.Duration(rr.Int63n(int64(time.Hour))))
		}
		return &Log{
			Prefix:     fmt.Sprintf("prefix_%d", rr.Intn(2048)),
			Level:      Level(rr.Intn(int(topLevel))),
			ErrorCode:  ErrorCode(rr.Intn(128)),
			Filename:   fmt.Sprintf("file_%d.go", rr.Intn(1024)),
			Line:       rr.Intn(4096),
			Timestamp:  ts,
			MessageFmt: "{" + strings.Join(fmtVerbs[:len(args)], " ") + "}",
			Args:       args,
			Context:    nil,
		}
	}

	testLogs := make([]*Log, 32)
	for i := range testLogs {
		testLogs[i] = genRandomLog()
	}

	// testLogs := []Log{
	// 	{
	// 		Prefix:     "prefix",
	// 		Level:      WARN,
	// 		ErrorCode:  1,
	// 		Filename:   "file.go",
	// 		Line:       123,
	// 		Timestamp:  time.Now(),
	// 		MessageFmt: "%s %d %s",
	// 		Args:       []interface{}{"a", 2, "b"},
	// 		Context:    nil,
	// 	},
	// }
	for _, ll := range testLogs {
		want := reference(ll)
		got := FormatLog(ll)
		if got != want {
			t.Errorf("FormatLog(%+v)\ngot:  %q;\nwant: %q", ll, got, want)
		}
	}

	// // [2024/03/15 18:41:05.147]
	// // time.Now().Format("2006/01/02 15:04:05.999")

	// now := time.Now()
	// ll := Log{Timestamp: now}
	// t.Error("want:", reference(&ll))
	// t.Error("got: ", now.Format("[2006/01/02 15:04:05.000]"))
}

func BenchmarkFormatLog(b *testing.B) {
	ll := Log{
		Prefix:     "prefix",
		Level:      WARN,
		ErrorCode:  1,
		Filename:   "file.go",
		Line:       123,
		Timestamp:  time.Now(),
		MessageFmt: "%s %d %s",
		Args:       []interface{}{"a", 2, "b"},
		Context:    nil,
	}
	// ll := Log{Timestamp: time.Now()} // TODO: use a fixed time
	for i := 0; i < b.N; i++ {
		FormatLogWithTimezone(&ll)
		// formatLogX(&ll, "[2006-01-02T15:04:05.000-0700]")
		// formatLog(&ll, ll.Timestamp.Format("[2006-01-02T15:04:05.000-0700]"))
	}
}

func TestGetFormatLogFunc(t *testing.T) {
	orig := GetFormatLogFunc()
	t.Cleanup(func() { SetFormatLogFunc(orig) })
	var n int
	for i := 1; i <= 3; i++ {
		SetFormatLogFunc(func(*Log) string {
			n = i
			return ""
		})
		GetFormatLogFunc()(nil)
		if n != i {
			t.Errorf("n = %d; want: %d", n, i)
		}
	}
}

func BenchmarkFlushFile(b *testing.B) {
	f, err := os.Create(b.TempDir() + "/tmp.log")
	if err != nil {
		b.Fatal(err)
	}
	// b.Log(f.Name())
	b.Cleanup(func() {
		f.Close()
		os.Remove(f.Name())
	})
	msg := strings.Repeat("a", 255) + "\n"
	b.SetBytes(int64(len(msg)))
	var sz int
	for i := 0; i < b.N; i++ {
		n, err := f.WriteString(msg)
		if err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
		sz += n
		if sz >= 128*1024*1024 {
			b.StopTimer()
			if _, err := f.Seek(0, 0); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}
