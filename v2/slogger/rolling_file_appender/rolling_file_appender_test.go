// Copyright 2013, 2015 MongoDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rolling_file_appender

import (
	"bufio"
	"bytes"
	"compress/gzip"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	. "github.com/mongodb/slogger/v2/slogger/test_util"
)

const rfaTestLogDir = "log"
const rfaTestLogFilename = "logger_rfa_test.log"
const rfaTestLogPath = rfaTestLogDir + "/" + rfaTestLogFilename

func TestLog(test *testing.T) {
	defer teardown()
	appender, logger := setup(test, 1000, 0, 10, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This is a log message")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertCurrentLogContains(test, "This is a log message")
}

func TestNoRotation(test *testing.T) {
	defer teardown()

	appender, logger := setup(test, 1000, 0, 10, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This is under 1,000 characters and should not cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertNumLogFiles(test, 1)
}

func TestNoRotation2(test *testing.T) {
	defer teardown()

	appender, logger := setup(test, -1, 0, 10, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This should not cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertNumLogFiles(test, 1)
}

func TestOldLogRemoval(test *testing.T) {
	defer teardown()

	appender, logger := setup(test, 10, 0, 2, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This is more than 10 characters and should cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())
	assertNumLogFiles(test, 2)

	_, errs = logger.Logf(slogger.WARN, "This is more than 10 characters and should cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())
	assertNumLogFiles(test, 3)

	_, errs = logger.Logf(slogger.WARN, "This is more than 10 characters and should cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())
	if err := appender.Close(); err != nil {
		test.Fatal(err)
	}
	assertNumLogFiles(test, 3)
}

func TestPreRotation(test *testing.T) {
	createLogDir(test)

	file, err := os.Create(rfaTestLogPath)
	if err != nil {
		test.Fatalf("Failed to create empty logfile: %v", err)
	}

	err = file.Close()
	if err != nil {
		test.Fatalf("Failed to close logfile: %v", err)
	}

	appender, logger := newAppenderAndLogger(test, 1000, 0, 2, true)
	defer appender.Close()
	AssertNoErrors(test, logger.Flush())
	assertNumLogFiles(test, 2)
}

func TestRotationSizeBased(test *testing.T) {
	defer teardown()

	appender, logger := setup(test, 10, 0, 10, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This is more than 10 characters and should cause a log rotation")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertNumLogFiles(test, 2)
}

func TestRotationTimeBased(test *testing.T) {
	defer teardown()

	func() {
		appender, logger := setup(test, -1, 100*time.Millisecond, 10, false)
		defer appender.Close()

		assertNumLogFiles(test, 1)
		time.Sleep(100*time.Millisecond + 50*time.Millisecond)
		_, errs := logger.Logf(slogger.WARN, "Trigger log rotation 1")
		AssertNoErrors(test, errs)
		assertNumLogFiles(test, 2)

		time.Sleep(100*time.Millisecond + 50*time.Millisecond)
		_, errs = logger.Logf(slogger.WARN, "Trigger log rotation 2")
		AssertNoErrors(test, errs)
		assertNumLogFiles(test, 3)
	}()

	// Test that time-based log rotation still works if we recreate
	// the appender.  This forces the state file to be read in
	appender, logger := newAppenderAndLogger(test, -1, 100*time.Millisecond, 10, false)
	defer appender.Close()

	assertNumLogFiles(test, 3)
	time.Sleep(time.Second + 50*time.Millisecond)
	_, errs := logger.Logf(slogger.WARN, "Trigger log rotation 3")
	AssertNoErrors(test, errs)
	assertNumLogFiles(test, 4)
}

func TestRotationManual(test *testing.T) {
	defer teardown()
	appender, _ := setup(test, -1, 0, 10, false)
	defer appender.Close()

	assertNumLogFiles(test, 1)

	if err := appender.Rotate(); err != nil {
		test.Fatal("appender.Rotate() returned an error: " + err.Error())
	}
	assertNumLogFiles(test, 2)

	if err := appender.Rotate(); err != nil {
		test.Fatal("appender.Rotate() returned an error: " + err.Error())
	}
	assertNumLogFiles(test, 3)
}

func TestReopen(test *testing.T) {
	defer teardown()

	// simulate manual log rotation via Reopen()

	appender, logger := setup(test, 0, 0, 0, false)
	defer appender.Close()

	_, errs := logger.Logf(slogger.WARN, "This is a log message 1")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertCurrentLogContains(test, "This is a log message 1")

	rotatedLogPath := rfaTestLogPath + ".rotated"
	if err := os.Rename(rfaTestLogPath, rotatedLogPath); err != nil {
		test.Fatalf("os.Rename() returned an error: %v", err)
	}

	if _, err := os.Stat(rfaTestLogPath); err == nil {
		test.Fatal(rfaTestLogPath + " should not exist after rename")
	}

	assertLogContains(test, rotatedLogPath, "This is a log message 1")

	_, errs = logger.Logf(slogger.WARN, "This is a log message 2")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertLogContains(test, rotatedLogPath, "This is a log message 2")

	// WARN
	appender.Close()

	if err := appender.Reopen(); err != nil {
		test.Fatalf("Error reopening log: %v", err)
	}

	assertLogContains(test, rotatedLogPath, "This is a log message 1")
	assertLogContains(test, rotatedLogPath, "This is a log message 2")

	assertCurrentLogDoesNotContain(test, "This is a log message 1")
	assertCurrentLogDoesNotContain(test, "This is a log message 2")

	_, errs = logger.Logf(slogger.WARN, "This is a log message 3")
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	assertCurrentLogContains(test, "This is a log message 3")
	assertLogDoesNotContain(test, rotatedLogPath, "This is a log message 3")
}

func TestCompressionOnRotation(test *testing.T) {
	// defer teardown()
	test.Cleanup(func() {
		if test.Failed() {
			test.Log("LOG FILE:", rfaTestLogPath)
		} else {
			teardown()
		}
	})

	appender, logger := setup(test, 10, 0, 10, false)
	appender.compressRotatedLogs = true
	appender.maxUncompressedLogs = 1
	defer appender.Close()

	compressibleMessage := strings.Repeat("This string is easily compressible", 1000)

	_, errs := logger.Logf(slogger.WARN, compressibleMessage)
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	checkFiles := func() (compressedLogFiles, sizeCompressedFile int) {
		err := filepath.Walk(rfaTestLogDir, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(info.Name(), ".gz") {
				compressedLogFiles++
				sizeCompressedFile = int(info.Size())
			}
			return err
		})
		if err != nil {
			test.Error(err)
		}
		return
	}

	compressedLogFiles, _ := checkFiles()
	assertNumLogFiles(test, 2)
	if compressedLogFiles != 0 {
		test.Errorf("expected to find no compressed log files")
	}

	_, errs = logger.Logf(slogger.WARN, compressibleMessage)
	AssertNoErrors(test, errs)
	AssertNoErrors(test, logger.Flush())

	// WARN: this fails because close does not wait for
	// the compression loop to finish
	if err := appender.Close(); err != nil {
		test.Fatal(err)
	}

	compressedLogFiles, sizeCompressedFile := checkFiles()
	assertNumLogFiles(test, 3)
	if compressedLogFiles != 1 {
		test.Errorf("expected to find one compressed log files")
	}
	if sizeCompressedFile >= len(compressibleMessage)/10 {
		test.Errorf("expected the compressed log file size %v to be smaller than the logged bytes %v", sizeCompressedFile, len(compressibleMessage))
	}
}

// Stress test in parallel to detect race conditions
func TestParallel(t *testing.T) {
	const maxFileSize = 64 * 1024
	tmp := t.TempDir()
	logName := filepath.Join(tmp, "test.log")
	builder := NewBuilder(logName, maxFileSize, time.Minute, 4, true, nil).
		WithLogCompression(1)

	appender, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	ll := slogger.Logger{
		Prefix:    "compress_test",
		Appenders: []slogger.Appender{appender},
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1_000; i++ {
				if err := appender.Flush(); err != nil {
					t.Error(err)
					return
				}
				ll.Logf(slogger.WARN, "message")
				if i != 0 && i%250 == 0 {
					if err := appender.Rotate(); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if err := appender.Close(); err != nil {
		t.Fatal(err)
	}

	// Make sure all the gzip files are valid

	validateGzip := func(name string) {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		gr, err := gzip.NewReader(bufio.NewReaderSize(f, 256*1024*1024))
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(gr); err != nil {
			t.Fatal(err)
		}
		if err := gr.Close(); err != nil {
			t.Fatal(err)
		}
		if buf.Len() == 0 {
			t.Fatal("empty gzip file:", filepath.Base(name))
		}
	}

	fis, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) == ".gz" {
			validateGzip(filepath.Join(tmp, fi.Name()))
		}
	}
}

func assertCurrentLogContains(test *testing.T, expected string) {
	assertLogContains(test, rfaTestLogPath, expected)
}

func assertCurrentLogDoesNotContain(test *testing.T, notExpected string) {
	assertLogDoesNotContain(test, rfaTestLogPath, notExpected)
}

func assertLogContains(test *testing.T, logPath, expected string) {
	actual := readLog(test, logPath)

	if !strings.Contains(actual, expected) {
		test.Errorf("Log %s contains: \n%s\ninstead of\n%s", logPath, actual, expected)
	}
}

func assertLogDoesNotContain(test *testing.T, logPath, notExpected string) {
	actual := readLog(test, logPath)

	if strings.Contains(actual, notExpected) {
		test.Errorf("Log %s should not contain: \n%s", logPath, notExpected)
	}
}

func assertNumLogFiles(test *testing.T, expected_n int) {
	test.Helper()
	got := listLogFiles(test)

	if expected_n != len(got) {
		test.Errorf(
			"Expected number of log files to be %d, not %d:\n%s",
			expected_n,
			len(got),
			strings.Join(got, "\n"),
		)
	}
}

func createLogDir(test *testing.T) {
	os.RemoveAll(rfaTestLogDir)
	err := os.MkdirAll(rfaTestLogDir, 0777)

	if err != nil {
		test.Fatal("setup() failed to create directory: " + err.Error())
	}
}

func randomBytes(t testing.TB, sz int) []byte {
	n := base64.StdEncoding.DecodedLen(sz)
	b := make([]byte, n)
	if _, err := io.ReadFull(crand.Reader, b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.AppendEncode(nil, b)
}

func TestCompressionDoesNotBlock(t *testing.T) {
	const maxFileSize = 20 * 1024 * 1024
	tmp := tempDir(t)
	logName := filepath.Join(tmp, "test.log")
	builder := NewBuilder(logName, maxFileSize, time.Minute, 4, true, nil).
		WithLogCompression(1).
		WithErrorLog(log.New(os.Stdout, "# ERROR: ", log.Lshortfile))

	appender, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	ll := slogger.Logger{
		Prefix:    "compress_test",
		Appenders: []slogger.Appender{appender},
	}

	msg := string(randomBytes(t, 256*1024))

	// Write less than max file size logs (so we don't rotate)
	// to calculate the average write time.
	start := time.Now()
	for i := 0; i < 40; i++ {
		ll.Logf(slogger.INFO, msg)
	}
	avg := time.Since(start) / 40
	t.Logf("average: %s", avg)

	if appender.curFileSize+int64(len(msg)) >= maxFileSize {
		t.Fatalf("wrote more than maxFileSize %d > %d",
			appender.curFileSize, maxFileSize)
	}

	start = time.Now()
	errCount := 0
	for i := 0; i < 20*1024*1024/len(msg); i++ {
		tt := time.Now()
		ll.Logf(slogger.INFO, msg)
		d := time.Since(tt)
		if d > avg*1_000 {
			t.Errorf("%d: write time: %s exceeds average %s by %dx",
				i, d, avg, d/avg)
			errCount++
			if errCount > 10 {
				break
			}
		}
	}
	d := time.Since(start)
	t.Logf("total: %s avg: %s", d, d/time.Duration(20*1024*1024/len(msg)))
	appender.Close()
}

// WARN: remove
func BenchmarkSync(b *testing.B) {
	f, err := os.Create(b.TempDir() + "/bench.txt")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	data := bytes.Repeat([]byte{'a'}, 256)
	b.SetBytes(int64(len(data)))

	n := 0
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(data); err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
		n += len(data)
		if n >= 1024*1024*128 {
			b.StopTimer()
			if _, err := f.Seek(0, 0); err != nil {
				b.Fatal(err)
			}
			if err := f.Truncate(0); err != nil {
				b.Fatal(err)
			}
			n = 0
			b.StartTimer()
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	// headers := func() []string { return []string{"header 1", "header 2"} }
	// appender, err := New(os.DevNull, -1, -1, -1, false, headers)
	appender, err := New(os.DevNull, -1, -1, -1, false, nil)
	if err != nil {
		b.Fatal(err)
	}
	ll := &slogger.Logger{
		Prefix:    "bench",
		Appenders: []slogger.Appender{appender},
	}

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			// ll.Logf(slogger.WARN, "log message: %d", i)
			ll.Logf(slogger.WARN, "log message")
		}
	})
}

func newAppenderAndLogger(test *testing.T, maxFileSize int64, maxDuration time.Duration, maxRotatedLogs int, rotateIfExists bool) (appender *RollingFileAppender, logger *slogger.Logger) {
	appender, err := New(
		rfaTestLogPath,
		maxFileSize,
		maxDuration,
		maxRotatedLogs,
		rotateIfExists,
		func() []string {
			return []string{"This is a header", "more header"}
		},
	)
	// WARN WARN WARN
	appender.errLog = log.New(os.Stdout, "# ERROR: ", log.Lshortfile)

	if err != nil {
		test.Fatal("NewRollingFileAppender() failed: " + err.Error())
	}

	logger = &slogger.Logger{
		Prefix:    "rfa",
		Appenders: []slogger.Appender{appender},
	}

	return
}

func listLogFiles(t *testing.T) []string {
	cwd, err := os.Open(rfaTestLogDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cwd.Close()

	names, err := cwd.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}

	a := names[:0]
	for _, s := range names {
		if !strings.HasPrefix(s, ".") {
			a = append(a, s)
		}
	}
	sort.Strings(a)
	return a
}

func readLog(test *testing.T, logPath string) string {
	bytes, err := ioutil.ReadFile(logPath)
	if err != nil {
		test.Fatal("Could not read log file")
	}

	return string(bytes)
}

func setup(test *testing.T, maxFileSize int64, maxDuration time.Duration, maxRotatedLogs int, rotateIfExists bool) (appender *RollingFileAppender, logger *slogger.Logger) {
	createLogDir(test)

	return newAppenderAndLogger(test, maxFileSize, maxDuration, maxRotatedLogs, rotateIfExists)
}

func teardown() {
	os.RemoveAll(rfaTestLogDir)
}

func tempDir(t testing.TB) string {
	t.Helper()
	tmp, err := os.MkdirTemp("", "slogger-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("# Test log directory: %q", tmp)
		} else {
			os.RemoveAll(tmp)
		}
	})
	return tmp
}

func TestAsyncCompression(t *testing.T) {
	t.Skip("FIXME")
	tmp := tempDir(t)

	// errLog := log.New(os.Stderr, "# error: ", log.Lshortfile)
	errLog := log.New(os.Stdout, "# ERROR: ", log.Lshortfile)

	logName := filepath.Join(tmp, "test.log")
	builder := NewBuilder(logName, 10, 10, 10, true, func() []string {
		return []string{
			"header1",
			"header2",
			"header3",
			"header4",
		}
	}).WithLogCompression(1).WithErrorLog(errLog)

	appender, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	ll := slogger.Logger{
		Prefix:    "compress_test",
		Appenders: []slogger.Appender{appender},
	}
	ll.Logf(slogger.ERROR, "messageFmt")
	ll.Logf(slogger.ERROR, "messageFmt")
	ll.Logf(slogger.ERROR, "messageFmt")
	ll.Logf(slogger.ERROR, "messageFmt")
	appender.Close()
	tree(tmp)

	t.Fatal("FIXME") // WARN
}

// WARN: remove this
func tree(dir string) {
	cmd := exec.Command("tree", dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

type WriterFunc func(b []byte) (int, error)

func (w WriterFunc) Write(b []byte) (int, error) {
	return w(b)
}

func TestGzipCompressSize(t *testing.T) {
	data, err := os.ReadFile("testdata/automation-agent.log")
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(WriterFunc(func(b []byte) (int, error) {
		// fmt.Println(len(b))
		return len(b), nil
	}))
	gw.Write(data)
}

func BenchmarkFlush(b *testing.B) {
	tmp := tempDir(b)
	logName := filepath.Join(tmp, "test.log")
	builder := NewBuilder(logName, 10, 10, 10, true, nil).WithLogCompression(5)
	appender, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	appender.syncInterval = time.Millisecond * 100
	ll := slogger.Logger{
		Prefix:    "compress_test",
		Appenders: []slogger.Appender{appender},
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ll.Flush()
		}
	})
}

func TestRotatedFilename(t *testing.T) {
	// Reference implementation - from the old code
	ref := func(baseFilename string, t time.Time, serial int) string {
		filename := fmt.Sprintf(
			"%s.%d-%02d-%02dT%02d-%02d-%02d",
			baseFilename,
			t.Year(),
			t.Month(),
			t.Day(),
			t.Hour(),
			t.Minute(),
			t.Second(),
		)
		if serial > 0 {
			filename = fmt.Sprintf("%s-%d", filename, serial)
		}
		return filename
	}

	rr := rand.New(rand.NewSource(1))
	now := time.Now()
	const limit = int64(time.Hour * 24 * 365)
	for i := 0; i < 100; i++ {
		for serial := 0; serial < 3; serial++ {
			tt := now.Add(time.Duration(rr.Int63n(limit)))
			want := ref("test.log", tt, serial)
			got := rotatedFilename("test.log", tt, serial)
			if got != want {
				t.Errorf("rotatedFilename(%q, %s, %d) = %q; want: %q", "test.log", tt, serial, got, want)
			}
		}
	}
}

func touch(t testing.TB, name string) {
	t.Helper()
	f, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNextLogFileName(t *testing.T) {
	N := 1024 + 1
	if testing.Short() {
		N = 100
	}

	base := filepath.Join(t.TempDir(), "test.log")
	touch(t, base)

	now := time.Now()

	// Create a file without the serial number - needed for the
	// below test
	touch(t, rotatedFilename(base, now, 0))

	for i := 1; i < N; i++ {
		touch(t, rotatedFilename(base, now, i))
		want := rotatedFilename(base, now, i+1)
		got, err := nextLogFileName(base, now)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%d: %q != %q", i, filepath.Base(got), filepath.Base(want))
		}
	}
}
