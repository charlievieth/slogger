// Copyright 2013, 2014 MongoDB, Inc.
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

// Package rolling_file_appender provides a slogger Appender that
// supports log rotation.

package rolling_file_appender

import (
	"bufio"
	"log"

	"github.com/mongodb/slogger/v2/slogger"

	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type RollingFileAppender struct {
	// These fields should not need to change
	maxFileSize          int64
	maxDuration          time.Duration
	maxRotatedLogs       int
	compressRotatedLogs  bool
	maxUncompressedLogs  int
	absPath              string
	headerGenerator      func() []string
	stringWriterCallback func(*os.File) slogger.StringWriter // CEV: why a callback ???

	lock sync.Mutex

	// These fields can change and the lock should be held when
	// reading or writing to them after construction of the
	// RollingFileAppender struct
	file        *os.File
	curFileSize int64

	// state holds "state" that is written to disk in a hidden state
	// file.  Not all "state" needs to go in here.  For example, the
	// current file size can be determined by a stat system call on
	// the file.  This state pointer should always be non-nil.  The
	// lock should also be held when reading or writing to state.
	state *state

	// Held when compressing logs. This is a separate mutex since
	// compression happens in its own goroutine and we don't want
	// to block the logger.
	compressLock sync.Mutex
	kick         chan struct{} // trigger compression
	stop         chan struct{} // close to stop compress loop
	done         chan struct{} // closed after the compress loop exits
	initialized  bool          // compress loop initialized
	closed       bool          // logger closed

	// TODO: remove this
	errLog *log.Logger // Log for internal errors (testing only)
}

type rollingFileAppenderBuilder struct {
	filename             string
	maxFileSize          int64
	maxDuration          time.Duration
	maxRotatedLogs       int
	rotateIfExists       bool
	compressRotatedLogs  bool
	maxUncompressedLogs  int
	headerGenerator      func() []string
	stringWriterCallback func(*os.File) slogger.StringWriter
	errLog               *log.Logger // Log for internal errors (testing only)
}

// NewBuilder returns a new rollingFileAppenderBuilder. You can directly
// call Build() to create a new RollingFileAppender, or configure
// additional options first.
//
// filename is path to the file to log to.  It can be a relative path
// (with respect to the current working directory) or an absolute
// path.
//
// maxFileSize is the approximate file size that will be allowed
// before the log file is rotated.  Rotated log files will have suffix
// of the form .YYYY-MM-DDTHH-MM-SS or .YYYY-MM-DDTHH-MM-SS-N (where N
// is an incrementing serial number used to resolve conflicts)
// appended to them.  Set maxFileSize to a non-positive number if you
// wish there to be no limit.
//
// maxDuration is how long to wait before rotating the log file.  Set
// to 0 if you do not want log rotation to be time-based.
//
// If both maxFileSize and maxDuration are set than the log file will
// be rotated whenever either threshold is met.  The duration used to
// determine whether a log file should be rotated (that is, the
// duration compared to maxDuration) is reset regardless of why the
// log was rotated previously.
//
// maxRotatedLogs specifies the maximum number of rotated logs allowed
// before old logs are deleted.  Set to a non-positive number if you
// do not want old log files to be deleted.
//
// If rotateIfExists is set to true and a log file with the same
// filename already exists, then the current one will be rotated.  If
// rotateIfExists is set to false and a log file with the same
// filename already exists, then the current log file will be appended
// to.  If a log file with the same filename does not exist, then a
// new log file is created regardless of the value of rotateIfExists.
//
// As RotatingFileAppender might be wrapped by an AsyncAppender, an
// errHandler can be provided that will be called when an error
// occurs.  It can set to nil if you do not want to provide one.
//
// The return value headerGenerator, if not nil, is logged at the
// beginning of every log file.
//
// Note that after building a RollingFileAppender with Build(), you will
// probably want to defer a call to RollingFileAppender's Close() (or
// at least Flush()).  This ensures that in case of program exit
// (normal or panicking) that any pending logs are logged.
func NewBuilder(filename string, maxFileSize int64, maxDuration time.Duration, maxRotatedLogs int, rotateIfExists bool, headerGenerator func() []string) *rollingFileAppenderBuilder {
	return &rollingFileAppenderBuilder{
		filename:             filename,
		maxFileSize:          maxFileSize,
		maxDuration:          maxDuration,
		maxRotatedLogs:       maxRotatedLogs,
		rotateIfExists:       rotateIfExists,
		compressRotatedLogs:  false,
		maxUncompressedLogs:  0,
		headerGenerator:      headerGenerator,
		stringWriterCallback: nil,
	}
}

func (b *rollingFileAppenderBuilder) WithLogCompression(maxUncompressedLogs int) *rollingFileAppenderBuilder {
	b.compressRotatedLogs = true
	b.maxUncompressedLogs = maxUncompressedLogs
	return b
}

func (b *rollingFileAppenderBuilder) WithStringWriter(stringWriterCallback func(*os.File) slogger.StringWriter) *rollingFileAppenderBuilder {
	b.stringWriterCallback = stringWriterCallback
	return b
}

// WARN: use or remove
func (b *rollingFileAppenderBuilder) WithErrorLog(log *log.Logger) *rollingFileAppenderBuilder {
	b.errLog = log
	return b
}

func (b *rollingFileAppenderBuilder) Build() (*RollingFileAppender, error) {
	if b.headerGenerator == nil {
		b.headerGenerator = func() []string {
			return []string{}
		}
	}
	if b.stringWriterCallback == nil {
		b.stringWriterCallback = func(f *os.File) slogger.StringWriter {
			return f
		}
	}

	absPath, err := filepath.Abs(b.filename)
	if err != nil {
		return nil, err
	}

	appender := &RollingFileAppender{
		maxFileSize:          b.maxFileSize,
		maxDuration:          b.maxDuration,
		maxRotatedLogs:       b.maxRotatedLogs,
		compressRotatedLogs:  b.compressRotatedLogs,
		maxUncompressedLogs:  b.maxUncompressedLogs,
		absPath:              absPath,
		headerGenerator:      b.headerGenerator,
		stringWriterCallback: b.stringWriterCallback,
		errLog:               b.errLog, // WARN: testing only
		kick:                 make(chan struct{}, 1),
		stop:                 make(chan struct{}),
		done:                 make(chan struct{}),
	}

	fileInfo, err := os.Stat(absPath)
	if err == nil && b.rotateIfExists { // err == nil means file exists
		return appender, appender.rotate()
	} else {
		// we're either creating a new log file or appending to the current one
		appender.file, err = os.OpenFile(
			absPath,
			os.O_WRONLY|os.O_APPEND|os.O_CREATE,
			0666,
		)
		if err != nil {
			return nil, err
		}

		if fileInfo != nil {
			appender.curFileSize = fileInfo.Size()
		}

		// WARN: this will cause tests to fail !!!
		const Benchmark = false // WARN

		if !Benchmark {
			stateExistsVar, err := stateExists(appender.statePath())
			if err != nil {
				appender.file.Close()
				return nil, err
			}

			if stateExistsVar {
				if err = appender.loadState(); err != nil {
					appender.file.Close()
					return nil, err
				}
			} else {
				if err = appender.stampStartTime(); err != nil {
					appender.file.Close()
					return nil, err
				}
			}
		}

		return appender, appender.logHeader()
	}
}

// New creates a new RollingFileAppender.
//
// This is deprecated in favor of calling NewBuilder().Build()
func New(filename string, maxFileSize int64, maxDuration time.Duration, maxRotatedLogs int, rotateIfExists bool, headerGenerator func() []string) (*RollingFileAppender, error) {
	return NewBuilder(filename, maxFileSize, maxDuration, maxRotatedLogs, rotateIfExists, headerGenerator).Build()
}

func NewWithStringWriter(filename string, maxFileSize int64, maxDuration time.Duration, maxRotatedLogs int, rotateIfExists bool, headerGenerator func() []string, stringWriterCallback func(*os.File) slogger.StringWriter) (*RollingFileAppender, error) {
	return NewBuilder(filename, maxFileSize, maxDuration, maxRotatedLogs, rotateIfExists, headerGenerator).WithStringWriter(stringWriterCallback).Build()
}

func (a *RollingFileAppender) errorf(format string, v ...any) {
	if a.errLog != nil {
		a.errLog.Output(2, fmt.Sprintf(format, v...))
	}
}

func (self *RollingFileAppender) shouldRotate() bool {
	if self.maxFileSize > 0 && self.curFileSize > self.maxFileSize {
		return true
	}
	if self.maxDuration > 0 && self.state != nil {
		return time.Since(self.state.LogStartTime) > self.maxDuration
	}
	return false
}

func (self *RollingFileAppender) Append(log *slogger.Log) error {
	msg := slogger.GetFormatLogFunc()(log)

	// TODO: we should rotate before writing the message

	self.lock.Lock()
	defer self.lock.Unlock()
	if self.file == nil {
		return NoFileError{}
	}
	n, err := self.stringWriterCallback(self.file).WriteString(msg)
	self.curFileSize += int64(n)

	if self.shouldRotate() {
		if err2 := self.rotate(); err2 != nil {
			if err == nil {
				err = err2
			} else {
				err = fmt.Errorf("append: write error: %w rotate error: %w", err, err2)
			}
		}
	}
	return err
}

func (self *RollingFileAppender) closeFileLocked() error {
	if self.file == nil {
		return nil
	}
	// TODO: ignore fs.ErrClosed errors?
	err := self.file.Sync()
	if err != nil {
		// WARN: don't use SyncError - this is the only place that uses it!
		err = &SyncError{Filename: self.absPath, Err: err}
	}
	// Always close the file even if sync error'd.
	if err2 := self.file.Close(); err2 != nil && err == nil {
		err = &CloseError{Filename: self.absPath, Err: err2}
	}
	self.file = nil
	return err
}

func (self *RollingFileAppender) closeLocked() error {
	if self.closed {
		return ErrAppenderClose
	}
	self.closed = true
	if self.compressRotatedLogs {
		self.compressLogs()
		close(self.stop)
		<-self.done
	}
	return self.closeFileLocked()
}

func (self *RollingFileAppender) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.closeLocked()
}

func (self *RollingFileAppender) Flush() error {
	// NB(charlie): This method used to call os.File.Sync (fsync) which is
	// extremely expensive and really not necessary for logs. Depending on
	// the filesystem and underlying storage medium this can decrease write
	// IOPs to the 10s or low 100s (on a M1 macOS calling fsync after each
	// write decreases write IOPs by a factor of 3100x).
	//
	// Calling fsync will also put additional pressure on the underlying storage
	// system which may adversely impact other programs running on the system
	// (e.g. a database).
	//
	// Additionally, some Appenders like AsyncAppender call this method after
	// each call to Append so this is often a very hot function.
	//
	// TODO(charlie): If at some point we wanted to sync the file to disk we
	// should only do so periodically.
	return nil
}

// WARN: compression is now asynchronous - should we wait for it to complete?
func (self *RollingFileAppender) Rotate() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.rotate()
}

// WARN: This is not used in the agent and I can't think of any reason
// why this function should exist.
//
// Useful for manual log rotation.  For example, logrotated may rename
// the log file and then ask us to reopen it.  Before reopening it we
// will be writing to the renamed log file.  After reopening we will
// be writing to a new log file with the original name.
func (self *RollingFileAppender) Reopen() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	// close current log if we have one open
	if err := self.closeLocked(); err != nil && err != ErrAppenderClose {
		return err
	}

	// WARN: read the size from the opened file (otherwise there is a race)
	fileInfo, err := os.Stat(self.absPath)
	if err == nil { // file exists
		self.curFileSize = fileInfo.Size()
	} else { // file does not exist
		self.curFileSize = 0
	}

	file, err := os.OpenFile(self.absPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666) // umask applies to perms
	if err != nil {
		self.file = nil
		return &OpenError{self.absPath, err}
	}
	self.file = file
	self.logHeader()

	// stamp start time
	if err = self.stampStartTime(); err != nil {
		return err
	}

	// remove really old logs
	self.removeMaxRotatedLogs()

	// Reset the close channels
	self.closed = false
	self.stop = make(chan struct{})
	self.done = make(chan struct{})

	return nil
}

func rotatedFilename(baseFilename string, t time.Time, serial int) string {
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

func (self *RollingFileAppender) appendSansSizeTracking(log *slogger.Log) (bytesWritten int, err error) {
	if self.file == nil {
		return 0, &NoFileError{}
	}
	f := slogger.GetFormatLogFunc()
	msg := f(log)
	bytesWritten, err = self.stringWriterCallback(self.file).WriteString(msg)

	if err != nil {
		err = &WriteError{self.absPath, err}
	}

	return
}

func (self *RollingFileAppender) logHeader() error {
	header := self.headerGenerator()
	for _, line := range header {

		log := &slogger.Log{
			Prefix:     "header",
			Level:      slogger.INFO,
			Filename:   "",
			Line:       0,
			Timestamp:  time.Now(),
			MessageFmt: line,
			Args:       []interface{}{},
		}

		// do not count header as part of size towards rotation in
		// order to prevent infinite rotation when max size is smaller
		// than header
		_, err := self.appendSansSizeTracking(log)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *RollingFileAppender) removeMaxRotatedLogs() error {
	if self.maxRotatedLogs <= 0 {
		return nil
	}

	rotationTimes, err := self.rotationTimeSlice()

	if err != nil {
		return &MinorRotationError{err}
	}

	numLogsToDelete := len(rotationTimes) - self.maxRotatedLogs

	// return if we're under the limit
	if numLogsToDelete <= 0 {
		return nil
	}

	// otherwise remove enough of the oldest logfiles to bring us
	// under the limit
	sort.Sort(rotationTimes)
	var first error
	for _, rotationTime := range rotationTimes[:numLogsToDelete] {
		if err := os.Remove(rotationTime.Filename); err != nil {
			// Ignore errors due to the file not existing and only return
			// the first error since stopping here breaks rotation.
			if !os.IsNotExist(err) && first == nil {
				first = err
			}
		}
	}
	if first != nil {
		return &MinorRotationError{first}
	}
	return nil
}

func (self *RollingFileAppender) compressMaxUncompressedLogs() error {
	if self.maxUncompressedLogs < 0 {
		return nil
	}
	self.compressLock.Lock() // TODO: with better synchronization we should not need this
	defer self.compressLock.Unlock()

	rotationTimes, err := self.rotationTimeSlice()
	if err != nil {
		return &MinorRotationError{err}
	}

	uncompressedRotationTimes := make(RotationTimeSlice, 0, len(rotationTimes))
	for _, v := range rotationTimes {
		if !strings.HasSuffix(v.Filename, ".gz") {
			uncompressedRotationTimes = append(uncompressedRotationTimes, v)
		}
	}

	numLogsToCompress := len(uncompressedRotationTimes) - self.maxUncompressedLogs
	if numLogsToCompress <= 0 {
		return nil
	}

	sort.Sort(uncompressedRotationTimes)
	for _, rt := range uncompressedRotationTimes[:numLogsToCompress] {
		if err := compressFile(rt.Filename); err != nil {
			return fmt.Errorf("rolling_file_appender: compressing file %s: %w",
				rt.Filename, err)
		}
	}
	return nil
}

func compressFile(name string) error {
	// 256kb is the optimal buffer size on most systems as determined by the GNU
	// coreutils team
	//
	// This script from coreutils can be instructive:
	// https://github.com/coreutils/coreutils/blob/master/src/ioblksize.h#L24-L77
	//
	const bufsize = 256 * 1024

	fi, err := os.Open(name)
	if err != nil {
		return err
	}
	defer fi.Close()

	info, err := fi.Stat()
	if err != nil {
		return err
	}

	// TODO: consider using a temp file (though this may lead to a race if
	// compressMaxUncompressedLogs() is called while this is running - using
	// singleflight would solve this
	fo, err := os.OpenFile(name+".gz", os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		// panic(err)
		return err
	}
	defer fo.Close()

	exit := func(err error) error {
		fo.Close()
		os.Remove(fo.Name()) // remove compressed log file on error
		return err

		// TODO: should we do this or let the caller handle it???
		// return fmt.Errorf("rolling_file_appender: error compressing file %s: %w", err)
	}

	bw := bufio.NewWriterSize(fo, bufsize)

	gw := gzip.NewWriter(bw)
	gw.ModTime = info.ModTime()

	// If anything here errors - remove the file.
	if _, err := io.Copy(gw, fi); err != nil {
		return exit(err)
	}
	if err := gw.Close(); err != nil {
		return exit(err)
	}
	if err := bw.Flush(); err != nil {
		return exit(err)
	}
	// Pedantically ensure we write this file disk.
	if err := fo.Sync(); err != nil {
		return exit(err)
	}
	if err := fo.Close(); err != nil {
		return exit(err)
	}
	_ = fi.Close() // ignore close error

	// TODO(charlie): This appears to be a "nice to have" and was
	// copied from the original implementation. An error here is
	// not fatal, but we should log it (once we decide on how to
	// log internal errors).
	_ = os.Chtimes(fo.Name(), time.Now(), info.ModTime())

	return os.Remove(name)
}

func (self *RollingFileAppender) compressLoop() {
	if !self.compressRotatedLogs {
		return
	}
	defer close(self.done)
	for {
		select {
		case <-self.kick:
			// TODO(charlie): log these errors either ourselves or write
			// them to STDERR.
			if err := self.compressMaxUncompressedLogs(); err != nil {
				// WARN: Log this error
				self.errorf("compress: %v", err)
			}
			if err := self.removeMaxRotatedLogs(); err != nil {
				self.errorf("remove max rotated logs: %v", err)
			}
		case <-self.stop:
			// Check if there is a request to rotate logs before exiting
			select {
			case <-self.kick:
				if err := self.compressMaxUncompressedLogs(); err != nil {
					// WARN: Log this error
					self.errorf("compress: %v", err)
				}
				if err := self.removeMaxRotatedLogs(); err != nil {
					self.errorf("remove max rotated logs: %v", err)
				}
			default:
			}
			return
		}
	}
}

func (self *RollingFileAppender) compressLogs() {
	// NOTE: lock much be held
	if !self.compressRotatedLogs {
		return
	}
	if !self.initialized {
		self.initialized = true
		go self.compressLoop()
	}
	select {
	case self.kick <- struct{}{}:
	default:
	}
}

// WARN: this is an insane value
const MAX_ROTATE_SERIAL_NUM = 1_000_000_000

func (self *RollingFileAppender) renameLogFile(oldFilename string) error {
	now := time.Now()

	var newFilename string
	// var err error

	// TODO: read the directory to get a list of names and use that
	// as the starting point for "serial".
	for serial := 0; ; serial++ { // err == nil means file exists
		if serial > MAX_ROTATE_SERIAL_NUM {
			return &RenameError{
				OldFilename: oldFilename,
				NewFilename: newFilename,
				Err:         fmt.Errorf("Reached max serial number: %d", MAX_ROTATE_SERIAL_NUM),
			}
		}
		// TODO: we can cache the result of this
		newFilename = rotatedFilename(self.absPath, now, serial)

		if _, err := os.Lstat(newFilename); err != nil {
			if os.IsNotExist(err) {
				break
			}
			// WARN: handle/log any other error we encounter here
			return err
		}
	}

	if err := os.Rename(oldFilename, newFilename); err != nil {
		return &RenameError{oldFilename, newFilename, err}
	}
	return nil
}

// func (self *RollingFileAppender) renameLogFile_XXX(oldFilename string) error {
// 	now := time.Now()
// 	// TODO: read the directory to get a list of names and use that
// 	// as the starting point for "serial".
// 	for serial := 0; serial < 100_000; serial++ { // err == nil means file exists
// 		// TODO: we can cache the result of this
// 		name := rotatedFilename(self.absPath, now, serial)
// 		_, err := os.Lstat(name)
// 		if err != nil && !os.IsNotExist(err) {
// 			return err
// 		}
// 		if err == nil {
// 			continue
// 		}
// 		// Exclusively create the file to prevent TOCTOU bugs.
// 		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
// 		if err != nil {
// 			continue
// 		}
// 		f.Close()
// 		if err := os.Rename(oldFilename, name); err != nil {
// 			return &RenameError{oldFilename, name, err}
// 		}
// 	}
// 	return &RenameError{
// 		OldFilename: oldFilename,
// 		NewFilename: rotatedFilename(self.absPath, now, 100_000),
// 		Err:         fmt.Errorf("Reached max serial number: %d", MAX_ROTATE_SERIAL_NUM),
// 	}
// }

func (self *RollingFileAppender) rotate() error {
	// close current log if we have one open
	if err := self.closeFileLocked(); err != nil {
		return err
	}
	self.curFileSize = 0

	// TODO: we should use the same timestamp for both renaming and stamping

	// rename old log
	err := self.renameLogFile(self.absPath)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(self.absPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return &OpenError{self.absPath, err}
	}
	self.file = file
	self.logHeader()

	// stamp start time
	if err := self.stampStartTime(); err != nil {
		return err
	}

	self.compressLogs()

	// WARN: we should do this in compression and not inline here
	self.removeMaxRotatedLogs()

	return nil
}

func (self *RollingFileAppender) rotationTimeSlice() (RotationTimeSlice, error) {
	candidateFilenames, err := filepath.Glob(self.absPath + ".*")
	if err != nil {
		return nil, err
	}

	rotationTimes := make(RotationTimeSlice, 0, len(candidateFilenames))
	for _, candidateFilename := range candidateFilenames {
		rotationTime, err := extractRotationTimeFromFilename(candidateFilename)
		if err == nil {
			rotationTimes = append(rotationTimes, rotationTime)
		}
	}

	return rotationTimes, nil
}

func (self *RollingFileAppender) loadState() error {
	state, err := readState(self.statePath())
	if err != nil {
		return err
	}

	self.state = state
	return nil
}

func (self *RollingFileAppender) stampStartTime() error {
	state := newState(time.Now())
	if err := state.write(self.statePath()); err != nil {
		return err
	}
	self.state = state
	return nil
}

// WARN: figure out what we want to do with this thing
/*
type writeSyncer struct {
	*os.File
	mu sync.Mutex
	// file          *os.File
	flushInterval time.Duration
	stopped       bool
	initialized   bool
	done          chan struct{}
	stop          chan struct{}
	ticker        *time.Ticker
}

func (w *writeSyncer) initialize() {
	if w.flushInterval == 0 {
		w.flushInterval = time.Second * 10
	}
	w.done = make(chan struct{})
	w.stop = make(chan struct{})
	w.ticker = time.NewTicker(w.flushInterval)
	w.initialized = true
	go func() {
		defer close(w.done)
		select {
		case <-w.ticker.C:
			_ = w.File.Sync()
		case <-w.stop:
			return
		}
	}()
}

func (w *writeSyncer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.initialized {
		w.initialize()
	}
	return w.File.Write(p)
}

func (w *writeSyncer) WriteString(s string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.initialized {
		w.initialize()
	}
	return w.File.WriteString(s)
}

func (w *writeSyncer) doClose() (stopped bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return false
	}

	if w.stopped {
		return true
	}
	w.stopped = true

	// Stop the flush loop and wait for it to exit.
	w.ticker.Stop()
	close(w.stop)
	<-w.done

	return false
}

func (w *writeSyncer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initialized && !w.stopped {
		// Stop the flush loop and wait for it to exit.
		w.ticker.Stop()
		close(w.stop)
		<-w.done
		w.stopped = true
	}
	if !w.stopped {
		close(w.stop)
		w.stopped = true
	}
	// if !w.doClose() {
	// 	w.file.Sync()
	// }
	return w.File.Close()
}
*/
