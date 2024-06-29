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
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
)

// Default interval at which to log file is flushed to disk with [os.File.Sync].
const DefaultSyncInterval = time.Second

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

	// Held when rotating logs. This is a separate mutex since
	// rotation happens in its own goroutine and we don't want
	// to block the logger.
	rotateLock  sync.Mutex
	kick        chan struct{} // trigger rotation / compression
	stop        chan struct{} // close to stop rotate loop
	done        chan struct{} // closed after the rotate loop exits
	initialized bool          // rotate loop initialized
	closed      bool          // appender closed / rotate loop stopped

	// File syncing
	lastSync     atomic.Pointer[time.Time]
	syncInterval time.Duration

	// Log for internal rotation / compression errors since we
	// cannot report them to the caller since the op is async.
	errLog *log.Logger
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
	syncInterval         time.Duration
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
		syncInterval:         DefaultSyncInterval,
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

// WithSyncInterval sets the interval at which the log file is flushed to disk
// or disables syncing if d is less than or equal to zero. If not set
// [DefaultSyncInterval] is used.
//
// Syncing to disk is extremely expensive so be careful with this value.
// Disabling syncing is both reasonable and matches the behavior of most other
// log systems.
func (b *rollingFileAppenderBuilder) WithSyncInterval(d time.Duration) *rollingFileAppenderBuilder {
	b.syncInterval = d
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
		errLog:               b.errLog,
		kick:                 make(chan struct{}, 1),
		stop:                 make(chan struct{}),
		done:                 make(chan struct{}),
	}

	fileInfo, err := os.Stat(absPath)
	if err == nil && b.rotateIfExists { // err == nil means file exists
		return appender, appender.rotate(false)
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

		// WARN: remove this code
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
				if err = appender.stampStartTime(time.Now()); err != nil {
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
		if err2 := self.rotate(false); err2 != nil {
			if err == nil {
				err = err2
			} else {
				err = fmt.Errorf("rolling_file_appender: write: %w rotate: %w", err, err2)
			}
		}
	}
	return err
}

func (self *RollingFileAppender) closeFileLocked() error {
	if self.file == nil {
		return nil
	}
	err := self.file.Sync()
	if err != nil {
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
		return ErrAppenderClosed
	}
	self.closed = true
	self.rotateLogsAsync()
	close(self.stop)
	<-self.done
	return self.closeFileLocked()
}

func (self *RollingFileAppender) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.closeLocked()
}

func (self *RollingFileAppender) Flush() (err error) {
	// NB(charlie): This method is extremely hot and is typically called after
	// each log message is written. Previously, this method called Sync() each
	// time, which is both extremely expensive and often unnecessary.
	//
	// The only realistic case in which we'd lose log data is if there is a
	// power loss and the drive not having an onboard capacitor or battery
	// backup. In that event, we'd expect to see lost data (maybe a few log
	// lines) but data corruption would be rare since we write an append only
	// file on what is most likely a journaled file system.
	//
	// Regarding the performance impact, it is extreme and around 310x on a NVMe
	// drive (this equate to ~250 IOPS on a M1 mac ~3k on EXT4). Additionally,
	// this might put additional stress on the drive controller and have knock
	// on effects to other processes trying to write/read (like a database).
	//
	// As a compromise (since there will undoubtedly be resistance to this)
	// we now optionally sync the file periodically so that at worst only a
	// small amount of data will be lost in the event of a power loss or any
	// other freak failure.

	// Never sync the file.
	if self.syncInterval <= 0 {
		return nil
	}

	// Atomically load the last sync time and only sync if we're past the sync
	// threshold and can CAS the current time pointer with the next one - this
	// prevents concurrent calls to sync.
	t := self.lastSync.Load()
	if t == nil || time.Since(*t) >= self.syncInterval {
		// CAS to prevent concurrent calls to sync.
		now := time.Now()
		if self.lastSync.CompareAndSwap(t, &now) {
			// Hold lock since we need to synchronize around self.file
			self.lock.Lock()
			if self.file != nil {
				err = self.file.Sync()
			}
			self.lock.Unlock()
		}
	}
	if err != nil {
		return &SyncError{Filename: self.absPath, Err: err}
	}
	return nil
}

func (self *RollingFileAppender) Rotate() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.rotate(true)
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
	if err := self.closeLocked(); err != nil && err != ErrAppenderClosed {
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
	if err = self.stampStartTime(time.Now()); err != nil {
		return err
	}

	// remove really old logs
	self.removeMaxRotatedLogs()

	// Reset the close channels
	self.closed = false
	self.initialized = false
	self.stop = make(chan struct{})
	self.done = make(chan struct{})

	return nil
}

func rotatedFilename(baseFilename string, t time.Time, serial int) string {
	stamp := t.Format("2006-01-02T15-04-05")
	if serial > 0 {
		return baseFilename + "." + stamp + "-" + strconv.Itoa(serial)
	}
	return baseFilename + "." + stamp
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
	var errs []error
	for _, rotationTime := range rotationTimes[:numLogsToDelete] {
		if err := os.Remove(rotationTime.Filename); err != nil {
			// Ignore errors due to the file not existing which
			// can happen if there is a race removing it.
			if !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) != 0 {
		return &MinorRotationError{Err: errors.Join(errs...)}
	}
	return nil
}

func (self *RollingFileAppender) compressMaxUncompressedLogs() error {
	if !self.compressRotatedLogs || self.maxUncompressedLogs < 0 {
		return nil
	}

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
	// coreutils team.
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

	fo, err := os.OpenFile(name+".gz", os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		if os.IsExist(err) {
			return nil // Race: someone else started compressing this before us
		}
		return err
	}
	defer fo.Close()

	// Close and remove gzip file on error.
	exit := func(err error) error {
		fo.Close()
		os.Remove(fo.Name()) // remove compressed log file on error
		// TODO: wrap error here or let the caller handle it?
		return err
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
	_ = fi.Close() // ignore close error since we only read the file

	// TODO(charlie): This appears to be a "nice to have" and was
	// copied from the original implementation. An error here is
	// not fatal, but we should log it (once we decide on how to
	// log internal errors).
	_ = os.Chtimes(fo.Name(), time.Now(), info.ModTime())

	return os.Remove(name)
}

func (self *RollingFileAppender) rotateAndCompress() error {
	self.rotateLock.Lock()
	defer self.rotateLock.Unlock()

	var err1 error
	if self.compressRotatedLogs && self.maxUncompressedLogs > 0 {
		if err1 = self.compressMaxUncompressedLogs(); err1 != nil {
			return err1
		}
	}
	err2 := self.removeMaxRotatedLogs()
	if err2 != nil && err1 == nil {
		err1 = err2
	}
	return err1
}

func (self *RollingFileAppender) rotateLoop() {
	defer close(self.done)
	for {
		select {
		case <-self.kick:
			if err := self.rotateAndCompress(); err != nil {
				self.errorf("rolling_file_appender: rotate and compress: %v", err)
			}
		case <-self.stop:
			// Check if there is a request to rotate logs before exiting
			select {
			case <-self.kick:
				if err := self.rotateAndCompress(); err != nil {
					self.errorf("rolling_file_appender: rotate and compress: %v", err)
				}
			default:
			}
			return
		}
	}
}

func (self *RollingFileAppender) rotateLogsAsync() {
	// NOTE: lock much be held
	if !self.initialized {
		self.initialized = true
		go self.rotateLoop()
	}
	select {
	case self.kick <- struct{}{}:
	default:
	}
}

func renameLogFile(oldFilename string, now time.Time) error {
	newFilename, err := nextLogFileName(oldFilename, time.Now())
	if err != nil {
		return err
	}
	if err := os.Rename(oldFilename, newFilename); err != nil {
		return err
	}
	return nil
}

func createFileExclusive(name string) (created bool) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

func nextLogFileName(oldFilename string, now time.Time) (newFilename string, _ error) {
	// NOTE: This function creates newFilename since that is the only way to
	// guarantee that it does not exist. This is okay, since we re-open and
	// truncate it.

	// Fast path if we don't have conflicting filenames.
	if name := rotatedFilename(oldFilename, now, 0); createFileExclusive(name) {
		return name, nil
	}

	// A file exists with the name we want. To handle this conflict we append
	// an numeric serial/ordinal suffix to the name (e.g. "foo-1", "foo-2").
	//
	// Perform a binary search to find the next file name.
	//
	// Small values are *much* more common and the larger the value the more
	// stat(2) calls the binary search needs to make (which are slow) - so we
	// front-load a few small values then quickly ramp up to large values since
	// the difference in runtime between 1024 and 2^30 is relatively small.
	//
	// NB: The larger values are really only for completeness and to handle the
	// pathological case of something essentially DOS'ing us by creating these
	// files or a badly configured program using thousands of appenders with
	// extremely small rotation values.
	for _, n := range []int{4, 8, 16, 128, 1024, 1024 * 1024 * 1024} {
		for {
			var err error
			i := sort.Search(n, func(m int) bool {
				_, err = os.Lstat(rotatedFilename(oldFilename, now, m))
				return err != nil && os.IsNotExist(err)
			})
			if err != nil && !os.IsNotExist(err) {
				return "", err
			}
			if i == n {
				break // search exhausted - continue outer loop
			}
			// Exclusively create the file to ensure it
			// does not exist and prevent TOCTOU bugs.
			newFilename = rotatedFilename(oldFilename, now, i)
			if createFileExclusive(newFilename) {
				return newFilename, nil
			}
		}
	}

	err := &RenameError{
		OldFilename: oldFilename,
		NewFilename: newFilename,
		Err:         fmt.Errorf("Reached max serial number: %d", 1024*1024*1024),
	}
	return "", err
}

func (self *RollingFileAppender) rotate(force bool) error {
	// close current log if we have one open
	if err := self.closeFileLocked(); err != nil {
		return err
	}
	self.curFileSize = 0

	// rename old log
	now := time.Now()
	err := renameLogFile(self.absPath, now)
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
	if err := self.stampStartTime(now); err != nil {
		return err
	}

	// Force rotation and possibly compression (used by Rotate())
	if force {
		return self.rotateAndCompress()
	}
	self.rotateLogsAsync() // Async compression
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

func (self *RollingFileAppender) stampStartTime(now time.Time) error {
	state := newState(now)
	if err := state.write(self.statePath()); err != nil {
		return err
	}
	self.state = state
	return nil
}
