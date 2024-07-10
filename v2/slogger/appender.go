// Copyright 2013 - 2015 MongoDB, Inc.
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

package slogger

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"
)

type Appender interface {
	Append(log *Log) error
	Flush() error
}

// TODO: consider renaming to formatFuncContainer
//
// formatVal is a container that allows use to atomically store/access
// format functions.
type formatVal struct {
	format func(log *Log) string
}

var formatFn unsafe.Pointer = unsafe.Pointer(&formatVal{format: FormatLog})

func GetFormatLogFunc() func(log *Log) string {
	return (*formatVal)(atomic.LoadPointer(&formatFn)).format
}

func SetFormatLogFunc(f func(log *Log) string) {
	if f == nil {
		panic("slogger: nil format log function")
	}
	atomic.StorePointer(&formatFn, unsafe.Pointer(&formatVal{format: f}))
}

// TODO: consider passing just the time layout
func formatLog(log *Log, timePart string) string {
	var buf [20]byte
	typ := log.Level.Type()
	msg := log.Message()
	var w strings.Builder
	// 50 is roughly the minimum size of a log with an error code but we pad
	// it to 64 to have a bit of headroom in the buffer
	w.Grow(64 + len(timePart) + len(log.Prefix) + len(typ) +
		len(log.Filename) + len(log.FuncName) + len(msg))
	w.WriteString(timePart)
	w.WriteString(" [")
	w.WriteString(log.Prefix)
	w.WriteByte('.')
	w.WriteString(typ)
	w.WriteString("] [")
	w.WriteString(log.Filename)
	w.WriteByte(':')
	w.WriteString(log.FuncName)
	w.WriteByte(':')
	w.Write(strconv.AppendInt(buf[:0], int64(log.Line), 10))
	w.WriteString("] ")
	if log.ErrorCode != NoErrorCode {
		w.WriteByte('[')
		w.Write(strconv.AppendInt(buf[:0], int64(log.ErrorCode), 10))
		w.WriteString("] ")
	}
	w.WriteString(msg)
	w.WriteByte('\n')
	return w.String()
}

func formatLogX(log *Log, timeLayout string) string {
	typ := log.Level.Type()
	msg := log.Message()
	// 50 is roughly the minimum size of a log with an error code but we pad
	// it to 64 to have a bit of headroom in the buffer
	b := make([]byte, 0, 64+len(timeLayout)+len(log.Prefix)+len(typ)+
		len(log.Filename)+len(log.FuncName)+len(msg))
	b = log.Timestamp.AppendFormat(b, timeLayout)
	b = append(b, " ["...)
	b = append(b, log.Prefix...)
	b = append(b, '.')
	b = append(b, typ...)
	b = append(b, "] ["...)
	b = append(b, log.Filename...)
	b = append(b, ':')
	b = append(b, log.FuncName...)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(log.Line), 10)
	b = append(b, "] "...)
	if log.ErrorCode != NoErrorCode {
		b = append(b, '[')
		b = strconv.AppendInt(b, int64(log.ErrorCode), 10)
		b = append(b, "] "...)
	}
	b = append(b, msg...)
	b = append(b, '\n')
	return unsafe.String(&b[0], len(b))
}

// TODO: fix
// WARN: the agent uses this
func FormatLogWithTimezone(log *Log) string {
	return formatLog(log, log.Timestamp.Format("[2006-01-02T15:04:05.000-0700]"))
}

func FormatLog(log *Log) string {
	return formatLog(log, log.Timestamp.Format("[2006/01/02 15:04:05.000]"))
}

type StringWriter interface {
	WriteString(s string) (ret int, err error)
	Sync() error
}

type FileAppender struct {
	StringWriter
}

func (self FileAppender) Append(log *Log) error {
	f := GetFormatLogFunc()
	_, err := self.WriteString(f(log))
	return err
}

func (self FileAppender) Flush() error {
	return self.Sync()
}

func StdOutAppender() *FileAppender {
	return &FileAppender{os.Stdout}
}

func StdErrAppender() *FileAppender {
	return &FileAppender{os.Stderr}
}

// TODO: don't use a file here - find a way to bybass
func DevNullAppender() (*FileAppender, error) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}

	return &FileAppender{devNull}, nil
}

type StringAppender struct {
	*bytes.Buffer
}

func NewStringAppender(buffer *bytes.Buffer) *StringAppender {
	return &StringAppender{buffer}
}

func (self StringAppender) Append(log *Log) error {
	f := GetFormatLogFunc()
	_, err := self.WriteString(f(log))
	return err
}

func (self StringAppender) Flush() error {
	return nil
}

// Return true if the log should be passed to the underlying
// `Appender`
type Filter func(log *Log) bool

type FilterAppender struct {
	Appender Appender
	Filter   Filter
}

func (self *FilterAppender) Append(log *Log) error {
	if self.Filter(log) == false {
		return nil
	}

	return self.Appender.Append(log)
}

func (self *FilterAppender) Flush() error {
	return self.Appender.Flush()
}

func LevelFilter(threshold Level, appender Appender) *FilterAppender {
	filterFunc := func(log *Log) bool {
		return log.Level >= threshold
	}

	return &FilterAppender{
		Appender: appender,
		Filter:   filterFunc,
	}
}
