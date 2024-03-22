package slogger

import "testing"

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
