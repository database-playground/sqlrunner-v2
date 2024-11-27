package sqlrunner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanner(t *testing.T) {
	t.Parallel()

	t.Run("int64", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(int64(42)))
		assert.Equal(t, "42", s.Value())
	})

	t.Run("float64 expless", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(float64(42.42)))
		assert.Equal(t, "42.42", s.Value())
	})

	t.Run("float64 exp", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(float64(42.42424242424242e8)))
		assert.Equal(t, "4242424242.424242", s.Value())
	})

	t.Run("bool true", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(true))
		assert.Equal(t, "1", s.Value())
	})

	t.Run("bool false", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(false))
		assert.Equal(t, "0", s.Value())
	})

	t.Run("[]byte", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan([]byte("hello")))
		assert.Equal(t, "68656c6c6f", s.Value())
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan("hello"))
		assert.Equal(t, "hello", s.Value())
	})

	t.Run("time.Time", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(time.Date(2021, 1, 2, 3, 4, 5, 6, time.UTC)))
		assert.Equal(t, "2021-01-02 03:04:05", s.Value())
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		s := &StringScanner{}
		require.NoError(t, s.Scan(nil))
		assert.Equal(t, "NULL", s.Value())
	})
}
