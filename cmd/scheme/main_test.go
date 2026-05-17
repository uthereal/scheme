package main

import (
  "testing"

  "github.com/stretchr/testify/assert"
)

func TestMainCmd(t *testing.T) {
  t.Run("no subcommand prints help and returns 1", func(t *testing.T) {
    code := run([]string{"scheme"})
    assert.Equal(t, 1, code, "Expected exit code 1.")
  })

  t.Run("help subcommand prints help and returns 0", func(t *testing.T) {
    code := run([]string{"scheme", "help"})
    assert.Equal(t, 0, code, "Expected exit code 0.")
  })

  t.Run("unknown subcommand returns 1", func(t *testing.T) {
    code := run([]string{"scheme", "unknown_cmd"})
    assert.Equal(t, 1, code, "Expected exit code 1.")
  })

  t.Run("valid subcommand runs successfully", func(t *testing.T) {
    code := run([]string{"scheme", "eject", "-h"})
    assert.Equal(t, 0, code, "Expected exit code 0 from subcommand.")
  })

  t.Run("valid subcommand gen runs successfully", func(t *testing.T) {
    code := run([]string{"scheme", "gen", "-h"})
    assert.Equal(t, 0, code, "Expected exit code 0 from subcommand.")
  })

  t.Run("valid subcommand migrate runs successfully", func(t *testing.T) {
    code := run([]string{"scheme", "migrate", "-h"})
    assert.Equal(t, 0, code, "Expected exit code 0 from subcommand.")
  })
}
