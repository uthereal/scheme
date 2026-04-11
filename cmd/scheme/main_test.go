package main

import (
	"testing"
)

func TestMainCmd(t *testing.T) {
	t.Run("no subcommand prints help and returns 1", func(t *testing.T) {
		code := run([]string{"scheme"})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("help subcommand prints help and returns 0", func(t *testing.T) {
		code := run([]string{"scheme", "help"})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}
	})

	t.Run("unknown subcommand returns 1", func(t *testing.T) {
		code := run([]string{"scheme", "unknown_cmd"})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("valid subcommand runs successfully", func(t *testing.T) {
		code := run([]string{"scheme", "eject", "-h"})
		if code != 0 {
			t.Errorf("expected exit code 0 from subcommand, got %d", code)
		}
	})

	t.Run("valid subcommand gen runs successfully", func(t *testing.T) {
		code := run([]string{"scheme", "gen", "-h"})
		if code != 0 {
			t.Errorf("expected exit code 0 from subcommand, got %d", code)
		}
	})

	t.Run("valid subcommand migrate runs successfully", func(t *testing.T) {
		code := run([]string{"scheme", "migrate", "-h"})
		if code != 0 {
			t.Errorf("expected exit code 0 from subcommand, got %d", code)
		}
	})
}
