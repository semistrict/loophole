//go:build linux

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/semistrict/loophole/env"
)

func run(args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "shell":
			return runShell(args[1:])
		case "internal.init":
			return runInternalInit()
		case "internal.ready":
			return runInternalReady()
		case "internal.exec":
			return runInternalExec(args[1:])
		}
	}

	fs := flag.NewFlagSet("loophole-sandboxd", flag.ContinueOnError)
	var profile string
	var socketPath string
	var loopholeBin string
	var runscBin string
	fs.StringVar(&profile, "p", "", "loophole profile")
	fs.StringVar(&profile, "profile", "", "loophole profile")
	fs.StringVar(&socketPath, "socket-path", "", "sandboxd socket path")
	fs.StringVar(&loopholeBin, "loophole-bin", "", "path to loophole binary")
	fs.StringVar(&runscBin, "runsc-bin", "", "path to runsc binary")
	if err := fs.Parse(args); err != nil {
		return err
	}

	dir := env.DefaultDir()
	cfg, err := env.LoadConfig(dir)
	if err != nil {
		return err
	}
	inst, err := cfg.Resolve(profile)
	if err != nil {
		return err
	}

	d, err := startDaemon(context.Background(), inst, dir, daemonOptions{
		SocketPath:  socketPath,
		LoopholeBin: loopholeBin,
		RunscBin:    runscBin,
	})
	if err != nil {
		return err
	}
	return d.serve(context.Background())
}

func runInternalInit() error {
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGCHLD)
	defer signal.Stop(signals)

	for {
		sig := <-signals
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			return nil
		case syscall.SIGCHLD:
			for {
				var status syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil {
					break
				}
			}
		}
	}
}

func runInternalExec(args []string) error {
	fs := flag.NewFlagSet("internal.exec", flag.ContinueOnError)
	var cwd string
	var envJSON string
	fs.StringVar(&cwd, "cwd", "", "working directory")
	fs.StringVar(&envJSON, "env-json", "", "base64-encoded env json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cmdArgs := fs.Args()
	if len(cmdArgs) > 0 && cmdArgs[0] == "--" {
		cmdArgs = cmdArgs[1:]
	}
	if len(cmdArgs) == 0 {
		return fmt.Errorf("command is required")
	}
	if cwd != "" {
		if err := os.Chdir(cwd); err != nil {
			return err
		}
	}
	env := os.Environ()
	if envJSON != "" {
		data, err := base64.StdEncoding.DecodeString(envJSON)
		if err != nil {
			return err
		}
		var vars map[string]string
		if err := json.Unmarshal(data, &vars); err != nil {
			return err
		}
		env = mergeEnv(env, vars)
	}
	for range 100 {
		path, err := exec.LookPath(cmdArgs[0])
		if err != nil {
			if isTransientExecError(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
		if err := syscall.Exec(path, cmdArgs, env); err != nil {
			if isTransientExecError(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
	}
	return fmt.Errorf("timed out waiting for executable %q to become ready", cmdArgs[0])
}

func runInternalReady() error {
	for _, path := range []string{"/", "/.loophole/bin/loophole-sandboxd", "/bin"} {
		if _, err := os.Stat(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func mergeEnv(base []string, overrides map[string]string) []string {
	m := make(map[string]string, len(base)+len(overrides))
	for _, kv := range base {
		key, value, ok := strings.Cut(kv, "=")
		if ok {
			m[key] = value
		}
	}
	for k, v := range overrides {
		m[k] = v
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

func isTransientExecError(err error) bool {
	return errors.Is(err, syscall.EIO) || strings.Contains(strings.ToLower(err.Error()), "input/output error")
}
