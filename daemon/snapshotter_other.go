//go:build !linux

package daemon

func (d *Daemon) startSnapshotter(sockPath string) error {
	return nil
}

func (d *Daemon) stopSnapshotter() {}
