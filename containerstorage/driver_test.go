//go:build linux

package containerstorage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go.podman.io/storage/drivers/graphtest"
	"go.podman.io/storage/pkg/reexec"
)

func init() {
	reexec.Init()
}

// testOption returns the driver option with a unique S3 prefix per test run.
var testRunPrefix = fmt.Sprintf("test-%d", time.Now().UnixNano())

func option(t *testing.T) string {
	base := os.Getenv("LOOPHOLE_S3_URL")
	if base == "" {
		t.Skip("containerstorage tests require LOOPHOLE_S3_URL")
	}
	return "loophole.url=" + base + "/" + testRunPrefix
}

func TestLoopholeSetup(t *testing.T) {
	graphtest.GetDriverNoCleanup(t, "loophole", option(t))
}

func TestLoopholeCreateEmpty(t *testing.T) {
	graphtest.DriverTestCreateEmpty(t, "loophole", option(t))
}

func TestLoopholeCreateBase(t *testing.T) {
	graphtest.DriverTestCreateBase(t, "loophole", option(t))
}

func TestLoopholeCreateSnap(t *testing.T) {
	graphtest.DriverTestCreateSnap(t, "loophole", option(t))
}

func TestLoopholeCreateFromTemplate(t *testing.T) {
	graphtest.DriverTestCreateFromTemplate(t, "loophole", option(t))
}

func TestLoopholeDiffApply10Files(t *testing.T) {
	graphtest.DriverTestDiffApply(t, 10, "loophole", option(t))
}

func TestLoopholeChanges(t *testing.T) {
	graphtest.DriverTestChanges(t, "loophole", option(t))
}

func TestLoopholeEcho(t *testing.T) {
	graphtest.DriverTestEcho(t, "loophole", option(t))
}

func TestLoopholeListLayers(t *testing.T) {
	graphtest.DriverTestListLayers(t, "loophole", option(t))
}

func TestLoopholeTeardown(t *testing.T) {
	graphtest.PutDriver(t)
}
