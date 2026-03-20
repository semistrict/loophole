package cachedserver_test

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/semistrict/loophole/cached"
	"github.com/semistrict/loophole/cached/cachedserver"
	"github.com/semistrict/loophole/internal/safepoint"
)

func TestInProcessRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cachedserver.InitInMemory(64)
		ln := cachedserver.NewPipeListener()
		if err := cachedserver.StartServerWithListener("", ln); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ln.Close()
			cachedserver.Shutdown()
		}()

		sp := safepoint.New()
		client := ln.Dial(sp)
		defer client.Close()

		page := bytes.Repeat([]byte{0xAB}, cached.SlotSize)
		client.PutPage("layer1", 1, page)

		got := client.GetPage("layer1", 1)
		if !bytes.Equal(got, page) {
			t.Fatalf("got %x, want %x", got[:4], page[:4])
		}

		client.InvalidatePage("layer1", 1)
		if got := client.GetPage("layer1", 1); got != nil {
			t.Fatal("expected nil after delete")
		}
	})
}

func TestInProcessGetPageRef(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cachedserver.InitInMemory(64)
		ln := cachedserver.NewPipeListener()
		if err := cachedserver.StartServerWithListener("", ln); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ln.Close()
			cachedserver.Shutdown()
		}()

		sp := safepoint.New()
		client := ln.Dial(sp)
		defer client.Close()

		page := bytes.Repeat([]byte{0xCD}, cached.SlotSize)
		client.PutPage("ref", 1, page)

		g := sp.Enter()
		data := client.GetPageRef(g, "ref", 1)
		if data == nil {
			g.Exit()
			t.Fatal("expected hit")
		}
		if !bytes.Equal(data, page) {
			g.Exit()
			t.Fatal("data mismatch")
		}
		g.Exit()
	})
}

func TestInProcessMultiClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cachedserver.InitInMemory(64)
		ln := cachedserver.NewPipeListener()
		if err := cachedserver.StartServerWithListener("", ln); err != nil {
			t.Fatal(err)
		}
		defer func() {
			ln.Close()
			cachedserver.Shutdown()
		}()

		sp := safepoint.New()
		c1 := ln.Dial(sp)
		defer c1.Close()
		c2 := ln.Dial(sp)
		defer c2.Close()

		page := bytes.Repeat([]byte{0xEE}, cached.SlotSize)
		c1.PutPage("shared", 1, page)

		got := c2.GetPage("shared", 1)
		if !bytes.Equal(got, page) {
			t.Fatal("client 2 should see client 1's page")
		}
	})
}
