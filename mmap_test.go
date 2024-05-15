package qf

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/edsrzf/mmap-go"
)

func TestMMap(t *testing.T) {
	dir := t.TempDir()
	fn := path.Join(dir, "notes.txt")
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(100); err != nil {
		t.Fatal(err)
	}
	// The file must be closed, even after calling Unmap.
	defer f.Close()

	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	m[90] = 'a'

	// m acts as a writable slice of bytes that is a view into the open file, notes.txt.
	// It is sized to the file contents automatically.
	fmt.Println(string(m))

	// The Unmap method should be called when finished with it to avoid leaking memory
	// and to ensure that writes are flushed to disk.
	if err := m.Unmap(); err != nil {
		t.Fatal(err)
	}

	// Hello, world
}
