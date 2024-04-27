package qf

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/edsrzf/mmap-go"
)

var _ Vector = (*mmapVector)(nil)

type mmapVector struct {
	f  *os.File
	m  mmap.MMap
	fn string

	Vector
}

func NewMmapVector(dir string, bitPacked bool) VectorAllocateFn {
	counter := 0 // for unique file names
	return func(bits uint, size uint64) Vector {
		if bits > bitsPerWord {
			panic(fmt.Sprintf("bit size of %d is greater than word size of %d, not supported",
				bits, bitsPerWord))
		}

		fn := path.Join(dir, fmt.Sprintf("mmap.%d", counter))
		f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		words := size
		if bitPacked {
			words = wordsRequired(bits, size)
		}
		if err := f.Truncate(int64(words * bytesPerWord)); err != nil {
			panic(err)
		}

		m, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			panic(err)
		}
		asUint64 := unsafeBytesToUint64Slice(m)

		var v Vector
		if bitPacked {
			v = &packed{genForbiddenMask(bits), bits, asUint64, size}
		} else {
			v = (*unpacked)(&asUint64)
		}

		counter++
		return &mmapVector{
			f:      f,
			m:      m,
			fn:     fn,
			Vector: v,
		}
	}
}

func (m *mmapVector) Close() error {
	if err := m.m.Unmap(); err != nil {
		return err
	}
	return m.f.Close()
}

func (m *mmapVector) WriteTo(w io.Writer) (n int64, err error) {
	panic("not implemented")
}

func (m *mmapVector) ReadFrom(r io.Reader) (n int64, err error) {
	panic("not implemented")
}
