package qf

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var _ Vector = (*mmapVector)(nil)

type MmapConfig struct {
	Dir       string
	BitPacked bool
}

type mmapMeta struct {
	Bits      uint32
	FnCounter uint32
}

type mmapVector struct {
	f *os.File
	m mmap.MMap

	mmapMeta
	MmapConfig
	Vector
}

func mkFn(dir string, counter uint32) string {
	return path.Join(dir, fmt.Sprintf("mmap.%d", counter))
}

func NewMmapVector(c MmapConfig) VectorAllocateFn {
	return func(bits uint, size uint64) Vector {
		if bits > bitsPerWord {
			panic(fmt.Sprintf("bit size of %d is greater than word size of %d, not supported",
				bits, bitsPerWord))
		}

		var (
			fn        string
			fnCounter uint32
		)
		for {
			fn = mkFn(c.Dir, fnCounter)
			if _, err := os.Stat(fn); os.IsNotExist(err) {
				break
			}
			fnCounter++
		}
		f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		words := size
		if c.BitPacked {
			words = wordsRequired(bits, size)
		}
		if err := f.Truncate(int64(words * bytesPerWord)); err != nil {
			panic(err)
		}

		var (
			v Vector
			m mmap.MMap
		)
		if size == 0 {
			v = (*unpacked)(nil)
		} else {
			m, err = mmap.Map(f, mmap.RDWR, 0)
			if err != nil {
				panic(err)
			}
			asUint64 := unsafeBytesToUint64Slice(m)
			if c.BitPacked {
				v = &packed{genForbiddenMask(bits), bits, asUint64, size}
			} else {
				v = (*unpacked)(&asUint64)
			}
		}

		return &mmapVector{
			f: f,
			m: m,
			mmapMeta: mmapMeta{
				Bits:      uint32(bits),
				FnCounter: fnCounter,
			},
			MmapConfig: c,
			Vector:     v,
		}
	}
}

func (m *mmapVector) Close() error {
	if m.m != nil {
		if err := m.m.Unmap(); err != nil {
			return err
		}
		m.m = nil
	}
	if err := m.f.Close(); err != nil {
		return err
	}
	m.f = nil
	return nil
}

func (m *mmapVector) WriteTo(w io.Writer) (n int64, err error) {
	if err = m.m.Flush(); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, m.mmapMeta); err != nil {
		return
	}
	// is this correct?
	n += int64(unsafe.Sizeof(m.mmapMeta))
	return
}

func (m *mmapVector) ReadFrom(r io.Reader) (n int64, err error) {
	if err = m.Close(); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &m.mmapMeta); err != nil {
		return
	}
	n += int64(unsafe.Sizeof(m.mmapMeta))

	m.f, err = os.OpenFile(mkFn(m.Dir, m.FnCounter), os.O_RDWR, 0644)
	if err != nil {
		return
	}
	info, err := m.f.Stat()
	if err != nil {
		return
	}
	m.m, err = mmap.Map(m.f, mmap.RDWR, 0)
	if err != nil {
		return
	}
	asUint64 := unsafeBytesToUint64Slice(m.m)

	if m.BitPacked {
		size := uint64(info.Size() / bytesPerWord)
		bits := uint(m.Bits)
		m.Vector = &packed{genForbiddenMask(bits), bits, asUint64, size}
	} else {
		m.Vector = (*unpacked)(&asUint64)
	}
	return
}
