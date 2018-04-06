package store

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

//Store the interface describes what a methods a backing store
// should implement in order to be accepted by the database engine
type Store interface {
	Create() error
	WriteAt(b []byte, off int) (int, error)
	ReadAt(b []byte, off int) (int, error)
	Sync(off int, n int) error
	Close() error
}

const (
	//FileSizeDb is the default size for each db file ~68Gb
	FileSizeDb = 4096 * 4096 * 4096

	//FileSizeIdx is the default size for each db file 4096 bytes
	FileSizeIdx = 4096
)

const (
	// MAPPED backend
	MAPPED = iota
	// NORMAL file backed
	NORMAL
)

// Store errors types
var (
	ErrZeroSlice = fmt.Errorf("Byte slice size must be more than 0")
	ErrNoData    = fmt.Errorf("Offset must be within valid data region")
	ErrSizeLimit = fmt.Errorf("Store max size limit of 1 tera reached")
)

// FileBackend is the dt responsible for backing the skiplist on disk
type FileBackend struct {
	files            []*os.File
	fname            string
	current          int
	fileStoreMaxsize int
}

// New instanciate a new store based on name size and flags and returns
// the Store interface
func New(name string, size uint, flag int) Store {
	var (
		fstore *FileBackend
		mstore *MappedBackend
	)

	if size == 0 {
		size = FileSizeIdx
	}

	if name == "" {
		name = "index."
	}

	if flag != MAPPED {
		fstore = &FileBackend{
			fname:            name,
			fileStoreMaxsize: int(size * 16),
		}
		return fstore
	}

	mstore = &MappedBackend{
		fstore: fstore,
		mstore: make([][]byte, 0),
	}

	return mstore
}

//Create new FileStore backing
func (s *FileBackend) Create() error {
	return s.resize(0)
}

//WriteAt write at said location
func (s *FileBackend) WriteAt(b []byte, off int) (int, error) {
	if err := s.resize(len(b)); err != nil {
		return -1, err
	}

	if off+len(b) > s.fileStoreMaxsize {
		return -1, ErrSizeLimit
	}

	if off >= s.current {
		s.current += len(b)
	}

	return s.files[len(s.files)-1].WriteAt(b, int64(off))
}

//ReadAt write at said location
func (s *FileBackend) ReadAt(b []byte, off int) (int, error) {
	return s.files[len(s.files)-1].ReadAt(b, int64(off))
}

func (s *FileBackend) resize(size int) error {
	if size+s.current > FileSizeDb || size == 0 {
		fname := fmt.Sprintf("%v%v", s.fname, len(s.files))
		file, err := os.Create(fname)
		if err != nil {
			return err
		}

		if err := file.Truncate(FileSizeDb); err != nil {
			return err
		}

		s.files = append(s.files, file)
	}

	return nil
}

// Sync either sync the everything of calls sync file range with the
// specified off and n number of bytes ( sync only the pages the need
// to be synched
func (s *FileBackend) Sync(off int, n int) error {
	if off == 0 && n == 0 {
		syscall.Sync()
		return nil
	}

	err := syscall.SyncFileRange(
		int(s.files[len(s.files)-1].Fd()),
		int64(off),
		int64(n),
		0,
	)

	return err
}

// Close the FileStore and syncs
func (s *FileBackend) Close() error {
	for i := range s.files {
		if err := s.files[i].Close(); err != nil {
			return err
		}
	}

	return s.Sync(0, 0)
}

// MappedBackend is a memory mapped store that only maps for writes
type MappedBackend struct {
	fstore *FileBackend
	mstore [][]byte
}

//Create a new mapped store
func (m *MappedBackend) Create() error {
	if err := m.fstore.Create(); err != nil {
		return err
	}

	prot := syscall.PROT_WRITE | syscall.PROT_READ
	flag := syscall.MAP_SHARED
	fd := m.fstore.files[len(m.fstore.files)-1].Fd()
	buf, err := syscall.Mmap(int(fd), 0, int(FileSizeDb), prot, flag)
	if err != nil {
		return err
	}
	m.mstore = append(m.mstore, buf)

	return nil
}

//WriteAt write at said location
func (m *MappedBackend) WriteAt(b []byte, off int) (int, error) {
	if err := m.fstore.resize(len(b)); err != nil {
		return -1, err
	}

	if len(b) == 0 {
		return -1, ErrZeroSlice

	}

	if off+len(b) > m.fstore.fileStoreMaxsize {
		return -1, ErrSizeLimit
	}

	if off >= m.fstore.current {
		m.fstore.current += len(b)
	}

	for i, j := off, 0; i < off+len(b) && j < len(b); i, j = i+1, j+1 {
		m.mstore[len(m.mstore)-1][i] = b[j]
	}

	return len(b), nil
}

//ReadAt write at said location
func (m *MappedBackend) ReadAt(b []byte, off int) (int, error) {
	if len(b) == 0 {
		return -1, ErrZeroSlice

	}

	if off+len(b)-1 > m.fstore.current {
		return -1, ErrNoData
	}

	for i, j := off, 0; i < off+len(b) && j < len(b); i, j = i+1, j+1 {
		b[j] = m.mstore[len(m.mstore)-1][i]

	}

	return len(b), nil
}

// Sync syncs the underline mapped storage or a region of it if anything
// other than zero is specified to it
func (m *MappedBackend) Sync(off int, n int) error {
	var (
		_p    unsafe.Pointer
		_zero uintptr
		err   error
	)

	if len(m.mstore[len(m.mstore)-1][off:off+n]) > 0 {
		_p = unsafe.Pointer(&m.mstore[len(m.mstore)-1][0])
	} else {
		_p = unsafe.Pointer(&_zero)
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(_p),
		uintptr(len(m.mstore[len(m.mstore)-1][off:off+n])),
		uintptr(syscall.MS_SYNC),
	)

	switch e {
	case syscall.EAGAIN:
		var EAGAIN error = syscall.EAGAIN
		err = EAGAIN
	case syscall.EINVAL:
		var EINVAL error = syscall.EINVAL
		err = EINVAL
	case syscall.ENOENT:
		var ENOENT error = syscall.ENONET
		err = ENOENT
	}

	return err

}

// Close the FileStore call to Munmap should also take care of syncying to disk
func (m *MappedBackend) Close() error {
	for i := range m.fstore.files {
		if err := syscall.Munmap(m.mstore[i]); err != nil {
			return err
		}
		m.mstore[i] = nil

		if err := m.fstore.files[i].Close(); err != nil {
			return err
		}
	}

	return nil
}
