package store

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Store the interface describes what a methods a backing store
// should implement in order to be accepted by the database engine
type Store interface {
	Open() error
	WriteAt(b []byte, off int) (int, error)
	ReadAt(b []byte, off int) (int, error)
	Sync(off int, n int) error
	Close() error
}

const (
	//FileSizeDb is the default size for each db file ~68Gb
	FileSizeDb = 4096 * 4096 * 4096

	//FileSizeIdx is the default size for each db file ~16Mb bytes
	FileSizeIdx = 4096 * 4096

	// FileSizeTx is the default transaction size of 4Kb
	FileSizeTx = 4096

	// FileSizeDefault is the default file size is case not specified otherwise
	FileSizeDefault = 4096 * 512
)

const (
	// MAPPED backend
	MAPPED = iota
	// NORMAL file backed
	NORMAL
)

// Store errors types
var (
	ErrZeroSlice     = fmt.Errorf("Byte slice size must be more than 0")
	ErrReadTooShort  = fmt.Errorf("Could not copy the requested bytes")
	ErrWriteTooShort = fmt.Errorf("Could not write the requested bytes")
	ErrNoData        = fmt.Errorf("Offset must be within valid data region")
	ErrSizeLimit     = fmt.Errorf("Store max size limit of 1 tera reached")
)

// Conf is a configuration struct to be given when a new store is
// initialized
type Conf struct {
	Name string
	Size int
	Mode int // Mode decides whether the store is mem mapped store
}

// New instanciate a new store based on name size and flags and returns
// the Store interface
func New(config *Conf) Store {
	if config.Size == 0 {
		config.Size = FileSizeDefault
	}

	fstore := &FileBackend{
		name:    config.Name,
		size:    config.Size,
		maxSize: config.Size * 16,
	}

	switch config.Mode {
	case MAPPED:
		return &MappedBackend{
			fstore: fstore,
			mstore: make([]byte, 0),
		}
	}

	return fstore
}

// FileBackend is the dt responsible for backing the skiplist on disk
type FileBackend struct {
	file    *os.File
	name    string
	size    int
	currPos int
	maxSize int
}

//Open new FileStore backing
func (s *FileBackend) Open() (err error) {
	_, err = os.Stat(s.name)
	if err != nil {
		s.file, err = os.OpenFile(
			s.name,
			os.O_RDWR|os.O_CREATE|os.O_TRUNC,
			0666,
		)
		err = s.Resize(s.size)
	} else {
		s.file, err = os.OpenFile(
			s.name,
			os.O_RDWR|os.O_TRUNC,
			0666,
		)
	}

	return err
}

//WriteAt write at said location
func (s *FileBackend) WriteAt(b []byte, off int) (int, error) {
	if err := s.Resize(len(b)); err != nil {
		return -1, err
	}

	if off+len(b) > s.maxSize {
		return -1, ErrSizeLimit
	}

	if off >= s.currPos {
		s.currPos += len(b)
	}

	return s.file.WriteAt(b, int64(off))
}

//ReadAt write at said location
func (s *FileBackend) ReadAt(b []byte, off int) (int, error) {
	return s.file.ReadAt(b, int64(off))
}

// Resize evaluates the current resize and double the current size to a multiple
// of filestore size
func (s *FileBackend) Resize(size int) error {
	if s.currPos+size > s.size {
		size += s.currPos
		size /= s.size
		size *= 2
		size *= s.size

		return s.file.Truncate(int64(size))
	} else if s.currPos+size == s.size && s.currPos == 0 {
		return s.file.Truncate(int64(size))
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
		int(s.file.Fd()),
		int64(off),
		int64(n),
		0,
	)

	return err
}

// Close the FileStore and syncs
func (s *FileBackend) Close() error {
	if err := s.file.Close(); err != nil {
		return err
	}

	return s.Sync(0, 0)
}

// MappedBackend is a memory mapped store that only maps for writes
type MappedBackend struct {
	fstore *FileBackend
	mstore []byte
}

//Open a new mapped store
func (m *MappedBackend) Open() (err error) {
	if err = m.fstore.Open(); err != nil {
		return err
	}

	m.mstore, err = syscall.Mmap(
		int(m.fstore.file.Fd()),
		0,
		m.fstore.size,
		syscall.PROT_WRITE|syscall.PROT_READ,
		syscall.MAP_SHARED,
	)

	return err
}

//WriteAt write at said location
func (m *MappedBackend) WriteAt(b []byte, off int) (int, error) {
	if err := m.fstore.Resize(len(b)); err != nil {
		return -1, err
	}

	if len(b) == 0 {
		return -1, ErrZeroSlice

	}

	if off+len(b) > m.fstore.maxSize {
		return -1, ErrSizeLimit
	}

	if off >= m.fstore.currPos {
		m.fstore.currPos += len(b)
	}

	n := copy(m.mstore[off:], b)
	if len(b) != n {
		return n, ErrWriteTooShort
	}

	//for i, j := off, 0; i < off+len(b) && j < len(b); i, j = i+1, j+1 {
	//		m.mstore[i] = b[j]
	//}

	return n, nil
}

//ReadAt write at said location
func (m *MappedBackend) ReadAt(b []byte, off int) (int, error) {
	if len(b) == 0 {
		return -1, ErrZeroSlice

	}

	if off+len(b)-1 > m.fstore.currPos {
		return -1, ErrNoData
	}

	n := copy(b, m.mstore[off:])
	if len(b) != n {
		return n, ErrReadTooShort
	}

	return n, nil
}

func (m *MappedBackend) madvise(off int, n int, advice int) error {
	var (
		_p    unsafe.Pointer
		_zero uintptr
		err   error
	)

	if len(m.mstore[off:off+n]) > 0 {
		_p = unsafe.Pointer(&m.mstore[0])
	} else {
		_p = unsafe.Pointer(&_zero)
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(_p),
		uintptr(len(m.mstore[off:off+n])),
		uintptr(advice),
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

// Sync syncs the underline mapped storage or a region of it if anything
// other than zero is specified to it
func (m *MappedBackend) Sync(off int, n int) error {
	var (
		_p    unsafe.Pointer
		_zero uintptr
		err   error
	)

	if len(m.mstore[off:off+n]) > 0 {
		_p = unsafe.Pointer(&m.mstore[0])
	} else {
		_p = unsafe.Pointer(&_zero)
	}
	_, _, e := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(_p),
		uintptr(len(m.mstore[off:off+n])),
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
	err := syscall.Munmap(m.mstore)
	if err != nil {
		return err
	}
	m.mstore = nil

	return m.fstore.Close()
}
