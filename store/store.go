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
	Sync(off int, n int)
	Close() error
}

//FileSize is the default size for each db file ~68Gb
const FileSize = 4096 * 4096 * 4096

// Store errors types
var (
	ErrZeroSlice = fmt.Errorf("Byte slice size must be more than 0")
	ErrNoData    = fmt.Errorf("Offset must be within valid data region")
	ErrSizeLimit = fmt.Errorf("Store max size limit of 1 tera reached")
)

// FileStore is the dt responsible for backing the skiplist on disk
type FileStore struct {
	files            []*os.File
	data             [][]byte
	fname            string
	current          int
	fileStoreMaxsize int
}

func newFileStore() *FileStore {
	return &FileStore{
		fname:            "index.",
		fileStoreMaxsize: FileSize * 15,
	}
}

//Create new FileStore backing
func (s *FileStore) Create() error {
	if err := s.resize(0); err != nil {
		return err
	}

	return nil
}

//WriteAt write at said location
func (s *FileStore) WriteAt(b []byte, off int) (int, error) {
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
func (s *FileStore) ReadAt(b []byte, off int) (int, error) {
	return s.files[len(s.files)-1].ReadAt(b, int64(off))
}

func (s *FileStore) resize(size int) error {
	if size+s.current > FileSize || size == 0 {
		fname := fmt.Sprintf("%v%v", s.fname, len(s.files))
		file, err := os.Create(fname)
		if err != nil {
			return err
		}

		if err := file.Truncate(FileSize); err != nil {
			return err
		}

		s.files = append(s.files, file)
	}

	return nil
}

// Sync either sync the everything of calls sync file range with the
// specified off and n number of bytes ( sync only the pages the need
// to be synched
func (s *FileStore) Sync(off int, n int) {
	if off == 0 && n == 0 {
		syscall.Sync()
		return
	}
	syscall.SyncFileRange(
		int(s.files[len(s.files)-1].Fd()),
		int64(off),
		int64(n),
		0,
	)
}

// Close the FileStore and syncs
func (s *FileStore) Close() error {
	for i := range s.files {
		if err := s.files[i].Close(); err != nil {
			return err
		}
	}

	s.Sync(0, 0)

	return nil
}

// MappedStore is a memory mapped store that only maps for writes
type MappedStore struct {
	fstore *FileStore
	mstore [][]byte
}

func newMappedStore() *MappedStore {
	return &MappedStore{newFileStore(), nil}
}

//Create a new mapped store
func (m *MappedStore) Create() error {
	if err := m.fstore.Create(); err != nil {
		return err
	}

	prot := syscall.PROT_WRITE | syscall.PROT_READ
	flag := syscall.MAP_SHARED
	fd := m.fstore.files[len(m.fstore.files)-1].Fd()
	buf, err := syscall.Mmap(int(fd), 0, int(FileSize), prot, flag)
	if err != nil {
		return err
	}
	m.mstore = append(m.mstore, buf)

	return nil
}

//WriteAt write at said location
func (m *MappedStore) WriteAt(b []byte, off int) (int, error) {
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
func (m *MappedStore) ReadAt(b []byte, off int) (int, error) {
	if len(b) == 0 {
		return -1, ErrZeroSlice

	}

	if int(off)+len(b)-1 > m.fstore.current {
		return -1, ErrNoData
	}

	for i, j := off, 0; i < off+len(b) && j < len(b); i, j = i+1, j+1 {
		b[j] = m.mstore[len(m.mstore)-1][i]

	}

	return len(b), nil
}

// Sync syncs the underline mapped storage or a region of it if anything
// other than zero is specified to it
func (m *MappedStore) Sync(off int, n int) error {
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
func (m *MappedStore) Close() error {
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
