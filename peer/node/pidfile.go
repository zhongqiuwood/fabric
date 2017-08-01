// +build !windows

package node

import (
	"fmt"
	"bytes"
	"os"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"syscall"
)

func writePid(fileName string, pid int) error {
	err := os.MkdirAll(filepath.Dir(fileName), 0755)
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("can't lock '%s', lock is held", fd.Name())
	}

	if _, err := fd.Seek(0, 0); err != nil {
		return err
	}

	if err := fd.Truncate(0); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(fd, "%d", pid); err != nil {
		return err
	}

	if err := fd.Sync(); err != nil {
		return err
	}

	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("can't release lock '%s', lock is held", fd.Name())
	}
	return nil
}

func killbyPidfile(pidFile string) error{
	
	fmt.Printf("Stopping local peer using process pid from %s \n", pidFile)
	//logger.Infof("Error trying to connect to local peer: %s", err)
	logger.Infof("Stopping local peer using process pid from %s", pidFile)
	pid, ferr := readPid(pidFile)
	if ferr != nil {
		err := fmt.Errorf("Error trying to read pid from %s: %s", pidFile, ferr)
		return err
	}
	killerr := syscall.Kill(pid, syscall.SIGTERM)
	if killerr != nil {
		err := fmt.Errorf("Error trying to kill -9 pid %d: %s", pid, killerr)
		return err
	}
	return nil;
}

func readPid(fileName string) (int, error) {
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fd.Close()
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return 0, fmt.Errorf("can't lock '%s', lock is held", fd.Name())
	}

	if _, err := fd.Seek(0, 0); err != nil {
		return 0, err
	}

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(string(bytes.TrimSpace(data)))
	if err != nil {
		return 0, fmt.Errorf("error parsing pid from %s: %s", fd.Name(), err)
	}

	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_UN); err != nil {
		return 0, fmt.Errorf("can't release lock '%s', lock is held", fd.Name())
	}

	return pid, nil

}