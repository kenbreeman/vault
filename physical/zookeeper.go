package physical

import (
	"fmt"
	"strings"
	"time"

        "github.com/samuel/go-zookeeper/zk"

)

// ZookeeperBackend is a Zookeeper-based physical backend.
type ZookeeperBackend struct {
	zks  []string
	datapath string
	lockpath string
	conn *zk.Conn
	acl []zk.ACL
}

func createPath(path string, conn *zk.Conn, acl []zk.ACL) error {
	exists, _, err := conn.Exists(path)
	if err != nil {
		return fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err.Error())
	}
	if !exists {
		_, err2 := conn.Create(path, []byte{}, int32(0), acl)
		if err2 != nil {
			return fmt.Errorf("Failed to Create configured path with Zookeeper physical backend: " + err2.Error())
		}
	}
	return nil
}

// NewZookeeper constructs a new Zookeeper backend
func newZookeeperBackend(conf map[string]string) (Backend, error) {
	// Get the connect string for Zookeeper
	connectStr, ok := conf["connect"]
	if !ok {
		return nil, fmt.Errorf("'connect' must be set for Zookeeper physical backend")
	}
	zks := strings.Split(connectStr, ",")
	// Get the path in Zookeeper
	basepath, ok := conf["path"]
	if !ok {
		basepath = "/vault"
	}
	// Ensure path is prefixed but not suffixed, opposite of Consul
	if !strings.HasPrefix(basepath, "/") {
		basepath = "/" + basepath
	}
	if strings.HasSuffix(basepath, "/") {
		basepath = strings.TrimSuffix(basepath, "/")
	}
	// Establish the initial connection to Zookeeper
	conn, _, err := zk.Connect(zks, time.Second)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to Zookeeper physical backend: " + err.Error())
	}
	// Create our basepath in Zookeeper if it doesn't already exist
	acl := zk.WorldACL(zk.PermAll)
	err1 := createPath(basepath, conn, acl)
	if err1 != nil {
		return nil, err1
	}
	lockpath := basepath + "/lock"
	err2 := createPath(lockpath, conn, acl)
	if err2 != nil {
		return nil, err2
	}
	datapath := basepath + "/data"
	err3 := createPath(datapath, conn, acl)
	if err3 != nil {
		return nil, err3
	}

	in := &ZookeeperBackend{
		zks:  zks,
		datapath: datapath,
		lockpath: lockpath,
		conn: conn,
		acl: acl,
	}
	return in, nil
}

// Put is used to insert or update an entry
func (i *ZookeeperBackend) Put(entry *Entry) error {
	datapath := i.getDataPath(entry.Key)
	lock := i.getLock(entry.Key)
	err1 := lock.Lock()
	if err1 != nil {
		return fmt.Errorf("Failed to Lock prior to Put with Zookeeper physical backend: " + err1.Error())
	}
	defer lock.Unlock()
	_, err2 := i.conn.Create(datapath, entry.Value, int32(0), i.acl)
	if err2 != nil {
		return fmt.Errorf("Failed to Put with Zookeeper physical backend: " + err2.Error())
	}
	return nil
}

// Get is used to fetch an entry
func (i *ZookeeperBackend) Get(key string) (*Entry, error) {
	datapath := i.getDataPath(key)
	lock := i.getLock(key)
	err1 := lock.Lock()
	if err1 != nil {
		return nil, fmt.Errorf("Failed to Lock prior to Get with Zookeeper physical backend: " + err1.Error())
	}
	defer lock.Unlock()
	exists, _, err2 := i.conn.Exists(datapath)
	if err2 != nil {
		return nil, fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil, nil
	}
	data, _, err3 := i.conn.Get(datapath)
	if err3 != nil {
		return nil, fmt.Errorf("Failed to Get with Zookeeper physical backend: " + err3.Error())
	}
	entry := &Entry {
		Key: key,
		Value: data,
	}
	return entry, nil
}

// Delete is used to permanently delete an entry
func (i *ZookeeperBackend) Delete(key string) error {
	datapath := i.getDataPath(key)
	lock := i.getLock(key)
	err1 := lock.Lock()
	if err1 != nil {
		return fmt.Errorf("Failed to Lock prior to Delete with Zookeeper physical backend: " + err1.Error())
	}
	defer lock.Unlock()
	exists, _, err2 := i.conn.Exists(datapath)
	if err2 != nil {
		return fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil
	}
	err3 := i.conn.Delete(datapath, int32(-1))
	if err3 != nil {
		return fmt.Errorf("Failed to Delete with Zookeeper physical backend: " + err3.Error())
	}
	return nil
}

// List is used ot list all the keys under a given
// prefix, up to the next prefix.
func (i *ZookeeperBackend) List(prefix string) ([]string, error) {
	datapath := i.getDataPath(prefix)
	exists, _, err2 := i.conn.Exists(datapath)
	if err2 != nil {
		return nil, fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil, nil
	}
	children, _, err3 := i.conn.Children(datapath)
	if err3 != nil {
		return nil, fmt.Errorf("Failed to Children with Zookeeper physical backend: " + err3.Error())
	}
	// Vault allows entries and directories to have duplicate names, differrentiated by a trailing '/'
	var listing []string
	for _, child := range children {
		listing = append(listing, child)
		grandchildren, _, err4 := i.conn.Children(datapath + "/" + child)
		if err4 != nil {
			return nil, fmt.Errorf("Failed to Children with Zookeeper physical backend: " + err3.Error())
		}
		if len(grandchildren) > 0 {
			listing = append(listing, child + "/")
		}
	}
	return listing, nil
}

func (i *ZookeeperBackend) getDataPath(key string) string {
	path := i.datapath + "/" + key
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	return path
}

func (i *ZookeeperBackend) getLockPath(key string) string {
	path := i.lockpath + "/" + key
	if !strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	return path
}

func (i *ZookeeperBackend) getLock(key string) *zk.Lock {
	acl := zk.WorldACL(zk.PermAll)
	lockpath := i.getLockPath(key)
	lock := zk.NewLock(i.conn, lockpath, acl)
	return lock
}

