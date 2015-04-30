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
	path string
	conn *zk.Conn
}

// NewZookeeper constructs a new Zookeeper backend
func NewZookeeper(conf map[string]string) (*ZookeeperBackend, error) {
	// Get the connect string for Zookeeper
	connectStr, ok := conf["connect"]
	if !ok {
		return nil, fmt.Errorf("'connect' must be set for Zookeeper physical backend")
	}
	zks := strings.Split(connectStr, ",")
	// Get the path in Zookeeper
	path, ok := conf["path"]
	if !ok {
		path = "/vault"
	}
	// Ensure path is prefixed but not suffixed, opposite of Consul
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	// Establish the initial connection to Zookeeper
	conn, _, err := zk.Connect(zks, time.Second)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to Zookeeper physical backend: " + err.Error())
	}
	// Create our base path in Zookeeper if it doesn't already exist
	acl := zk.WorldACL(zk.PermAll)
	exists, _, err2 := conn.Exists(path)
	if err2 != nil {
		return nil, fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		_, err3 := conn.Create(path, []byte{}, int32(0), acl)
		if err3 != nil {
			return nil, fmt.Errorf("Failed to Create configured path with Zookeeper physical backend: " + err3.Error())
		}
	}
	in := &ZookeeperBackend{
		zks:  zks,
		path: path,
		conn: conn,
	}
	return in, nil
}

// Put is used to insert or update an entry
func (i *ZookeeperBackend) Put(entry *Entry) error {
	fullpath := i.path + "/" + entry.Key
	acl := zk.WorldACL(zk.PermAll)
//	lock := zk.NewLock(i.conn, fullpath, acl)
//	err1 := lock.Lock()
//	if err1 != nil {
//		return fmt.Errorf("Failed to Lock prior to Put with Zookeeper physical backend: " + err.Error())
//	}
//	defer lock.Unlock()
	_, err2 := i.conn.Create(fullpath, entry.Value, int32(0), acl)
	if err2 != nil {
		return fmt.Errorf("Failed to Put with Zookeeper physical backend: " + err2.Error())
	}
	return nil
}

// Get is used to fetch an entry
func (i *ZookeeperBackend) Get(key string) (*Entry, error) {
	fullpath := i.path + "/" + key
//	acl := zk.WorldACL(zk.PermAll)
//	lock := zk.NewLock(i.conn, fullpath, acl)
//	err1 := lock.Lock()
//	if err1 != nil {
//		return nil, fmt.Errorf("Failed to Lock prior to Get with Zookeeper physical backend: " + err.Error())
//	}
//	defer lock.Unlock()
	exists, _, err2 := i.conn.Exists(fullpath)
	if err2 != nil {
		return nil, fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil, nil
	}
	data, _, err3 := i.conn.Get(fullpath)
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
	fullpath := i.path + "/" + key
//	acl := zk.WorldACL(zk.PermAll)
//	lock := zk.NewLock(i.conn, fullpath, acl)
//	err1 := lock.Lock()
//	if err1 != nil {
//		return fmt.Errorf("Failed to Lock prior to Delete with Zookeeper physical backend: " + err.Error())
//	}
//	defer lock.Unlock()
	exists, _, err2 := i.conn.Exists(fullpath)
	if err2 != nil {
		return fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil
	}
	err3 := i.conn.Delete(fullpath, int32(-1))
	if err3 != nil {
		return fmt.Errorf("Failed to Get with Zookeeper physical backend: " + err3.Error())
	}
	return nil
}

// List is used ot list all the keys under a given
// prefix, up to the next prefix.
func (i *ZookeeperBackend) List(prefix string) ([]string, error) {
	fullpath := i.path + "/" + prefix
	if strings.HasSuffix(fullpath, "/") {
		fullpath = strings.TrimSuffix(fullpath, "/")
	}
//	acl := zk.WorldACL(zk.PermAll)
//	lock := zk.NewLock(i.conn, fullpath, acl)
//	err1 := lock.Lock()
//	if err1 != nil {
//		return nil, fmt.Errorf("Failed to Lock prior to List with Zookeeper physical backend: " + err.Error())
//	}
//	defer lock.Unlock()
	exists, _, err2 := i.conn.Exists(fullpath)
	if err2 != nil {
		return nil, fmt.Errorf("Failed to Exists with Zookeeper physical backend: " + err2.Error())
	}
	if !exists {
		return nil, nil
	}
	children, _, err3 := i.conn.Children(fullpath)
	if err3 != nil {
		return nil, fmt.Errorf("Failed to Children with Zookeeper physical backend: " + err3.Error())
	}
	// Vault allows entries and directories to have duplicate names, differrentiated by a trailing '/'
	listing := children
	for _, child := range children {
		grandchildren, _, err4 := i.conn.Children(fullpath + "/" + child)
		if err4 != nil {
			return nil, fmt.Errorf("Failed to Children with Zookeeper physical backend: " + err3.Error())
		}
		if len(grandchildren) > 0 {
			listing = append(listing, child + "/")
		}
	}
	return listing, nil
}

