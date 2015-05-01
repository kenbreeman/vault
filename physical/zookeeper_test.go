package physical

import "testing"

func TestZookeeper(t *testing.T) {
	conf := make(map[string]string)
	conf["connect"] = "localhost:2181"
	z, err := newZookeeperBackend(conf)
	if err != nil {
		panic("Failed to create NewZookeeper: " + err.Error())
	}
	testBackend(t, z)
	testBackend_ListPrefix(t, z)
}

