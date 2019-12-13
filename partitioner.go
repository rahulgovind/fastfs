package main

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/rahulgovind/fastfs/consistenthash"
	log "github.com/sirupsen/logrus"
)

// Partitioner has one job:
// Given a file and block number where should the file ideally be stored?
type Partitioner interface {
	GetServer(path string, block int) string
	NotifyJoin(n *memberlist.Node)
	NotifyLeave(n *memberlist.Node)
	NotifyUpdate(n *memberlist.Node)
}

type HashPartitioner struct {
	hm *consistenthash.Map
}

func NewHashPartitioner() *HashPartitioner {
	hp := new(HashPartitioner)
	hp.hm = consistenthash.New(3, nil)
	return hp
}

func (hp *HashPartitioner) GetServer(path string, block int) string {
	return hp.hm.Get(fmt.Sprintf("%s:%d", path, block))
}

func (hp *HashPartitioner) NotifyJoin(n *memberlist.Node) {
	log.Infof("Adding %v to hash ring", n.Name)
	hp.hm.Add(n.Name)
}

func (hp *HashPartitioner) NotifyLeave(n *memberlist.Node) {
	log.Infof("Removing %v from hash ring", n.Name)
	hp.hm.Remove(n.Name)
}

func (hp *HashPartitioner) NotifyUpdate(n *memberlist.Node) {
	log.Infof("Ignoring hash ring metadata update for %v", n.Name)
}
