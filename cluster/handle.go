package cluster

import (
	"encoding/json"
	"github.com/hashicorp/memberlist"
	"log"
)

type TokenDelegate interface {
	NotifyNewToken(token int, node *Node)
	NotifyRemovedToken(token int, node *Node)
}

type Config struct {
	BindAddr     string
	BindPort     int
	MetaFilename string
}

func (c Config) SetDefaults() {
	if c.BindPort == 0 {
		c.BindPort = 8084
	}
	if c.MetaFilename == "" {
		c.MetaFilename = "/var/opt/influxdb-ha/meta"
	}
}

type Handle struct {
	list          *memberlist.Memberlist
	Nodes         map[string]*Node
	TokenDelegate TokenDelegate
	LocalNode     *LocalNode
	Config        Config
}

func NewHandle(config Config) (*Handle, error) {
	handle := &Handle{}
	config.SetDefaults()
	handle.Config = config
	handle.Nodes = make(map[string]*Node)
	nodeErr := handle.createLocalNode(config)
	if nodeErr != nil {
		return handle, nodeErr
	}

	conf := memberlist.DefaultWANConfig()
	conf.Events = eventDelegate{handle}
	conf.Delegate = nodeDelegate{handle}
	conf.BindAddr = config.BindAddr
	if config.BindPort != 0 {
		conf.BindPort = config.BindPort
	} else {
		conf.BindPort = 8084
	}
	log.Printf("[Cluster] Listening on %s:%d", conf.BindAddr, conf.BindPort)
	list, err := memberlist.Create(conf)
	if err != nil {
		return handle, err
	}
	handle.list = list
	handle.addMember(list.LocalNode())
	return handle, nil
}

// Join connects to one or more seed nodes to join the cluster.
func (h *Handle) Join(existing []string) error {
	_, err := h.list.Join(existing)
	if err != nil {
		return err
	}
	for _, member := range h.list.Members() {
		h.addMember(member)
	}
	return nil
}

type nodeUpdate struct {
	Name   string
	Tokens []int
}

func (h *Handle) BroadcastTokens() error {
	local := h.list.LocalNode()
	data, err := json.Marshal(nodeUpdate{
		h.LocalNode.Name,
		h.LocalNode.Tokens,
	})
	if err != nil {
		return err
	}
	var sendErr error
	// TODO send updates concurrently
	for _, member := range h.list.Members() {
		if member.Name != local.Name {
			sendErr = h.list.SendReliable(member, data)
			if sendErr != nil {
				log.Printf("[Cluster] Failed to send update to %s at %s", member.Name, member.Addr.String())
			}
		}
	}
	return sendErr
}

func (h *Handle) createLocalNode(config Config) error {
	filePath := config.MetaFilename
	if filePath == "" {
		filePath = "/var/opt/influxdb-ha/meta"
	}
	storage, err := openBoltStorage(filePath)
	if err != nil {
		return err
	}
	h.LocalNode = CreateNodeWithStorage(storage)
	h.LocalNode.DataLocation = "0.0.0.0:18086"
	return h.LocalNode.Init()
}

func (h *Handle) RemoveNode(name string) {
	panic("Not implemented")
}

func (h *Handle) addMember(member *memberlist.Node) {
	if _, ok := h.Nodes[member.Name]; !ok {
		node := &Node{}
		node.updateFromBytes(member.Meta)
		node.Name = member.Name
		node.DataLocation = member.Addr.String() + ":18086"
		// the resolver needs to be aware of new tokens.
		h.Nodes[member.Name] = node
		log.Printf("[Cluster] Added cluster member %s", member.Name)
		if h.TokenDelegate != nil {
			for _, token := range node.Tokens {
				h.TokenDelegate.NotifyNewToken(token, node)
			}
		}
	}
}

type eventDelegate struct {
	handle *Handle
}

func (e eventDelegate) NotifyJoin(member *memberlist.Node) {
	e.handle.addMember(member)
	if member.Name == e.handle.LocalNode.Name {
		return
	}
	data, err := json.Marshal(nodeUpdate{
		e.handle.LocalNode.Name,
		e.handle.LocalNode.Tokens})
	if err != nil {
		panic(err)
	}
	e.handle.list.SendReliable(member, data)
}

func (e eventDelegate) NotifyLeave(member *memberlist.Node) {
	if node, ok := e.handle.Nodes[member.Name]; ok {
		node.Status = STATUS_REMOVED
		log.Printf("[Cluster] Member removed %s", member.Name)
		// TODO Don't remove tokens until specifically told so. Listen for a broad-casted remove message.
		delete(e.handle.Nodes, member.Name)
		if e.handle.TokenDelegate != nil {
			for _, token := range node.Tokens {
				e.handle.TokenDelegate.NotifyRemovedToken(token, node)
			}
		}
	}
}

func (e eventDelegate) NotifyUpdate(member *memberlist.Node) {
	if node, ok := e.handle.Nodes[member.Name]; ok {
		oldTokens := []int{}
		copy(oldTokens, node.Tokens)
		node.updateFromBytes(member.Meta)
		newTokens := node.Tokens
		removed, added := compareIntSlices(oldTokens, newTokens)
		if e.handle.TokenDelegate != nil {
			for _, token := range removed {
				e.handle.TokenDelegate.NotifyRemovedToken(token, node)
			}
			for _, token := range added {
				e.handle.TokenDelegate.NotifyNewToken(token, node)
			}
		}
	}
}

type nodeDelegate struct {
	handle *Handle
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (d nodeDelegate) NodeMeta(limit int) []byte {
	// TODO get necessary data about the node like influxdb port
	return []byte{}
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (d nodeDelegate) NotifyMsg(msg []byte) {
	// TODO handle remove node, or handle it in node updates
	var update nodeUpdate
	err := json.Unmarshal(msg, &update)
	if err != nil {
		log.Print(err)
	} else {
		var node *Node
		if _, ok := d.handle.Nodes[update.Name]; !ok {
			log.Printf("[Cluster] Missing node with name %s. Creating a new node.", update.Name)
			node = &Node{}
			node.Name = update.Name
			d.handle.Nodes[node.Name] = node
		} else {
			node = d.handle.Nodes[update.Name]
		}
		if len(update.Tokens) != 0 {
			log.Printf("[Cluster] Received update from %s", node.Name)
			removed, added := compareIntSlices(node.Tokens, update.Tokens)
			if d.handle.TokenDelegate != nil {
				for _, token := range removed {
					d.handle.TokenDelegate.NotifyRemovedToken(token, node)
				}
				for _, token := range added {
					d.handle.TokenDelegate.NotifyNewToken(token, node)
				}
			}
			node.Tokens = update.Tokens
		}
	}
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (d nodeDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (d nodeDelegate) LocalState(join bool) []byte {
	return []byte{}
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (d nodeDelegate) MergeRemoteState(buf []byte, join bool) {

}

func compareIntSlices(a []int, b []int) ([]int, []int) {
	amap := map[int]bool{}
	for _, token := range a {
		amap[token] = true
	}
	bmap := map[int]bool{}
	for _, token := range b {
		bmap[token] = true
	}
	adiff := []int{}
	for _, token := range a {
		_, ok := bmap[token]
		if !ok {
			adiff = append(adiff, token)
		}
	}
	bdiff := []int{}
	for _, token := range b {
		_, ok := amap[token]
		if !ok {
			bdiff = append(bdiff, token)
		}
	}
	return adiff, bdiff
}
