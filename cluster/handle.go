package cluster

import (
	"github.com/hashicorp/memberlist"
)

type TokenDelegate interface {
	NotifyNewToken(token int, node *Node)
	NotifyRemovedToken(token int, node *Node)
}

type Handle struct {
	list		*memberlist.Memberlist
	Nodes		[]*Node
	TokenDelegate	*TokenDelegate
}

func NewHandle() (*Handle, error) {
	handle := &Handle{}
	handle.Nodes = []*Node{}

	conf := memberlist.DefaultWANConfig()
	conf.Events = eventDelegate{handle}
	list, err := memberlist.Create(conf)
	if err != nil {
		return handle, err
	}
	handle.list = list

	return handle, nil
}

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

func (h *Handle) addMember(member *memberlist.Node) {
	node := &Node{}
	node.updateFromBytes(member.Meta)
	node.Name = member.Name
	// the resolver needs to be aware of new tokens.
	h.Nodes = append(h.Nodes, node)
	if h.TokenDelegate != nil {
		for _, token := range node.Tokens {
			(*h.TokenDelegate).NotifyNewToken(token, node)
		}
	}
}

type eventDelegate struct {
	handle	*Handle
}

func (e eventDelegate) NotifyJoin(member *memberlist.Node) {
	e.handle.addMember(member)
}

func (e eventDelegate) NotifyLeave(member *memberlist.Node) {
	for i, node := range e.handle.Nodes {
		if node.Name == member.Name {
			e.handle.Nodes = append(e.handle.Nodes[:i], e.handle.Nodes[i+1:]...)
			if e.handle.TokenDelegate != nil {
				for _, token := range node.Tokens {
					(*e.handle.TokenDelegate).NotifyRemovedToken(token, node)
				}
			}
		}
	}
}

func (e eventDelegate) NotifyUpdate(member *memberlist.Node) {
	for _, node := range e.handle.Nodes {
		if node.Name == member.Name {
			oldTokens := []int{}
			copy(oldTokens, node.Tokens)
			node.updateFromBytes(member.Meta)
			newTokens := node.Tokens
			removed, added := compareIntSlices(oldTokens, newTokens)
			if e.handle.TokenDelegate != nil {
				for _, token := range removed {
					(*e.handle.TokenDelegate).NotifyRemovedToken(token, node)
				}
				for _, token := range added {
					(*e.handle.TokenDelegate).NotifyNewToken(token, node)
				}
			}
		}
	}
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