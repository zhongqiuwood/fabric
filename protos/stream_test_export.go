package protos

import (
	"fmt"
	_ "github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

func HandleDummyWrite(ctx context.Context, h *StreamHandler) {

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-h.writeQueue:
			//"swallow" the message silently
			h.BeforeSendMessage(m)
		}
	}

}

func HandleDummyComm(ctx context.Context, hfrom *StreamHandler, hto *StreamHandler) error {

	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-hfrom.writeQueue:
			if err := hfrom.BeforeSendMessage(m); err != nil {
				err = hto.HandleMessage(m)
				if err != nil {
					return err
				}
			}
		}
	}
}

//integrate the bi-direction comm into one func, but may lead to unexpected dead lock
func HandleDummyBiComm(ctx context.Context, h1 *StreamHandler, h2 *StreamHandler) error {

	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-h1.writeQueue:
			if err := h1.BeforeSendMessage(m); err != nil {
				err = h2.HandleMessage(m)
				if err != nil {
					return err
				}
			}
		case m := <-h2.writeQueue:
			if err := h2.BeforeSendMessage(m); err != nil {
				err = h1.HandleMessage(m)
				if err != nil {
					return err
				}
			}
		}
	}

}

type simuPeerStub struct {
	id *PeerID
	*StreamStub
}

//s1 is act as client and s2 as service, create bi-direction comm between two handlers
func (s1 *simuPeerStub) ConnectTo(ctx context.Context, s2 *simuPeerStub) (err error) {

	var hi StreamHandlerImpl
	hi, _ = s1.NewStreamHandlerImpl(s2.id, true)
	s1h := newStreamHandler(hi)
	err = s1.registerHandler(s1h, s2.id)
	if err != nil {
		err = fmt.Errorf("reg s1 fail: %s", err)
		return
	}

	hi, _ = s2.NewStreamHandlerImpl(s2.id, true)
	s2h := newStreamHandler(hi)
	err = s1.registerHandler(s2h, s1.id)
	if err != nil {
		err = fmt.Errorf("reg s2 fail: %s", err)
		return
	}

	go HandleDummyBiComm(ctx, s1h, s2h)

	return
}

func NewSimuPeerStub(id string, fac StreamHandlerFactory) *simuPeerStub {

	return &simuPeerStub{
		id:         &PeerID{id},
		StreamStub: NewStreamStub(fac),
	}

}
