package protos

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"sync"
)

type StreamHandlerFactory interface {
	NewStreamHandlerImpl(*PeerID, bool) (StreamHandlerImpl, error)
	NewClientStream(*grpc.ClientConn) (grpc.ClientStream, error)
}

type StreamHandlerImpl interface {
	Tag() string
	EnableLoss() bool
	NewMessage() proto.Message
	HandleMessage(proto.Message) error
	BeforeSendMessage(proto.Message) error
	OnWriteError(error)
	Stop()
}

type StreamHandler struct {
	sync.RWMutex
	StreamHandlerImpl
	tag         string
	enableLoss  bool
	writeQueue  chan proto.Message
	writeExited chan error
}

const (
	defaultWriteBuffer = 32
)

func newStreamHandler(impl StreamHandlerImpl) *StreamHandler {
	return &StreamHandler{
		StreamHandlerImpl: impl,
		tag:               impl.Tag(),
		enableLoss:        impl.EnableLoss(),
		writeQueue:        make(chan proto.Message, defaultWriteBuffer),
		writeExited:       make(chan error),
	}
}

func (h *StreamHandler) SendMessage(m proto.Message) error {

	h.RLock()
	defer h.RUnlock()
	if h.writeQueue == nil {
		return fmt.Errorf("Streamhandler %s has been killed", h.tag)
	}

	if h.enableLoss {
		select {
		case h.writeQueue <- m:
			return nil
		default:
			return fmt.Errorf("Streamhandler %s's write channel full, rejecting", h.tag)
		}
	} else {
		h.writeQueue <- m
		return nil
	}

}

func (h *StreamHandler) handleWrite(stream grpc.Stream) {

	for m := range h.writeQueue {

		err := h.BeforeSendMessage(m)
		if err == nil {
			err = stream.SendMsg(m)
			if err != nil {
				h.writeExited <- err
				return
			}
		}
	}

	h.writeExited <- nil

}

func (h *StreamHandler) endHandler() {
	h.Lock()
	defer h.Unlock()
	close(h.writeQueue)
	h.writeQueue = nil

}

func (h *StreamHandler) handleStream(stream grpc.Stream) error {

	//dispatch write goroutine
	go h.handleWrite(stream)

	defer h.Stop()
	defer h.endHandler()

	for {
		in := h.NewMessage()
		err := stream.RecvMsg(in)
		if err == io.EOF {
			return fmt.Errorf("received EOF")
		} else if err != nil {
			return err
		}

		err = h.HandleMessage(in)
		if err != nil {
			//don't need to call onError again
			return err
		}

		select {
		case err := <-h.writeExited:
			//if the writting goroutine exit unexpectedly, we resume it ...
			h.OnWriteError(err)
			go h.handleWrite(stream)
		default:
			//or everything is ok
		}
	}

	return nil
}

type shandlerMap struct {
	sync.Mutex
	m map[PeerID]*StreamHandler
}

//a default implement, use two goroutine for read and write simultaneously
type StreamStub struct {
	StreamHandlerFactory
	handlerMap *shandlerMap
}

func NewStreamStub(factory StreamHandlerFactory) *StreamStub {
	return &StreamStub{
		StreamHandlerFactory: factory,
		handlerMap: &shandlerMap{
			m: make(map[PeerID]*StreamHandler),
		},
	}
}

func (s *StreamStub) registerHandler(h *StreamHandler, peerid *PeerID) error {
	s.handlerMap.Lock()
	defer s.handlerMap.Unlock()
	if _, ok := s.handlerMap.m[*peerid]; ok {
		// Duplicate,
		return fmt.Errorf("Duplicate handler for peer %s", peerid)
	}
	s.handlerMap.m[*peerid] = h
	return nil
}

func (s *StreamStub) unRegisterHandler(peerid *PeerID) {
	s.handlerMap.Lock()
	defer s.handlerMap.Unlock()
	if _, ok := s.handlerMap.m[*peerid]; ok {
		delete(s.handlerMap.m, *peerid)
	}
}

func (s *StreamStub) PickHandlers(peerids []*PeerID) []*StreamHandler {

	s.handlerMap.Lock()
	defer s.handlerMap.Unlock()

	ret := make([]*StreamHandler, 0, len(peerids))

	for _, id := range peerids {
		h, ok := s.handlerMap.m[*id]
		if ok {
			ret = append(ret, h)
		}
	}

	return ret
}

func (s *StreamStub) HandleClient(conn *grpc.ClientConn, peerid *PeerID) error {
	clistrm, err := s.NewClientStream(conn)

	if err != nil {
		return err
	}

	defer clistrm.CloseSend()

	err = clistrm.SendMsg(peerid)
	if err != nil {
		return err
	}

	himpl, err := s.NewStreamHandlerImpl(peerid, true)

	if err != nil {
		return err
	}

	h := newStreamHandler(himpl)

	err = s.registerHandler(h, peerid)
	if err != nil {
		return err
	}

	defer s.unRegisterHandler(peerid)
	return h.handleStream(clistrm)
}

func (s *StreamStub) HandleServer(stream grpc.ServerStream) error {

	id := new(PeerID)
	err := stream.RecvMsg(id)
	if err != nil {
		return err
	}

	himpl, err := s.NewStreamHandlerImpl(id, false)

	if err != nil {
		return err
	}

	h := newStreamHandler(himpl)

	err = s.registerHandler(h, id)
	if err != nil {
		return err
	}

	defer s.unRegisterHandler(id)
	return h.handleStream(stream)
}
