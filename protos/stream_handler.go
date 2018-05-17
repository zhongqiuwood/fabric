package protos

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"sync"
)

/*

  a streamhandler factory include two methods:

  1. Stream creator: generate a stream from given connection
  2. Handler creator: generate a handler which is binded to specified peer, with a bool parameter indicate
     it should act as client or server (may be omitted in a bi-directory session)

*/
type StreamHandlerFactory interface {
	NewClientStream(*grpc.ClientConn) (grpc.ClientStream, error)
	NewStreamHandlerImpl(*PeerID, bool) (StreamHandlerImpl, error)
}

/*

  each streamhandler implement exposed following methods for working in a streaming, it supposed message with
  certain type is transmitted in the stream and each end handle this message by a streamhandler implement:

  Tag: providing a string tag for the implement
  EnableLoss: indicate the message transmitted in stream can be dropped for any reason (send buffer full, bad
			  linking, deliberately omitted by the other side ...)
  NewMessage: provided a prototype object of the transamitted message for receiving and handling later in a HandleMessage call
  HandleMessage: handling a message received from the other side. The object was allocated before in a NewMessage call and
			  the wire data was parsed and put into it
  BeforeSendMessage: when a message is ready to send to the other side (by calling SendMessage in a StreamHandler), this method
			  is called to give handler a last chance for dropping (by return a non-nil error) the message or do any statistics
			  jobs. Method MAY be called in different thread so you must protect your data from any racing
  OnWriteError: method is called if any error raised in sending message
  Stop: when the stream is broken this method is called and no more calling will be made on this handler

  *** All calling to the methods (except for BeforeSendMessage) are ensured being raised in the same thread ***

*/
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

	select {
	case h.writeQueue <- m:
		return nil
	default:
		if !h.enableLoss {
			return fmt.Errorf("Streamhandler %s's write channel full, rejecting", h.tag)
		}
	}

	return nil

}

func (h *StreamHandler) handleWrite(stream grpc.Stream) {

	var m proto.Message
	for m = range h.writeQueue {

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
