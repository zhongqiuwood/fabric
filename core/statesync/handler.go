package statesync

import (
	pb "github.com/abchain/fabric/protos"
	"github.com/looplab/fsm"
)

var syncPhase = []string{"synclocating", "syncdelta", "syncblock", "syncsnapshot"}

var enterGetBlock = "GetBlock"
var enterGetSnapshot = "GetSnapshot"
var enterGetDelta = "GetDelta"
var enterSyncBegin = "SyncBegin"
var enterSyncFinish = "SyncFinish"

func newFsmHandler(h *stateSyncHandler) *fsm.FSM {

	return fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: pb.SyncMsg_SYNC_STATE_NOTIFY.String(), Src: []string{"idle"}, Dst: "idle"},
			{Name: pb.SyncMsg_SYNC_STATE_OPT.String(),    Src: []string{"idle"}, Dst: "idle"},

			//serving phase
			{Name: pb.SyncMsg_SYNC_SESSION_START.String(),        Src: []string{"idle"},  Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_QUERY.String(),        Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_BLOCKS.String(),   Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_SNAPSHOT.String(), Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_GET_DELTAS.String(),   Src: []string{"serve"}, Dst: "serve"},
			{Name: pb.SyncMsg_SYNC_SESSION_END.String(),          Src: []string{"serve"}, Dst: "idle"},

			//client phase
			{Name: pb.SyncMsg_SYNC_SESSION_START_ACK.String(), Src: []string{"synchandshake"}, Dst: "synclocating"},
			{Name: pb.SyncMsg_SYNC_SESSION_RESPONSE.String(),  Src: []string{"synclocating"},  Dst: "synclocating"},
			{Name: pb.SyncMsg_SYNC_SESSION_BLOCKS.String(),    Src: []string{"syncblock"},     Dst: "syncblock"},
			{Name: pb.SyncMsg_SYNC_SESSION_SNAPSHOT.String(),  Src: []string{"syncsnapshot"},  Dst: "syncsnapshot"},
			{Name: pb.SyncMsg_SYNC_SESSION_DELTAS.String(),    Src: []string{"syncdelta"},     Dst: "syncdelta"},
			{Name: enterSyncBegin,   Src: []string{"idle"}, Dst: "synchandshake"},
			{Name: enterGetBlock,    Src: syncPhase,        Dst: "syncblock"},
			{Name: enterGetSnapshot, Src: syncPhase,        Dst: "syncsnapshot"},
			{Name: enterGetDelta,    Src: syncPhase,        Dst: "syncdelta"},
			{Name: enterSyncFinish,  Src: syncPhase,        Dst: "idle"},

		},
		fsm.Callbacks{
			// for both server and client
			"leave_idle":                                          func(e *fsm.Event) { h.leaveIdle(e) },
			"enter_idle":                                          func(e *fsm.Event) { h.enterIdle(e) },

			// server
			"before_" + pb.SyncMsg_SYNC_SESSION_START.String():        func(e *fsm.Event) { h.beforeSyncStart(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_QUERY.String():        func(e *fsm.Event) { h.server.beforeQuery(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_GET_BLOCKS.String():   func(e *fsm.Event) { h.server.beforeGetBlocks(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_GET_DELTAS.String():   func(e *fsm.Event) { h.server.beforeGetDeltas(e) },
			"before_" + pb.SyncMsg_SYNC_SESSION_END.String():          func(e *fsm.Event) { h.server.beforeSyncEnd(e) },
			"leave_serve":                                             func(e *fsm.Event) { h.server.leaveServe(e) },
			"enter_serve":                                             func(e *fsm.Event) { h.server.enterServe(e) },

			// client
			"after_" + pb.SyncMsg_SYNC_SESSION_START_ACK.String(): func(e *fsm.Event) { h.client.afterSyncStartResponse(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_RESPONSE.String():  func(e *fsm.Event) { h.client.afterQueryResponse(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_BLOCKS.String():    func(e *fsm.Event) { h.client.afterSyncBlocks(e) },
			"after_" + pb.SyncMsg_SYNC_SESSION_DELTAS.String():    func(e *fsm.Event) { h.client.afterSyncStateDeltas(e) },

			"leave_synclocating":                                  func(e *fsm.Event) { h.client.leaveSyncLocating(e) },
			"leave_syncblock":                                     func(e *fsm.Event) { h.client.leaveSyncBlocks(e) },
			"leave_syncsnapshot":                                  func(e *fsm.Event) { h.client.leaveSyncStateSnapshot(e) },
			"leave_syncdelta":                                     func(e *fsm.Event) { h.client.leaveSyncStateDeltas(e) },

			"enter_synclocating":                                  func(e *fsm.Event) { h.client.enterSyncLocating(e) },
			"enter_syncblock":                                     func(e *fsm.Event) { h.client.enterSyncBlocks(e) },
			"enter_syncsnapshot":                                  func(e *fsm.Event) { h.client.enterSyncStateSnapshot(e) },
			"enter_syncdelta":                                     func(e *fsm.Event) { h.client.enterSyncStateDeltas(e) },
		},
	)

}
