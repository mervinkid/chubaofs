// Copyright 2018 The The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
)

const (
	MaxUsedMemFactor = 1.1
)

func (m *MetadataManager) opMasterHeartbeat(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	// For ack to master
	m.responseAckOKToMaster(conn, p)
	var (
		req       = &proto.HeartBeatRequest{}
		resp      = &proto.MetaNodeHeartbeatResponse{}
		adminTask = &proto.AdminTask{
			Request: req,
		}
	)
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		goto end
	}

	// collect memory info
	resp.Total = configTotalMem
	resp.Used, err = util.GetProcessMemory(os.Getpid())
	if err != nil {
		adminTask.Status = proto.TaskFailed
		goto end
	}
	m.Range(func(id uint64, partition *MetaPartition) bool {
		mConf := partition.GetBaseConfig()
		mpr := &proto.MetaPartitionReport{
			PartitionID: mConf.PartitionId,
			Start:       mConf.Start,
			End:         mConf.End,
			Status:      proto.ReadWrite,
			MaxInodeID:  mConf.Cursor,
			VolName:     mConf.VolName,
		}
		addr, isLeader := partition.IsLeader()
		if addr == "" {
			mpr.Status = proto.Unavailable
		}
		mpr.IsLeader = isLeader
		if mConf.Cursor >= mConf.End {
			mpr.Status = proto.ReadOnly
		}
		if resp.Used > uint64(float64(resp.Total)*MaxUsedMemFactor) {
			mpr.Status = proto.ReadOnly
		}
		resp.MetaPartitionReports = append(resp.MetaPartitionReports, mpr)
		return true
	})
	resp.ZoneName = m.zoneName
	resp.Status = proto.TaskSucceeds
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	data, _ := json.Marshal(resp)
	log.LogInfof("%s [opMasterHeartbeat] req:%v; respAdminTask: %v, "+
		"resp: %v", remoteAddr, req, adminTask, string(data))
	return
}

func (m *MetadataManager) opCreateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	defer func() {
		var buf []byte
		status := proto.OpOk
		if err != nil {
			status = proto.OpErr
			buf = []byte(err.Error())
		}
		p.PacketErrorWithBody(status, buf)
		m.respondToClient(conn, p)
	}()
	req := &proto.CreateMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]: Unmarshal AdminTask"+
			" struct: %s", err.Error())
		return
	}
	log.LogDebugf("[opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", remoteAddr, adminTask)
	// create a new meta partition.
	if err = m.createPartition(req.PartitionID, req.VolName,
		req.Start, req.End, req.Members); err != nil {
		err = errors.NewErrorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
		return
	}
	log.LogInfof("%s [opCreateMetaPartition] req:%v; resp: %v", remoteAddr,
		req, adminTask)
	return
}

// Handle OpCreate inode.
func (m *MetadataManager) opCreateInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.CreateInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInode(req, p)
	// reply the operation result to the client through TCP
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCreateInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaLinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.LinkInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInodeLink(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLinkInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpCreate
func (m *MetadataManager) opFreeInodeOnRaftFollower(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp.internalDelete(p.Data[:p.Size])
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

// Handle OpCreate
func (m *MetadataManager) opCreateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.CreateDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opCreateDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *MetadataManager) opDeleteDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.DeleteDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteDentry] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opUpdateDentry(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.UpdateDentryRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.UpdateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opUpdateDentry] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaUnlinkInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.UnlinkInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.UnlinkInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Handle OpReadDir
func (m *MetadataManager) opReadDir(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opReadDir] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaInodeGet(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.InodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaInodeGet]: %s", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.NewErrorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaInodeGet] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaEvictInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.EvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaEvictInode] request unmarshal: %v", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opMetaEvictInode] req: %v, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = mp.EvictInode(req, p); err != nil {
		err = errors.NewErrorf("[opMetaEvictInode] req: %v, resp: %v", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaEvictInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opSetAttr(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.SetAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %v", req, err.Error())
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %v", req, err.Error())
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.SetAttr(p.Data, p); err != nil {
		err = errors.NewErrorf("[opSetAttr] req: %v, error: %s", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opSetAttr] req: %d - %v, resp: %v, body: %s", remoteAddr,
		p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Lookup request
func (m *MetadataManager) opMetaLookup(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.Lookup(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaLookup] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaExtentsAdd(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.NewErrorf("%s, response to client: %s", err.Error(),
			p.GetResultMsg())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("%s [opMetaExtentsAdd] ExtentAppend: %s, "+
			"response to client: %s", remoteAddr, err.Error(), p.GetResultMsg())
	}
	log.LogDebugf("%s [opMetaExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaExtentsList(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaExtentsList] req: %d - %v; resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaExtentsDel(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	panic("not implemented yet")
}

func (m *MetadataManager) opMetaExtentsTruncate(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.TruncateRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	mp.ExtentsTruncate(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [OpMetaTruncate] req: %d - %v, resp body: %v, "+
		"resp body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

// Delete a meta partition.
func (m *MetadataManager) opDeleteMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	req := &proto.DeleteMetaPartitionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}
	// Ack the master request
	conf := mp.GetBaseConfig()
	mp.Stop()
	mp.DeleteRaft()
	m.deletePartition(mp.GetBaseConfig().PartitionId)
	os.RemoveAll(conf.RootDir)
	p.PacketOkReply()
	m.respondToClient(conn, p)
	runtime.GC()
	log.LogInfof("%s [opDeleteMetaPartition] req: %d - %v, resp: %v",
		remoteAddr, p.GetReqID(), req, err)
	return
}

func (m *MetadataManager) opUpdateMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := new(proto.UpdateMetaPartitionRequest)
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	m.responseAckOKToMaster(conn, p)
	resp := &proto.UpdateMetaPartitionResponse{
		VolName:     req.VolName,
		PartitionID: req.PartitionID,
		End:         req.End,
	}
	err = mp.UpdatePartition(req, resp)
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
	log.LogInfof("%s [opUpdateMetaPartition] req[%v], response[%v].",
		remoteAddr, req, adminTask)
	return
}

func (m *MetadataManager) opLoadMetaPartition(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.MetaPartitionLoadRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = mp.ResponseLoadMetaPartition(p); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		log.LogErrorf("%s [opLoadMetaPartition] req[%v], "+
			"response marshal[%v]", remoteAddr, req, err.Error())
		m.respondToClient(conn, p)
		return
	}
	m.respondToClient(conn, p)
	log.LogInfof("%s [opLoadMetaPartition] req[%v], response status[%s], "+
		"response body[%s], error[%v]", remoteAddr, req, p.GetResultMsg(), p.Data,
		err)
	return
}

func (m *MetadataManager) opDecommissionMetaPartition(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.MetaPartitionDecommissionRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opDecommissionMetaPartition]: partitionID= %d, "+
			"Marshal %s", req.PartitionID, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *MetadataManager) opAddMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.AddMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpTryOtherAddr, ([]byte)(proto.ErrMetaPartitionNotExists.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if mp.IsExsitPeer(req.AddPeer) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.AddPeer.ID == 0 {
		err = errors.NewErrorf("[opAddMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali AddPeerID %v", req.AddPeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *MetadataManager) opRemoveMetaPartitionRaftMember(conn net.Conn,
	p *Packet, remoteAddr string) (err error) {
	var reqData []byte
	req := &proto.RemoveMetaPartitionRaftMemberRequest{}
	adminTask := &proto.AdminTask{
		Request: req,
	}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}

	if !mp.IsExsitPeer(req.RemovePeer) {
		p.PacketOkReply()
		m.respondToClient(conn, p)
		return
	}

	if !m.serveProxy(conn, mp, p) {
		return nil
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, err)
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if err = mp.CanRemoveRaftMember(req.RemovePeer); err != nil {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if req.RemovePeer.ID == 0 {
		err = errors.NewErrorf("[opRemoveMetaPartitionRaftMember]: partitionID= %d, "+
			"Marshal %s", req.PartitionId, fmt.Sprintf("unavali RemovePeerID %v", req.RemovePeer.ID))
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		m.respondToClient(conn, p)
		return err
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)

	return
}

func (m *MetadataManager) opMetaBatchInodeGet(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.BatchInodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, nil)
		return
	}
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchInodeGet] req: %d - %v, resp: %v, "+
		"body: %s", remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaPartitionTryToLeader(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	mp, err := m.getPartition(p.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = mp.TryToLeader(p.PartitionID); err != nil {
		goto errDeal
	}
	p.PacketOkReply()
	m.respondToClient(conn, p)
	return
errDeal:
	p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
	m.respondToClient(conn, p)
	return
}

func (m *MetadataManager) opMetaDeleteInode(conn net.Conn, p *Packet,
	remoteAddr string) (err error) {
	req := &proto.DeleteInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteInode(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaDeleteInode] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaSetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.SetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.SetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaSetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.GetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaBatchGetXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.BatchGetXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchGetXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchGetXAttr req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaRemoveXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.RemoveXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.RemoveXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaListXAttr(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.ListXAttrRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ListXAttr(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaGetXAttr] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opMetaBatchExtentsAdd(conn net.Conn, p *Packet, remoteAddr string) (err error) {
	req := &proto.AppendExtentKeysRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.BatchExtentAppend(req, p)
	_ = m.respondToClient(conn, p)
	log.LogDebugf("%s [opMetaBatchExtentsAdd] req: %d - %v, resp: %v, body: %s",
		remoteAddr, p.GetReqID(), req, p.GetResultMsg(), p.Data)
	return
}

func (m *MetadataManager) opCreateMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.CreateMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *MetadataManager) opRemoveMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.RemoveMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.RemoveMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *MetadataManager) opGetMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.GetMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.GetMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}

func (m *MetadataManager) opAppendMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	defer func() {
		if err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		}
		_ = m.respondToClient(conn, p)
	}()
	req := &proto.AddMultipartPartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.AppendMultipart(req, p)
	return
}

func (m *MetadataManager) opListMultipart(conn net.Conn, p *Packet, remote string) (err error) {
	req := &proto.ListMultipartRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionId)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		_ = m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ListMultipart(req, p)
	_ = m.respondToClient(conn, p)
	return
}
