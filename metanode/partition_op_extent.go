// Copyright 2018 The Chubao Authors.
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
	"encoding/json"

	"os"

	"github.com/chubaofs/chubaofs/proto"
)

// ExtentAppend appends an extent.
func (mp *MetaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ext := req.Extent
	ino.Extents.Append(&ext)
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}

// ExtentsList returns the list of extents.
func (mp *MetaPartition) ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	retMsg := mp.getInode(ino)
	ino = retMsg.Msg
	var (
		reply  []byte
		status = retMsg.Status
	)
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{}
		ino.DoReadFunc(func() {
			resp.Generation = ino.Generation
			resp.Size = ino.Size
			ino.Extents.Range(func(item BtreeItem) bool {
				ext := item.(*proto.ExtentKey)
				resp.Extents = append(resp.Extents, *ext)
				return true
			})
		})
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PacketErrorWithBody(status, reply)
	return
}

// ExtentsTruncate truncates an extent.
func (mp *MetaPartition) ExtentsTruncate(req *proto.TruncateRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, proto.Mode(os.ModePerm))
	ino.Size = req.Size
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.submit(opFSMExtentTruncate, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	msg := resp.(*InodeResponse)
	p.PacketErrorWithBody(msg.Status, nil)
	return
}

func (mp *MetaPartition) BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	extents := req.Extents
	for _, extent := range extents {
		ino.Extents.Append(&proto.ExtentKey{
			FileOffset:   extent.FileOffset,
			PartitionId:  extent.PartitionId,
			ExtentId:     extent.ExtentId,
			ExtentOffset: extent.ExtentOffset,
			Size:         extent.Size,
			CRC:          extent.CRC,
		})
	}
	val, err := ino.Marshal()
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.submit(opFSMExtentsAdd, val)
	if err != nil {
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		return
	}
	p.PacketErrorWithBody(resp.(uint8), nil)
	return
}
