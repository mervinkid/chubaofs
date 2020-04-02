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
	"bytes"
	"encoding/binary"
	"io"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *MetaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *MetaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	i.IncNLink()
	resp.Msg = i
	return
}

func (mp *MetaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	i.AccessTime = Now.GetCurrentTime().Unix()
	resp.Msg = i
	return
}

func (mp *MetaPartition) hasInode(ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		ok = false
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		ok = false
		return
	}
	ok = true
	return
}

func (mp *MetaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

// Ascend is the wrapper of inodeTree.Ascend
func (mp *MetaPartition) Ascend(f func(i BtreeItem) bool) {
	mp.inodeTree.Ascend(f)
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *MetaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := item.(*Inode)
	if inode.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = inode
	inode.DoWriteFunc(func() {
		inode.ModifyTime = ino.ModifyTime
	})

	if inode.IsEmptyDir() {
		mp.inodeTree.Delete(inode)
	}

	inode.DecNLink()
	return
}

func (mp *MetaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *MetaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}
}

func (mp *MetaPartition) internalDeleteInode(ino *Inode) {
	mp.inodeTree.Delete(ino)
	mp.freeList.Remove(ino.Inode)
	mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
	return
}

func (mp *MetaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	var items []BtreeItem
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := item.(*Inode)
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}
	ino.Extents.Range(func(item BtreeItem) bool {
		items = append(items, item)
		return true
	})
	items = ino2.AppendExtents(items, ino.ModifyTime)
	for _, item := range items {
		log.LogInfof("fsmAppendExtents inode(%v) ext(%v)", ino2.Inode, item.(*proto.ExtentKey))
		mp.extDelCh <- item
	}
	return
}

func (mp *MetaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	var delExtents []BtreeItem
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}
	i.Extents.AscendGreaterOrEqual(&proto.ExtentKey{FileOffset: ino.Size},
		func(item BtreeItem) bool {
			delExtents = append(delExtents, item)
			return true
		})
	i.ExtentsTruncate(delExtents, ino.Size, ino.ModifyTime)
	// now we should delete the extent
	for _, ext := range delExtents {
		log.LogInfof("fsmExtentsTruncate inode(%v) ext(%v)", i.Inode, ext.(*proto.ExtentKey))
		mp.extDelCh <- ext
	}
	return
}

func (mp *MetaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.ShouldDelete() {
		return
	}
	if proto.IsDir(i.Type) {
		if i.IsEmptyDir() {
			i.SetDeleteMark()
		}
		return
	}

	if i.IsTempFile() {
		i.SetDeleteMark()
		mp.freeList.Push(i.Inode)
	}
	return
}

func (mp *MetaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino.Inode)
	}
}

func (mp *MetaPartition) fsmSetAttr(req *proto.SetAttrRequest) (err error) {
	ino := NewInode(req.Inode, req.Mode)
	item := mp.inodeTree.CopyGet(ino)
	if item == nil {
		return
	}
	ino = item.(*Inode)
	if ino.ShouldDelete() {
		return
	}
	ino.SetAttr(req.Valid, req.Mode, req.Uid, req.Gid)
	return
}
