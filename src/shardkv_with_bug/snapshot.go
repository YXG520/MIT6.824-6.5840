package shardkv

import (
	"MIT6.824-6.5840/labgob"
	"bytes"
)

func (kv *ShardKV) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	len := kv.persister.RaftStateSize()
	//DPrintf(10000001, "kv.maxraftstate is %d, and the len of raft log is %d ", kv.maxraftstate, len)

	//return kv.maxraftstate >= len-50 && kv.maxraftstate <= len
	return len >= kv.maxraftstate
}

// 制作快照
func (kv *ShardKV) makeSnapshot(index int) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		DPrintf(11111, "非leader节点，无权限做快照")
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(11111, "是leader，准备编码")

	//e.Encode(kv.lastApplied)
	e.Encode(kv.shardPersist)
	e.Encode(kv.seqMap)
	snapshot := w.Bytes()
	//kv.snapshot = w.Bytes()
	DPrintf(11111, "快照制作完成，准备发送快照")
	// 快照完马上递送给leader节点
	kv.rf.Snapshot(index, snapshot)
	DPrintf(11111, "完成快照发送")
	DPrintf(11111, "print快照数据：")
	//go kv.deliverSnapshot()
}

// 解码快照
func (kv *ShardKV) decodeSnapshot(index int, snapshot []byte) {

	// 这里必须判空，因为当节点第一次启动时，持久化数据为空，如果还强制读取数据会报错
	if snapshot == nil || len(snapshot) < 1 {
		DPrintf(11111, "持久化数据为空！")
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastIncludeIndex = index

	if d.Decode(&kv.shardPersist) != nil || d.Decode(&kv.seqMap) != nil {
		DPrintf(999, "%v: readPersist snapshot decode error\n", kv.rf.SayMeL())
		panic("error in parsing snapshot")
	}
}

// 将快照传递给leader节点，再由leader节点传递给各个从节点
//func (kv *ShardKV) deliverSnapshot() {
//	_, isleader := kv.rf.GetState()
//	if !isleader {
//		return
//	}
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	DPrintf(1111, "准备发送")
//	r := bytes.NewBuffer(kv.snapshot)
//	d := labgob.NewDecoder(r)
//	var lastApplied int
//	if d.Decode(&lastApplied) != nil {
//		DPrintf(999, "%d: state machine reads Snapshot decode error\n", kv.me)
//		panic(" state machine reads Snapshot decode error")
//	}
//	kv.rf.Snapshot(lastApplied, kv.snapshot)
//}
