// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lmdb

// WARNING: LMDB support is expermental, DO NOT USE IT IN PRODUCTION.

import (
	"bytes"
	"fmt"
	"sync"
	"os"

	"github.com/benbjohnson/clock"
	"github.com/bmatsuo/lmdb-go/lmdb"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/fileutil"
	"github.com/lni/dragonboat/v3/internal/logdb/kv"
	"github.com/lni/dragonboat/v3/internal/vfs"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	lmdblog = logger.GetLogger("lmdbkv")
)

const (
	maxLogFileSize = 1024 * 1024 * 128

	LMDB_BATCH_OP_PUT uint8 = 1
	LMDB_BATCH_OP_DEL uint8 = 2
)

var lmdbBatchItemPool = sync.Pool{
	New: func() interface{} {
		return &lmdbBatchItem{}
	},
}

type lmdbBatchItem struct {
	op uint8
	k []byte
	v []byte
}

var lmdbBatchPool = sync.Pool{
	New: func() interface{} {
		return &lmdbWriteBatch{}
	},
}

type lmdbWriteBatch struct {
	items []*lmdbBatchItem
	env *lmdb.Env
}

func (w *lmdbWriteBatch) Destroy() {
	w.items = []*lmdbBatchItem{}
	lmdbBatchPool.Put(w)
}

func (w *lmdbWriteBatch) Put(key []byte, val []byte) {
	item := lmdbBatchItemPool.Get().(*lmdbBatchItem)
	item.k = append([]byte{}, key...)
	item.v = append([]byte{}, val...)
	item.op = LMDB_BATCH_OP_PUT
	w.items = append(w.items, item)
}

func (w *lmdbWriteBatch) Delete(key []byte) {
	item := lmdbBatchItemPool.Get().(*lmdbBatchItem)
	item.k = append([]byte{}, key...)
	item.op = LMDB_BATCH_OP_DEL
	w.items = append(w.items, item)
}

func (w *lmdbWriteBatch) Clear() {
	for _, item := range w.items {
		lmdbBatchItemPool.Put(item)
	}
	w.items = []*lmdbBatchItem{}
}

func (w *lmdbWriteBatch) Count() int {
	return len(w.items)
}

// NewKVStore returns an LMDB based IKVStore instance.
func NewKVStore(config config.LogDBConfig,
	dir string, wal string, fs vfs.IFS) (kv.IKVStore, error) {
	return openLMDB(config, dir, wal, fs)
}

// KV is an LMDB based IKVStore type.
type KV struct {
	clock clock.Clock
	env   *lmdb.Env
	dbi   lmdb.DBI
	mutex sync.Mutex
}

var _ kv.IKVStore = (*KV)(nil)

var lmdbWarning sync.Once

func openLMDB(config config.LogDBConfig,
	dir string, walDir string, fs vfs.IFS) (store kv.IKVStore, err error) {
	if config.IsEmpty() {
		panic("invalid LogDBConfig")
	}
	if err := fileutil.MkdirAll(dir, fs); err != nil {
		return nil, err
	}
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return
	}
	lmdbWarning.Do(func() {
		lmdblog.Warningf("LMDB support is experimental, DO NOT USE IN PRODUCTION")
	})
	var dbi lmdb.DBI
	env, err := lmdb.NewEnv()
	env.SetMaxDBs(2)
	env.SetMapSize(int64(1 << 39))
	env.Open(dir, 0, 0777)
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("main", lmdb.Create)
		return
	})
	if err != nil {
		return
	}
	return &KV{
		clock: clock.New(),
		env: env,
		dbi: dbi,
	}, nil
}

// Name returns the IKVStore type name.
func (r *KV) Name() string {
	return "lmdb"
}

// Close closes the RDB object.
func (r *KV) Close() error {
	if r.env != nil {
		r.env.Close()
	}
	r.env = nil
	return nil
}

// IterateValue ...
func (r *KV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) (err error) {
	err = r.env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(r.dbi)
		if err != nil {
			return
		}
		k, v, err := cur.Get(fk, nil, lmdb.SetRange)
		if err != nil {
			if lmdb.IsNotFound(err) {
				err = nil
			}
			return
		}
		for len(k) > 0 && bytes.Compare(k, lk) < 0 || inc && bytes.Compare(k, lk) == 0 {
			cont, err := op(k, v)
			if err != nil {
				return err
			}
			if !cont {
				break
			}
			k, v, err = cur.Get(nil, nil, lmdb.Next)
			if err != nil {
				if lmdb.IsNotFound(err) {
					err = nil
				}
				return err
			}
		}
		return nil
	})
	return
}

// GetValue ...
func (r *KV) GetValue(key []byte, op func([]byte) error) (err error) {
	var val []byte
	err = r.env.Update(func(txn *lmdb.Txn) (err error) {
		val, err = txn.Get(r.dbi, key)
		return
	})
	if err != nil && !lmdb.IsNotFound(err) {
		return
	}
	return op(val)
}

// SaveValue ...
func (r *KV) SaveValue(k []byte, v []byte) error {
	return r.env.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Put(r.dbi, k, v, 0)
		return
	})
}

// DeleteValue ...
func (r *KV) DeleteValue(k []byte) error {
	return r.env.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Del(r.dbi, k, nil)
		if lmdb.IsNotFound(err) {
			err = nil
		}
		return
	})
}

// GetWriteBatch ...
func (r *KV) GetWriteBatch(ctx raftio.IContext) kv.IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			lwb := wb.(*lmdbWriteBatch)
			if lwb.env == r.env {
				return lwb
			}
		}
	}
	return lmdbBatchPool.Get().(*lmdbWriteBatch)
}

// CommitWriteBatch ...
func (r *KV) CommitWriteBatch(wb kv.IWriteBatch) (err error) {
	lwb, ok := wb.(*lmdbWriteBatch)
	if !ok {
		panic("unknown type")
	}
	err = r.env.Update(func(txn *lmdb.Txn) (err error) {
		for _, item := range lwb.items {
			switch item.op {
			case LMDB_BATCH_OP_PUT:
				err = txn.Put(r.dbi, item.k, item.v, 0)
				if err != nil && !lmdb.IsNotFound(err) {
					return
				}
			case LMDB_BATCH_OP_DEL:
				err = txn.Del(r.dbi, item.k, nil)
				if err != nil && !lmdb.IsNotFound(err) {
					return
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("--- err: %s", err.Error())
	}
	return
}

// BulkRemoveEntries ...
func (r *KV) BulkRemoveEntries(fk []byte, lk []byte) (err error) {
	err = r.env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(r.dbi)
		if err != nil {
			return
		}
		k, _, err := cur.Get(fk, nil, lmdb.SetRange)
		if err != nil && !lmdb.IsNotFound(err) {
			return
		}
		for bytes.Compare(k, lk) < 0 {
			err = cur.Del(0)
			if err != nil {
				return
			}
			k, _, err = cur.Get(nil, nil, lmdb.Next)
			if err != nil {
				if lmdb.IsNotFound(err) {
					err = nil
				}
				return
			}
		}
		return nil
	})
	return
}

// CompactEntries ...
func (r *KV) CompactEntries(fk []byte, lk []byte) (err error) {
	return
}

// FullCompaction ...
func (r *KV) FullCompaction() (err error) {
	fk := make([]byte, kv.MaxKeyLength)
	lk := make([]byte, kv.MaxKeyLength)
	for i := uint64(0); i < kv.MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	return
}
