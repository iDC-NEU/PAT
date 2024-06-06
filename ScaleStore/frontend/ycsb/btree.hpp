#pragma once

#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <sched.h>

namespace btreeolc {

enum class PageType : uint8_t { BTreeInner=1, BTreeLeaf=2 };

static const uint64_t pageSize=4*1024;

struct OptLock {
   std::atomic<uint64_t> typeVersionLockObsolete{0b100}; // 4

   bool isLocked(uint64_t version) {
      return ((version & 0b10) == 0b10); // version = 10/11 均表示locked
   }

   uint64_t readLockOrRestart(bool &needRestart) {  // 尝试读锁
      uint64_t version;
      version = typeVersionLockObsolete.load();
      if (isLocked(version) || isObsolete(version)) {
         _mm_pause();
         needRestart = true;
      }
      return version;
   }

   void writeLockOrRestart(bool &needRestart) { // 尝试写锁
      uint64_t version;
      version = readLockOrRestart(needRestart);
      if (needRestart) return;

      upgradeToWriteLockOrRestart(version, needRestart);
      if (needRestart) return;
   }

   void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) { // 升级到写锁
      if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) { // 若typeVersion == version,则typeVersion更新为version+0b10,否则更新为version
         version = version + 0b10;
      } else {
         _mm_pause();
         needRestart = true;
      }
   }

   void writeUnlock() { // 解写锁，version+2
      typeVersionLockObsolete.fetch_add(0b10);
   }

   bool isObsolete(uint64_t version) { // 过时的version
      return (version & 1) == 1;
   }

   void checkOrRestart(uint64_t startRead, bool &needRestart) const {
      readUnlockOrRestart(startRead, needRestart);
   }

   void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
      needRestart = (startRead != typeVersionLockObsolete.load());
   }
 
   void writeUnlockObsolete() { // 解写锁，过时的version，version+3
      typeVersionLockObsolete.fetch_add(0b11);
   }
};

struct NodeBase : public OptLock{
   PageType type;
   uint16_t count;
};

struct BTreeLeafBase : public NodeBase {
   static const PageType typeMarker=PageType::BTreeLeaf;
};

template<class Key,class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   struct Entry {
      Key k;
      Payload p;
   };

   static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(Payload));

   Key keys[maxEntries];
   Payload payloads[maxEntries];
   int partition_id = -1;

   BTreeLeaf() {
      count=0;
      type=typeMarker;
   }

   bool isFull() { return count==maxEntries; };

   unsigned lowerBound(Key k) {
      unsigned lower=0;
      unsigned upper=count;
      do {
         unsigned mid=((upper-lower)/2)+lower;
         if (k<keys[mid]) {
            upper=mid;
         } else if (k>keys[mid]) {
            lower=mid+1;
         } else {
            return mid;
         }
      } while (lower<upper);
      return lower;
   }

   unsigned lowerBoundBF(Key k) {
      auto base=keys;
      unsigned n=count;
      while (n>1) {
         const unsigned half=n/2;
         base=(base[half]<k)?(base+half):base;
         n-=half;
      }
      return (*base<k)+base-keys;
   }

   void insert(Key k,Payload p) {
      assert(count<maxEntries);
      if (count) {
         unsigned pos=lowerBound(k);
         if ((pos<count) && (keys[pos]==k)) {
            // Upsert
            payloads[pos] = p;
            return;
         }
         memmove(keys+pos+1,keys+pos,sizeof(Key)*(count-pos));
         memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
         keys[pos]=k;
         payloads[pos]=p;
      } else {
         keys[0]=k;
         payloads[0]=p;
      }
      count++;
   }

   BTreeLeaf* split(Key& sep) {
      BTreeLeaf* newLeaf = new BTreeLeaf();
      newLeaf->count = count-(count/2);
      count = count-newLeaf->count;
      memcpy(newLeaf->keys, keys+count, sizeof(Key)*newLeaf->count);
      memcpy(newLeaf->payloads, payloads+count, sizeof(Payload)*newLeaf->count);
      sep = keys[count-1];
      return newLeaf;
   }

   BTreeLeaf* split(Key &sep, uint16_t pos, int r_partition_id) {
      BTreeLeaf* newLeaf = new BTreeLeaf();
      newLeaf->count = count - pos;
      ensure(newLeaf->count < count && newLeaf->count > 0);
      newLeaf->partition_id = r_partition_id;
      count = count - newLeaf->count;
      memcpy(newLeaf->keys, keys + count, sizeof(Key)*newLeaf->count);
      memcpy(newLeaf->payloads, payloads + count, sizeof(Payload)*newLeaf->count);
      sep = keys[count - 1];
      return newLeaf;
   }

   BTreeLeaf* split(Key &sep, uint16_t pos, int l_partition_id, int r_partition_id) {
      partition_id = l_partition_id;
      BTreeLeaf* newLeaf = new BTreeLeaf();
      newLeaf->count = count - pos;
      ensure(newLeaf->count < count && newLeaf->count > 0);
      newLeaf->partition_id = r_partition_id;
      count = count - newLeaf->count;
      memcpy(newLeaf->keys, keys + count, sizeof(Key)*newLeaf->count);
      memcpy(newLeaf->payloads, payloads + count, sizeof(Payload)*newLeaf->count);
      sep = keys[count - 1];
      return newLeaf;
   }
};

struct BTreeInnerBase : public NodeBase {
   static const PageType typeMarker=PageType::BTreeInner;
};

template<class Key>
struct BTreeInner : public BTreeInnerBase {
   static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(NodeBase*));
   
   NodeBase* children[maxEntries];
   Key keys[maxEntries];

   BTreeInner() {
      count=0;
      type=typeMarker;
   }

   bool isFull() { return count==(maxEntries-1); };

   unsigned lowerBoundBF(Key k) {
      auto base=keys;
      unsigned n=count;
      while (n>1) {
         const unsigned half=n/2;
         base=(base[half]<k)?(base+half):base;
         n-=half;
      }
      return (*base<k)+base-keys;
   }

   unsigned lowerBound(Key k) {
      unsigned lower=0;
      unsigned upper=count;
      do {
         unsigned mid=((upper-lower)/2)+lower;
         if (k<keys[mid]) {
            upper=mid;
         } else if (k>keys[mid]) {
            lower=mid+1;
         } else {
            return mid;
         }
      } while (lower<upper);
      return lower;
   }

   BTreeInner* split(Key& sep) {
      BTreeInner* newInner=new BTreeInner();
      newInner->count=count-(count/2);
      count=count-newInner->count-1;
      sep=keys[count];
      memcpy(newInner->keys,keys+count+1,sizeof(Key)*(newInner->count+1));
      memcpy(newInner->children,children+count+1,sizeof(NodeBase*)*(newInner->count+1));
      return newInner;
   }

   void insert(Key k,NodeBase* child) {
      assert(count<maxEntries-1);
      unsigned pos=lowerBound(k);
      memmove(keys+pos+1,keys+pos,sizeof(Key)*(count-pos+1));
      memmove(children+pos+1,children+pos,sizeof(NodeBase*)*(count-pos+1));
      keys[pos]=k;
      children[pos]=child;
      std::swap(children[pos],children[pos+1]);
      count++;
   }

   bool remove(uint64_t pos) {
      if (count) {
         if (pos < count) {
            memmove(keys + pos, keys + pos + 1, sizeof(Key) * (count - (pos + 1)));
            memmove(children + pos + 1, children + pos + 2, sizeof(NodeBase *) * (count - (pos + 2) + 1));
            count--;
            return true;
         }
      }
      return false;
   }

};

template<class Key,class Value>
struct BTree {
   std::atomic<NodeBase*> root;
   int32_t increase_count = 0;

   BTree() {
      root = new BTreeLeaf<Key,Value>();
   }

   void makeRoot(Key k,NodeBase* leftChild,NodeBase* rightChild) {
      auto inner = new BTreeInner<Key>();
      inner->count = 1;
      inner->keys[0] = k;
      inner->children[0] = leftChild;
      inner->children[1] = rightChild;
      root = inner;
   }

   void yield(int count) {
      // if (count>3)
      //    sched_yield();
      // else
      (void) count;
         _mm_pause();
   }


   // void update_metis_index(Key start_k, Key end_k, int partition_id) {
   //    int restartCount = 0;
   // restart:
   //    if (restartCount++) 
   //       yield(restartCount);
   //    bool needRestart = false;

   //    // Current node
   //    NodeBase* node = root;
   //    uint64_t versionNode = node->readLockOrRestart(needRestart); // 根节点加读锁
   //    if (needRestart || (node != root)) goto restart;

   //    // Parent
   //    BTreeInner<Key>* parent = nullptr;
   //    uint64_t versionParent = 0;
   //    uint16_t pos = -1;
      
   //    while (node->type == PageType::BTreeInner) { // 开始向下查找
   //       auto inner = static_cast<BTreeInner<Key>*>(node);

   //       if(inner->isFull()) { // 当前节点满，分裂
   //          if (parent) {
   //             parent->upgradeToWriteLockOrRestart(versionParent, needRestart); // 父节点升级写锁
   //             if (needRestart) goto restart;
   //          }
   //          node->upgradeToWriteLockOrRestart(versionNode, needRestart); // 当前节点升级写锁
   //          if (needRestart) { // 先父节点解锁，再restart
   //             if (parent)
   //                parent->writeUnlock();
   //             goto restart;
   //          }
   //          if (!parent && (node != root)) { // 当前节点有新的父节点
   //             node->writeUnlock();
   //             goto restart;
   //          }
   //          // Split
   //          Key sep; 
   //          BTreeInner<Key>* newInner = inner->split(sep);
   //          if (parent)
   //             parent->insert(sep,newInner);
   //          else
   //             makeRoot(sep,inner,newInner);
   //          // Unlock and restart
   //          node->writeUnlock();
   //          if (parent)
   //             parent->writeUnlock();
   //          goto restart;
   //       }
   //       // 不满，下一层
   //       if (parent) {
   //          parent->readUnlockOrRestart(versionParent, needRestart);
   //          if (needRestart) goto restart;
   //       }

   //       parent = node;
   //       versionParent = versionNode;

   //       node = inner->children[inner->lowerBound(start_k)];
   //       inner->checkOrRestart(versionNode, needRestart);
   //       if (needRestart) goto restart;
   //       versionNode = node->readLockOrRestart(needRestart);
   //       if (needRestart) goto restart;
   //    }
   // // ---------------------------
   // // lock
   // if (parent) {
   //    parent->upgradeToWriteLockOrRestart(versionParent, needRestart); // 父节点 独占锁
   //    if (needRestart) goto restart;
   // }
   // //------------------------------
   // leaf_restart:
   //    if (start_k > end_k || start_k == end_k) {  // 此时父节点写锁，当前节点读锁
   //       if (parent) 
   //          parent->writeUnlock();
   //       node->readUnlockOrRestart(versionNode, needRestart);
   //       if (needRestart) goto restart;
   //       return;
   //    }

   //    // leaf
   //    auto leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
   //    node->upgradeToWriteLockOrRestart(versionNode, needRestart); // 当前节点升级写锁
   //    if (needRestart) {
   //       if (parent) 
   //          parent->writeUnlock();
   //       goto restart;
   //    }
   //    if (!parent && (node != root)) {
   //       node->writeUnlock();
   //       goto restart;
   //    }

   //    // if (node->type != PageType::BTreeLeaf) {
   //    //    std::cout <<"not leaf!";
   //    //    std::cout <<start_k;
   //    //    std::cout <<end_k;
   //    //    parent->writeUnlock();
   //    //    node->readUnlockOrRestart(versionNode, needRestart);
   //    //    goto restart;
   //    // }
   //    // ensure(node->type == PageType::BTreeLeaf);

   //    uint64_t l_pos = leaf->lowerBound(start_k);
   //    uint64_t r_pos = leaf->lowerBound(end_k);
   //    if (r_pos == 0) {
   //       if (parent)
   //          parent->writeUnlock();
   //       node->writeUnlock();
   //       return ;
   //    }
   //    if (r_pos == l_pos && !(leaf->keys[l_pos] == start_k)) {
   //       if (parent) 
   //          parent->writeUnlock();
   //       node->writeUnlock();
   //       return ;
   //    }

   //    if (l_pos < leaf->count) { 
   //       if (l_pos > 0) { // 当前节点中，l_pos之前的部分不属于当前分区，分裂出去
   //          Key sep;
   //          BTreeLeaf<Key,Value>* newLeaf = leaf->split(sep, l_pos, partition_id);
   //          increase_count++;
   //          if (parent)
   //             parent->insert(sep,newLeaf);
   //          else 
   //             makeRoot(sep,leaf,newLeaf);
   //          if (parent)
   //             parent->writeUnlock();
   //          node->writeUnlock();
   //          goto restart;
   //       }
   //       else { // start_k <= keys[0]
   //          if (r_pos == leaf->count) { // keys[count - 1] <= end_k,该节点中均为该分区的数据，尝试向左合并
   //             leaf->partition_id = partition_id;
   //             if (!parent) {
   //                node->writeUnlock();
   //                return;
   //             }
   //             start_k = leaf->keys[r_pos - 1]; // start_k == keys[count - 1]
   //             start_k++; // start_k = keys[count - 1] + 1 
   //             if (parent->count > 2 && (pos - 1) >= 0 && pos < parent->count) { // 合并
   //                BTreeLeaf<Key,Value>* left = parent->children[pos - 1]; // 当前节点的左节点
   //                ensure(!(left == leaf));
   //                left->writeLockOrRestart(needRestart); // 左节点加写锁
   //                if (needRestart) {
   //                   if (parent)
   //                      parent->writeUnlock();
   //                   node->writeUnlock();
   //                   goto restart;
   //                }
   //                if (left->count + leaf->count < Leaf::maxEntries - 1 && leaf->partition_id == left->partition_id) { // left <- leaf
   //                   memcpy(left->keys + left->count, leaf->keys, sizeof(Key) * leaf->count);
   //                   memcpy(left->payloads + left->count, leaf->payloads, sizeof(Value) * leaf->count);
   //                   left->count += leaf->count;
   //                   parent->remove(pos - 1); 
   //                   // xg_node.reclaim(); // 是否需要回收node？
   //                   pos = pos - 1;
   //                   increase_count--;
   //                }
   //                left->writeUnlock();
   //             }
   //             pos = pos + 1;
   //             if (pos >= parent->count || pos == 0) {
   //                if (parent)
   //                   parent->writeUnlock();
   //                node->writeUnlock();
   //                goto restart;
   //             }
   //             node->writeUnlock();
   //             node = parent->children[pos]; // next node
   //             versionNode = node->readLockOrRestart(needRestart); // 读锁
   //             if (needRestart) {
   //                if (parent)
   //                   parent->writeUnlock();
   //                goto restart;
   //             }
   //             goto leaf_restart;
   //          }
   //          else { // r_pos之后的部分不属于当前分区，分裂出去
   //             Key sep;
   //             BTreeLeaf<Key,Value>* newLeaf = leaf->split(sep, r_pos, partition_id, -1);
   //             increase_count++;
   //             if (!parent) {
   //                makeRoot(sep, leaf, newLeaf);
   //                node->writeUnlock();
   //                return ;
   //             }
   //             else {
   //                parent->insert(sep, newLeaf);
   //                bool needMerge = false; // 判断node是否被合并
   //                if ((pos - 1) > 0 && pos < parent->count) { // 分裂后，当前节点尝试向左合并
   //                   BTreeLeaf<Key,Value>* left = parent->children[pos - 1]; // 当前节点的左节点
   //                   ensure(!(left == leaf));
   //                   left->writeLockOrRestart(needRestart);
   //                   if (needRestart) {
   //                      if (parent)
   //                         parent->writeUnlock();
   //                      node->writeUnlock();
   //                      goto restart;
   //                   }
   //                   if (left->count + leaf->count < Leaf::maxEntries - 1 && left->partition_id == leaf->partition_id) {
   //                      needMerge = true;
   //                      memcpy(left->keys + left->count, leaf->keys, sizeof(Key) * leaf->count);
   //                      memcpy(left->payloads + left->count, leaf->payloads, sizeof(Value) * leaf->count);
   //                      left->count += leaf->count;
   //                      parent->remove(pos - 1);
   //                      // delete leaf;
   //                      // xg_node.reclaim(); // 是否需要回收node？
   //                      increase_count--;
   //                      // node->writeUnlock(); // 被合并后的leaf还需要解写锁吗?
   //                   }
   //                   left->writeUnlock();
   //                }
   //                parent->writeUnlock();
   //                if (!needMerge)
   //                   node->writeUnlock(); 
   //                return ;
   //             }
   //             // return ; // 当end_k处理完成即结束 此处return提到if和else中
   //          }
   //       }
   //    }
   // }

   void insert(Key k, Value v) {
      int restartCount = 0;
   restart:
      if (restartCount++)
         yield(restartCount);
      bool needRestart = false;

      // Current node
      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node!=root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type==PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         // Split eagerly if full
         if (inner->isFull()) {
            // Lock
            if (parent) {
               parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
               if (needRestart) goto restart;
            }
            node->upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart) {
               if (parent)
                  parent->writeUnlock();
               goto restart;
            }
            if (!parent && (node != root)) { // there's a new parent
               node->writeUnlock();
               goto restart;
            }
            // Split
            Key sep; 
            BTreeInner<Key>* newInner = inner->split(sep);
            if (parent)
               parent->insert(sep,newInner);
            else
               makeRoot(sep,inner,newInner);
            // Unlock and restart
            node->writeUnlock();
            if (parent)
               parent->writeUnlock();
            goto restart;
         }

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }

      auto leaf = static_cast<BTreeLeaf<Key,Value>*>(node);

      // Split leaf if full
      if (leaf->count==leaf->maxEntries) {
         // Lock
         if (parent) {
            parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }
         node->upgradeToWriteLockOrRestart(versionNode, needRestart);
         if (needRestart) {
            if (parent) parent->writeUnlock();
            goto restart;
         }
         if (!parent && (node != root)) { // there's a new parent
            node->writeUnlock();
            goto restart;
         }
         // Split
         Key sep; BTreeLeaf<Key,Value>* newLeaf = leaf->split(sep);
         if (parent)
            parent->insert(sep, newLeaf);
         else
            makeRoot(sep, leaf, newLeaf);
         // Unlock and restart
         node->writeUnlock();
         if (parent)
            parent->writeUnlock();
         goto restart;
      } else {
         // only lock leaf node
         node->upgradeToWriteLockOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) {
               node->writeUnlock();
               goto restart;
            }
         }
         leaf->insert(k, v);
         node->writeUnlock();
         return; // success
      }
   }

   bool lookup(Key k, Value& result) {
      int restartCount = 0;
   restart:
      if (restartCount++)
         yield(restartCount);
      bool needRestart = false;

      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node!=root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type==PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }

      BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
      unsigned pos = leaf->lowerBound(k);
      bool success = false;
      if ((pos<leaf->count) && (leaf->keys[pos]==k)) {
         success = true;
         result = leaf->payloads[pos];
      }
      if (parent) {
         parent->readUnlockOrRestart(versionParent, needRestart);
         if (needRestart) goto restart;
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      return success;
   }

   uint64_t scan(Key k, int range, Value* output) {
      int restartCount = 0;
   restart:
      if (restartCount++)
         yield(restartCount);
      bool needRestart = false;

      NodeBase* node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node!=root)) goto restart;

      // Parent of current node
      BTreeInner<Key>* parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type==PageType::BTreeInner) {
         auto inner = static_cast<BTreeInner<Key>*>(node);

         if (parent) {
            parent->readUnlockOrRestart(versionParent, needRestart);
            if (needRestart) goto restart;
         }

         parent = inner;
         versionParent = versionNode;

         node = inner->children[inner->lowerBound(k)];
         inner->checkOrRestart(versionNode, needRestart);
         if (needRestart) goto restart;
         versionNode = node->readLockOrRestart(needRestart);
         if (needRestart) goto restart;
      }

      BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
      unsigned pos = leaf->lowerBound(k);
      int count = 0;
      for (unsigned i=pos; i<leaf->count; i++) {
         if (count==range)
            break;
         output[count++] = leaf->payloads[i];
      }

      if (parent) {
         parent->readUnlockOrRestart(versionParent, needRestart);
         if (needRestart) goto restart;
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;

      return count;
   }


};

}
