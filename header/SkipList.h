#ifndef LSM_SKIPLIST_H
#define LSM_SKIPLIST_H

#include <fstream>
#include <stack>
#include "BloomFilter.h"
#include "Exception.h"
#include "utils.h"
#include "SSTable.h"
#include <iostream>

using std::stack;
using std::ofstream;
using std::ios;

class Node;
typedef shared_ptr<Node> NodePtr;

template<typename Key, typename Value>
class SkipList {
private:
    class Node {
    public:
        Key key;
        Value value;
        shared_ptr<Node> left, right, down;

    public:
        Node(Key key, Value value, shared_ptr<Node> left, shared_ptr<Node> right, shared_ptr<Node> down): key(key), value(value), left(left), right(right), down(down) {}
        Node(): right(nullptr), down(nullptr) {}
    };

    typedef shared_ptr<Node> NodePtr;

    BloomFilter<Key> bloomFilter;

    NodePtr head;

    Size _size;     // deleted也算是一条记录

    Size _fileSize;

    Key getMinKey();

    Key getMaxKey();

private:
    bool shouldInsertUp();

    // 在替换时计算文件大小的改变
    int computeFileSizeChange(const Value &, const Value &);

    // 在插入时计算文件大小的改变
    int computeFileSizeChange(const Value &);

    NodePtr getNode(const Key &);

    NodePtr getBottomHead();

public:
    SkipList();

    Size size();

    Size fileSize();

    Value *get(const Key &);

    void put(const Key &, const Value &);

    bool del(const Key &);

    void reset();

    SSTablePtr toFile(TimeStamp, const String &);

    bool isEmpty();
};

template<typename Key, typename Value>
SkipList<Key, Value>::SkipList() {
    _size = 0;
    _fileSize = HEADER_SIZE + BLOOM_FILTER_SIZE; // header and bloom filter
    head = make_shared<Node>();
}

template<typename Key, typename Value>
Size SkipList<Key, Value>::size() {
    return _size;
}

template<typename Key, typename Value>
Size SkipList<Key, Value>::fileSize() {
    return _fileSize;
}

// 如果查找不到，返回nullptr
template<typename Key, typename Value>
Value* SkipList<Key, Value>::get(const Key& key) {
    if (!bloomFilter.isProbablyPresent(key)) {
        return nullptr;
    }

    NodePtr node = getNode(key);

    // 如果memTable里是deletionMark，那就是被删了
    if (node) {
        return &node->value;
    }
    return nullptr;
}

template<typename Key, typename Value>
void SkipList<Key, Value>::put(const Key& key, const Value& value) {
    stack<NodePtr> pathStack;
    NodePtr p = head;
    while(p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        pathStack.push(p);
        p = p->down;
    }

    // 替换，_size不变，_fileSize改变
    NodePtr nodeToInsert = pathStack.empty() ? nullptr : pathStack.top()->right;
    if (nodeToInsert && nodeToInsert->key == key) {
        // 修改文件大小， 不用修改filter
        int fileSizeDifference = computeFileSizeChange(nodeToInsert->value, value);
        if (_fileSize + fileSizeDifference > MAX_SSTABLE_SIZE) {
            throw MemTableFull();
        }
        // deletion也要加到filter， 这样才可以找到删除记录
        bloomFilter.put(key);
        _fileSize += fileSizeDifference;

        computeFileSizeChange(nodeToInsert->value, value);
        while (!pathStack.empty()) {
            NodePtr leftToTheReplaced = pathStack.top();
            if (!leftToTheReplaced->right || leftToTheReplaced->right->key != key) {
                return;
            }
            pathStack.pop();
            leftToTheReplaced->right->value = value;
        }
        return;
    }

    // 插入，_size和_fileSize都改变
    int fileSizeDifference = computeFileSizeChange(value);
    if (_fileSize + fileSizeDifference > MAX_SSTABLE_SIZE) {
        throw MemTableFull();
    }
    bloomFilter.put(key);
    ++_size;
    _fileSize += fileSizeDifference;
    // 插入且当前层依然存在，_size和_fileSize都增加
    bool insertUp = true;
    NodePtr downNode = nullptr;
    while(insertUp && !pathStack.empty()) {
        NodePtr insert = pathStack.top();
        pathStack.pop();
        insert->right = make_shared<Node>(key, value, insert, insert->right, downNode); //add新结点
        downNode = insert->right;
        if (downNode->right) {
            downNode->right->left = downNode;
        }
        insertUp = shouldInsertUp();
    }
    // 已经插到顶层，但可能要继续插入
    while (insertUp) {
        //插入新的头结点，加层
        NodePtr oldHead = head;
        head = make_shared<Node>();
        head->right = make_shared<Node>(key, value, head, nullptr, downNode);
        downNode = head->right;
        head->down = oldHead;
        insertUp = shouldInsertUp();
    }
}

template<typename Key, typename Value>
bool SkipList<Key, Value>::del(const Key& key) {
    // 找要被删的最上层结点
    NodePtr topNode = getNode(key);

    // 没有找到，找到被删除的记录也算是没有找到
    if (topNode == nullptr || topNode->value == DELETION_MARK) {
        return false;
    }

    int decrementedFileSize = computeFileSizeChange(topNode->value);
    _fileSize -= decrementedFileSize;
    --_size;

    // 开始向下删除
    NodePtr oldNode;
    while (topNode) {
        oldNode = topNode;
        topNode->left->right = topNode->right;
        if (topNode->right) {
            topNode->right->left = topNode->left;
        }
        topNode = topNode->down;
        oldNode.reset();
    }

    // 删除多余的层
    while (head->down && !head->right) {
        NodePtr oldHead = head;
        head = head->down;
        oldHead.reset();
    }

    return true;
}

template<typename Key, typename Value>
void SkipList<Key, Value>::reset() {
    _size = 0;
    _fileSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    bloomFilter.reset();

    // p points to the start of the level
    NodePtr p = head;
    while (p) {
        // q points to current level
        NodePtr q = p;
        p = p->down;
        while (q) {
            NodePtr oldQ = q;
            q = q->right;
            oldQ.reset();
        }

    }

    head = make_shared<Node>();
}

template<typename Key, typename Value>
bool SkipList<Key, Value>::shouldInsertUp() {
    return rand() & 1;
}

template<typename Key, typename Value>
int SkipList<Key, Value>::computeFileSizeChange(const Value& oldValue, const Value& newValue) {
    // shouldn't overflow, because string is not that long
    return (int)newValue.size() - (int)oldValue.size();
}

template<typename Key, typename Value>
int SkipList<Key, Value>::computeFileSizeChange(const Value &value) {
    // shoud + 1 ?
    return INDEX_SIZE_PER_VALUE + (int)value.size();
}

template<typename Key, typename Value>
typename SkipList<Key, Value>::NodePtr SkipList<Key, Value>::getNode(const Key& key) {
    NodePtr node = head;
    while (node) {
        while (node->right && node->right->key < key) {
            node = node->right;
        }
        if (node->right && key == node->right->key) {
            return node->right;
        }
        node = node->down;
    }

    return nullptr;
}

template<typename Key, typename Value>
SSTablePtr SkipList<Key, Value>::toFile(TimeStamp timeStamp, const String &dir) {
    String level0Path = dir + "/level-0";
    String filePath = level0Path + "/" + std::to_string(timeStamp) + ".sst";

    Key minKey = getMinKey();
    Key maxKey = getMaxKey();

    if (!utils::dirExists(level0Path)) {
        utils::mkdir(level0Path.c_str());
    }

    ofstream ssTable(filePath, ios::out | ios::binary);

    // write header
    ssTable.write((char *)&timeStamp, 8)
           .write((char *)&_size, 8)
           .write((char *)&minKey, 8)
           .write((char *)&maxKey, 8);

    SSTablePtr ssTablePtr = make_shared<SSTable>();
    ssTablePtr->fullPath = filePath;
    ssTablePtr->timeStamp = timeStamp;
    ssTablePtr->numOfKeys = _size;
    ssTablePtr->minKey = minKey;
    ssTablePtr->maxKey = maxKey;

    // write bloom filter
    bloomFilter.toFile(ssTable);
    ssTablePtr->bloomFilter = bloomFilter;


    // offset = header + bloom filter + _size * (key + offset)
    size_t offset = HEADER_SIZE + BLOOM_FILTER_SIZE + _size * INDEX_SIZE_PER_VALUE;
    NodePtr nodeForKey = getBottomHead()->right;
    NodePtr nodeForValue = nodeForKey;

    while (nodeForKey) {
        ssTable.write((char *)&nodeForKey->key, 8)
               .write((char *)&offset, 4);

        ssTablePtr->keys.emplace_back(nodeForKey->key);
        ssTablePtr->offset.emplace_back(offset);

        offset += nodeForKey->value.size();
        nodeForKey = nodeForKey->right;
    }

    while (nodeForValue) {
        Value& value = nodeForValue->value;
        size_t length = value.size();
        const char * str = value.c_str();
        ssTable.write(str, length);
        nodeForValue = nodeForValue->right;
    }

    ssTablePtr->fileSize = offset;

    ssTable.close();

    return ssTablePtr;
}

template<typename Key, typename Value>
Key SkipList<Key, Value>::getMinKey() {
    // delete的key也算进去
    NodePtr nodePtr = getBottomHead();

    nodePtr = nodePtr->right;

    return nodePtr ? nodePtr->key : std::numeric_limits<Key>::quiet_NaN();
}

template<typename Key, typename Value>
Key SkipList<Key, Value>::getMaxKey() {
    // delete的key也算进去，这样合并才能把删除记录合并
    NodePtr nodePtr = head;

    while (nodePtr->down) {
        while (nodePtr->right) {
            nodePtr = nodePtr->right;
        }
        nodePtr = nodePtr->down;
    }

    while (nodePtr->right) {
        nodePtr = nodePtr->right;
    }

    return nodePtr ? nodePtr->key : std::numeric_limits<Key>::quiet_NaN();
}

template<typename Key, typename Value>
typename SkipList<Key, Value>::NodePtr SkipList<Key, Value>::getBottomHead() {
    NodePtr node = head;
    while (node->down) {
        node = node->down;
    }
    return node;
}

template<typename Key, typename Value>
bool SkipList<Key, Value>::isEmpty() {
    return _size == 0;
}

#endif //LSM_SKIPLIST_H
