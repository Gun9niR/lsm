#ifndef LSM_SKIPLIST_H
#define LSM_SKIPLIST_H

#include <memory>
#include <stack>
#include <iostream>

using std::stack;
using std::shared_ptr;
using std::make_shared;

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

    NodePtr head;

    size_t _size;

    size_t _fileSize;

private:
    bool shouldInsertUp();

    // 在替换时计算文件大小的改变
    int computeFileSizeChange(const Value &, const Value &);

    // 在插入时计算文件大小的改变
    int computeFileSizeChange(const Value &);

    NodePtr getNode(const Key &);

public:
    SkipList();

    size_t size();

    size_t fileSize();

    Value *get(const Key &);

    void put(const Key &, const Value &);

    bool del(const Key &);

    void reset();
};

template<typename Key, typename Value>
SkipList<Key, Value>::SkipList() {
    _size = 0;
    _fileSize = 32 + 10240; // header and bloom filter
    head = make_shared<Node>();
}

template<typename Key, typename Value>
size_t SkipList<Key, Value>::size() {
    return _size;
}

template<typename Key, typename Value>
size_t SkipList<Key, Value>::fileSize() {
    return _fileSize;
}

// 如果查找不到，返回nullptr
template<typename Key, typename Value>
Value* SkipList<Key, Value>::get(const Key& key) {
    NodePtr node = getNode(key);
    if (node != nullptr) {
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
        int fileSizeDifference = computeFileSizeChange(nodeToInsert->value, value);
        // todo: 检查是否需要Compaction，需要的话就先compaction
        // 修改文件大小
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

    int fileSizeDifference = computeFileSizeChange(value);
    // todo: 检查是否需要Compaction，需要的话就先compaction
    ++_size;
    _fileSize += fileSizeDifference;
    // 插入且当前层依然存在，_size和_fileSize都增加
    bool insertUp = true;
    NodePtr downNode = nullptr;
    while(insertUp && pathStack.size() > 0) {
        NodePtr insert = pathStack.top();
        pathStack.pop();
        insert->right = make_shared<Node>(key, value, insert, insert->right, downNode); //add新结点
        downNode = insert->right;
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
    // 没有找到
    if (topNode == nullptr) {
        return false;
    }

    // 开始向下删除
    // todo: 插入deletion mark，并修改fileSize
    while (topNode) {
        NodePtr oldNode = topNode;
        topNode->left->right = topNode->right;
        if (topNode->right) {
            topNode->right->left = topNode->left;
        }
        topNode = topNode->down;
        oldNode.reset();
    }
    --_size;

    return true;
}

template<typename Key, typename Value>
void SkipList<Key, Value>::reset() {
    _size = 0;
    _fileSize = HEADER_SIZE + BLOOM_FILTER_SIZE;

    // p points to the start of the level
    NodePtr p = head;
    while (p) {
        // q points to current level
        NodePtr q = p;
        while (q) {
            NodePtr oldQ = q;
            q = q->right;
            oldQ.reset();
        }
        p = p->down;
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
    return INDEX_SIZE_PER_VALUE + (int)value.size() + 1;
}

template<typename Key, typename Value>
shared_ptr<typename SkipList<Key, Value>::Node> SkipList<Key, Value>::getNode(const Key& key) {
    NodePtr node = head;
    while (node != nullptr) {
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

#endif //LSM_SKIPLIST_H
