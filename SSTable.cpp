#include "header/SSTable.h"
#include <fstream>

SSTable::SSTable(): minKey(std::numeric_limits<Key>::max()), maxKey(std::numeric_limits<Key>::min())
{ }

/*
 * construct an SSTable by reading from a file
 */
SSTable::SSTable(const String & fileFullPath) {
    fullPath = fileFullPath;
    ifstream ssTableInFile(fileFullPath, ios::binary);

    // read header
    ssTableInFile.read((char *)&timeStamp, 8)
                 .read((char *)&numOfKeys, 8)
                 .read((char *)&minKey, 8)
                 .read((char *)&maxKey, 8);

    keys.resize(numOfKeys);
    offset.resize(numOfKeys);

    bloomFilter.fromFile(ssTableInFile);

    for (Size i = 0; i < numOfKeys; ++i) {
        ssTableInFile.read((char *)&keys[i], 8)
                     .read((char *)&offset[i], 4);
    }

    ssTableInFile.seekg (0, ssTableInFile.end);
    fileSize = ssTableInFile.tellg();

    ssTableInFile.close();
}

ostream& operator<<(ostream & ostream, const SSTable &ssTable) {
    ostream << "Path: " << ssTable.fullPath << endl
            << "Timestamp: " << ssTable.timeStamp << endl
            << "Number of keys: " << ssTable.numOfKeys << endl
            << "Min Key: " << ssTable.minKey << endl
            << "Max Key: " << ssTable.maxKey << endl
            << "Keys: ";

    for (const Key& key: ssTable.keys) {
        ostream << key << " ";
    }
    ostream << endl;
    return ostream;
}

bool SSTable::isProbablyPresent(const Key& key) const {
    return bloomFilter.isProbablyPresent(key);
}

// 为了提高效率返回指针，如果找不到返回nullptr
shared_ptr<String> SSTable::get(const Key& key) const {
    if (key >= minKey && key <= maxKey && isProbablyPresent(key)) {
        // 二分查找，找到了不管是不是删除标记都返回指针
        size_t idx = binarySearch(key);

        if (idx != std::numeric_limits<size_t>::max()) {
            // 从文件中读取value
            return get(idx);
        }
    }
    return nullptr;
}

shared_ptr<String> SSTable::get(size_t idx) const {
    size_t length = (idx != numOfKeys - 1) ?
                    offset[idx + 1] - offset[idx] :
                    fileSize - offset[idx];

    shared_ptr<String> ret = make_shared<String>(length, 0);
    ifstream file(fullPath, ios::binary);

    file.seekg(offset[idx]);
    file.read(&(*ret)[0], length);

    file.close();

    return ret;
}

size_t SSTable::binarySearch(const Key& key) const {
    long left = 0;
    long right = keys.size() - 1;
    while (left <= right) {
        long mid = (left + right) >> 1;
        Key k = keys[mid];
        if (k == key) {
            return mid;
        } else if (k < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return std::numeric_limits<size_t>::max();
}

bool SSTable::contains(const Key& key) const {
    return minKey <= key && maxKey >= key;
}

Key SSTable::getMaxKey() const {
    return maxKey;
}

Key SSTable::getMinKey() const {
    return minKey;
}

/* when calling this function, every field is set */

void SSTable::toFile(vector<shared_ptr<String>>& values) {
    ofstream file(fullPath, ios::out | ios::binary);

    file.write((char *)&timeStamp, 8)
        .write((char *)&numOfKeys, 8)
        .write((char *)&minKey, 8)
        .write((char *)&maxKey, 8);

    bloomFilter.toFile(file);

    for (int i = 0; i < numOfKeys; ++i) {
        file.write((char *)&keys[i], 8)
            .write((char *)&offset[i], 4);
    }

    for (int i = 0; i < numOfKeys; ++i) {
        file.write(values[i]->c_str(), values[i]->size());
    }
    file.close();
}

shared_ptr<vector<StringPtr>> SSTable::getAllValues() const {
    shared_ptr<vector<StringPtr>> ret = make_shared<vector<StringPtr>>();
    ret->reserve(numOfKeys);

    ifstream file(fullPath, ios::binary);

    file.seekg(offset[0]);

    for (int i = 0; i < numOfKeys - 1; ++i) {
        size_t length = offset[i + 1] - offset[i];
        StringPtr value = make_shared<String>(length, 0);
        file.read(&(*value)[0], length);
        ret->emplace_back(value);
    }

    size_t length = fileSize - offset[numOfKeys - 1];
    StringPtr value = make_shared<String>(length, 0);
    file.read(&(*value)[0], length);
    ret->emplace_back(value);
    file.close();

    return ret;
}
