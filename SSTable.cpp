#include "header/SSTable.h"
#include <fstream>

SSTable::SSTable(const String & fileFullPath) {
    // construct an SSTable by reading from a file
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

    for (int i = 0; i < numOfKeys; ++i) {
        ssTableInFile.read((char *)&keys[i], 8)
                     .read((char *)&offset[i], 4);
    }

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
            size_t length = (idx != numOfKeys - 1) ?
                    offset[idx + 1] - offset[idx] :
                    fileSize - offset[idx];

            shared_ptr<String> ret = make_shared<String>(length, 0);
            ifstream file(fullPath, ios::binary);
            file.seekg(offset[idx]);
            file.read(&(*ret)[0], length);
            return ret;
        }
    }
    return nullptr;
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