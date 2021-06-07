#include "kvstore.h"
#include <random>

const int keyNum = 10000;
const int rounds = 4;

void test_p_g_d(KVStore &kv, int valSize) {
    size_t putTotal = 0;
    size_t getTotal = 0;
    size_t delTotal = 0;

    double averageDelay;
    double throughPut;

    cout << "%%%%%%%%%% Value Size = " << valSize << " %%%%%%%%%%" << endl;
    for (int T = 0; T < rounds; ++T) {
        cout << "========== Round " << T + 1 << " ==========" << endl;
        String val = String(valSize, 's');
        vector<int> keys(keyNum);
        for (int i = 0; i < keyNum; ++i) {
            keys[i] = i;
        }
        clock_t startTime, endTime;
        size_t totalTime = 0;

        totalTime = 0;
        shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < keyNum; ++i) {
            startTime = clock();
            kv.put(keys[i], val);
            endTime = clock();
            totalTime += endTime - startTime;
        }
        putTotal += totalTime;

        totalTime = 0;
        shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < keyNum; ++i) {
            startTime = clock();
            kv.get(keys[i]);
            endTime = clock();
            totalTime += endTime - startTime;
        }
        getTotal += totalTime;

        totalTime = 0;
        shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < keyNum; ++i) {
            startTime = clock();
            kv.del(keys[i]);
            endTime = clock();
            totalTime += endTime - startTime;
        }
        delTotal += totalTime;

        kv.reset();
    }

    averageDelay = (double) putTotal / keyNum / CLOCKS_PER_SEC / rounds;
    throughPut = 1 / (averageDelay);
    cout << "<PUT> Average delay: " << averageDelay <<"s\t" << "Throughput: " <<
         throughPut << endl;

    averageDelay = (double) getTotal / keyNum / CLOCKS_PER_SEC / rounds;
    throughPut = 1 / (averageDelay);
    cout << "<GET> Average delay: " << averageDelay <<"s\t" << "Throughput: " <<
         throughPut << endl;

    averageDelay = (double) delTotal / keyNum / CLOCKS_PER_SEC / rounds;
    throughPut = 1 / (averageDelay);
    cout << "<DEL> Average delay: " << averageDelay <<"s\t" << "Throughput: " <<
         throughPut << endl;
}

void test_p_g(KVStore &kv, int valSize) {
    size_t getTotal = 0;

    for (int T = 0; T < rounds; ++T) {
        cout << "========== Round " << T + 1 << " ==========" << endl;
        String val = String(valSize, 's');
        vector<int> keys(keyNum);
        for (int i = 0; i < keyNum; ++i) {
            keys[i] = i;
        }
        clock_t startTime, endTime;
        size_t totalTime;

        shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < keyNum; ++i) {
            kv.put(keys[i], val);
        }

        totalTime = 0;
        shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < keyNum; ++i) {
            startTime = clock();
            kv.get(keys[i]);
            endTime = clock();
            totalTime += endTime - startTime;
        }
        getTotal += totalTime;

        kv.reset();
    }

    cout << "<GET> Average delay: " << (double) getTotal / keyNum / CLOCKS_PER_SEC / rounds <<"s\t" << endl;
}

int main() {
    KVStore kv("./data");

//    // 值大小为50
//    test_p_g_d(kv, 50);
//    // 值大小为500
//    test_p_g_d(kv, 500);
//    // 值大小为5000
//    test_p_g_d(kv ,5000);
//    // 值大小为50000
//    test_p_g_d(kv, 50000);
//    // 值大小为500000
//    test_p_g_d(kv, 500000);
    test_p_g(kv, 5000);
}
