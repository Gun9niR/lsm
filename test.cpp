#include "kvstore.h"
#include <random>
#include <thread>
#include <ctime>

const int keyNum = 10000;
const int rounds = 4;
std::default_random_engine r(time(nullptr));

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

void testCompaction(KVStore &kv, int valSize, int sec) {
    Key numOfPuts = 0;
    String val = String(valSize, 's');

    auto counter = [&]() {
        cout << "thread begin" << endl;
        int lastSec = 0;
        int currentSec = 0;
        int lastOps = 0;

        while (currentSec < sec) {
            currentSec = clock() / CLOCKS_PER_SEC;
            if (currentSec > lastSec) {
                lastSec = currentSec;
                int currentPuts = numOfPuts;
                int opsThisSec = currentPuts - lastOps;
                lastOps = currentPuts;
                cout << opsThisSec << ", " << std::flush;
            }
        }
    };

    std::thread t(counter);

    while (1) {
        kv.put(r(), val);
        ++numOfPuts;
    }

    t.join();
}

int main() {
    KVStore kv("./data");

    testCompaction(kv, 128, 60);
}
