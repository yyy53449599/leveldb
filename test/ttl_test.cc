

#include "gtest/gtest.h"

#include "leveldb/env.h"
#include "leveldb/db.h"


using namespace leveldb;

constexpr int value_size = 2048;
constexpr int data_size = 128 << 20;

Status OpenDB(std::string dbName, DB **db) {
  Options options;
  options.create_if_missing = true;
  return DB::Open(options, dbName, db);
}

void InsertData(DB *db, uint64_t ttl/* second */) {
  WriteOptions writeOptions;
  int key_num = data_size / value_size;
  srand(static_cast<unsigned int>(time(0)));

  for (int i = 0; i < key_num; i++) {
    int key_ = rand() % key_num+1;
    std::string key = std::to_string(key_);
    std::string value(value_size, 'a');
    db->Put(writeOptions, key, value, ttl);
  }
}

void GetData(DB *db, int size = (1 << 30)) {
  ReadOptions readOptions;
  int key_num = data_size / value_size;
  
  // 点查
  srand(static_cast<unsigned int>(time(0)));
  for (int i = 0; i < 100; i++) {
    int key_ = rand() % key_num+1;
    std::string key = std::to_string(key_);
    std::string value;
    db->Get(readOptions, key, &value);
  }
}

TEST(TestTTL, ReadTTL) {
    DB *db;
    if(OpenDB("testdb", &db).ok() == false) {
        std::cerr << "open db failed" << std::endl;
        abort();
    }

    uint64_t ttl = 20;

    InsertData(db, ttl);

    ReadOptions readOptions;
    Status status;
    int key_num = data_size / value_size;
    srand(static_cast<unsigned int>(time(0)));
    for (int i = 0; i < 100; i++) {
        int key_ = rand() % key_num+1;
        std::string key = std::to_string(key_);
        std::string value;
        status = db->Get(readOptions, key, &value);
        ASSERT_TRUE(status.ok());
    }

    Env::Default()->SleepForMicroseconds(ttl * 1000000);

    for (int i = 0; i < 100; i++) {
        int key_ = rand() % key_num+1;
        std::string key = std::to_string(key_);
        std::string value;
        status = db->Get(readOptions, key, &value);
        ASSERT_FALSE(status.ok());
    }
}

// 给出的测试
TEST(TestTTL, CompactionTTL) {
    DB *db;

    if(OpenDB("testdb", &db).ok() == false) {
        std::cerr << "open db failed" << std::endl;
        abort();
    }

    uint64_t ttl = 20;
    InsertData(db, ttl);

    leveldb::Range ranges[1];
    ranges[0] = leveldb::Range("-", "A");
    uint64_t sizes[1];
    db->GetApproximateSizes(ranges, 1, sizes);
    ASSERT_GT(sizes[0], 0);

    Env::Default()->SleepForMicroseconds(ttl * 1000000);

    db->CompactRange(nullptr, nullptr);

    leveldb::Range ranges[1];
    ranges[0] = leveldb::Range("-", "A");
    uint64_t sizes[1];
    db->GetApproximateSizes(ranges, 1, sizes);
    ASSERT_EQ(sizes[0], 0);
}

// 修改后的测试
// TEST(TestTTL, CompactionTTL) {
//     DB *db;

//     if(OpenDB("testdb", &db).ok() == false) {
//         std::cerr << "open db failed" << std::endl;
//         abort();
//     }

//     uint64_t ttl = 20;
//     // 插入足够的数据以确保 Level0 有至少一个 SSTable
//     for (int i = 0; i < 1000; ++i) {
//         InsertData(db, ttl);
//     }

//     // 获取 Level0 的 SSTable 数量
//     std::string num_files;
//     db->GetProperty("leveldb.num-files-at-level0", &num_files);
//     ASSERT_GT(std::stoi(num_files), 0);

//     // 使用覆盖整个数据库的数据范围
//     leveldb::Range full_range;
//     full_range.start = "";
//     full_range.limit = "";

//     uint64_t sizes[1];
//     db->GetApproximateSizes(&full_range, 1, sizes);
//     ASSERT_GT(sizes[0], 0);

//     Env::Default()->SleepForMicroseconds(ttl * 1000000);

//     leveldb::Slice start(full_range.start);
//     leveldb::Slice limit(full_range.limit);
//     db->CompactRange(&start, &limit);

//     db->GetApproximateSizes(&full_range, 1, sizes);
//     ASSERT_EQ(sizes[0], 0);
// }


int main(int argc, char** argv) {
  // All tests currently run with the same read-only file limits.
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
