#include <external/catch2/Catch2.h>
#include <external/expected/Expected.h>
#include <internal/storage/Compressor.h>
#include <internal/storage/Decompressor.h>

bool create_testing_directory(const std::string &new_structure, const int text_size)
{
        for (int i = 0; i < 3; ++i) {
                std::string filesystem_structure = new_structure;
                if (i > 0)
                        filesystem_structure += "/" + std::to_string(i);
                
                fs::path path{filesystem_structure};
                path /= "file.txt";
                if (!fs::create_directories(path.parent_path()))
                        return false;
                
                std::ofstream ofs(path);
                for (int j = 0; j < text_size; ++j)
                        ofs << "this is some text in the new file\n";
                ofs.close();
        }
        return true;
}

bool exists(const fs::path &p, fs::file_status s = fs::file_status{ })
{
        if (fs::status_known(s) ? fs::exists(s) : fs::exists(p))
                return true;
        else
                return false;
}

bool compare_folders(const std::string &first, const std::string &second)
{
        auto initial = fs::recursive_directory_iterator(first);
        for (auto &compressed: fs::recursive_directory_iterator(second)) {
                using compr = compression::storage::detail::CompressorInternal;
                std::string compressed_np = compressed.path();
                std::string initial_np = compressed.path();
                
                if (compressed_np != initial_np)
                        return false;
                
                if (!compressed.is_directory() && !initial->is_directory()) {
                        if (compr::return_file_info(compressed_np) != compr::return_file_info(initial_np))
                                return false;
                }
                initial++;
        }
        return true;
}

bool clean(std::map<std::string, std::string> &files)
{
        unsigned long long int initial_size = std::accumulate(
                fs::recursive_directory_iterator(files["change_name"].c_str()),
                fs::recursive_directory_iterator(), 0,
                [ ](auto sz, auto entry) { return is_directory(entry) ? sz : sz + file_size(entry); });
        unsigned long long int compressed_size = fs::file_size(files["compressed_name"]);
        
        REQUIRE(compressed_size < initial_size);
        std::cout << std::endl << std::endl << "#############################################################" << std::endl;
        std::cout << "Passed with initial size: " << initial_size << " and compressed size: " << compressed_size << std::endl;
        std::cout << "#############################################################" << std::endl << std::endl << std::endl;
        
        return std::ranges::all_of(files.cbegin(), files.cend(),
                                   [ ](const auto &pair) {
                                           return fs::remove_all(pair.second);
                                   });
}

void basic_test(std::map<std::string, std::string> &params, const int text_size)
{
        for (const auto &item: params)
                REQUIRE(!exists(item.second));
        
        REQUIRE(create_testing_directory(params["test_dir_name"], text_size));
        REQUIRE(exists(params["test_dir_name"]));
        
        compression::Compressor compress{
                std::vector<std::string>(1, params["test_dir_name"]),
                params["compressed_name"]
        };
        compress();
        REQUIRE(exists("test"));
        
        
        fs::rename(params["test_dir_name"], params["change_name"]);
        REQUIRE(exists(params["change_name"]));
        
        decompression::Decompressor decompress{params["compressed_name"]};
        decompress();
        REQUIRE(exists(params["test_dir_name"]));
        
        REQUIRE(compare_folders("InitialDir", "ForTesting"));
        REQUIRE(clean(params));
}

TEST_CASE("TestCompressorDecompressor compress_decompress", "[compressor_decompressor]")
{
        std::map<std::string, std::string> params;
        params["test_dir_name"] = "ForTesting";
        params["change_name"] = "InitialDir";
        params["compressed_name"] = "Test";
        
        for (int i = 1; i < 3; ++i)
                basic_test(params, (int) pow(10, i));
}