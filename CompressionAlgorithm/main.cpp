#include <iostream>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <vector>
#include <filesystem>
#include <map>


/// Compression algorithm is based on
/// <a href="https://en.wikipedia.org/wiki/Huffman_coding#Basic_technique">huffman coding</a>
/// and is separated in 2 parts:
/// <br> <br>
/// <h2>Part 1</h2>
/// <ol type = "1">
///     <li>Size information</li>
///     <li>Counting usage frequency of unique bytes and unique byte count</li>
///     <li>Creating the base of the translation array</li>
///     <li>Creating the translation tree inside the translation array by weight distribution</li>
///     <li>Adding strings from top to bottom to create translated versions of unique bytes</li>
/// </ol>
/// <br>
/// <h2>Part 2</h2>
/// <ul>
///     <li>first (one byte) -> letter_count</li>
///     <li>second (bit groups)
///         <ul>
///             <li>(8 bits) -> current unique byte</li>
///             <li>(8 bits) -> length of the transformation</li>
///             <li>(bits) -> transformation code of that unique byte</li>
///         </ul>
///     </li>
///     <li>fourth (2 bytes)**1** -> file_count (inside the current folder)
///     <li>fifth (1 bit)**2** -> file or folder information -> folder(0), file(1)</li>
///     <li>sixth (8 bytes) -> size of current input_file (IF FILE)
///     <li> seventh (bit group)
///         <ul>
///             <li>(8 bits) -> length of current input_file's or folder's name</li>
///             <li>(bits) -> transformed version of current input_file's or folder's name</li>
///         </ul>
///     </li>
///     <li>eight (a lot of bits) -> transformed version of current input_file (IF FILE)</li>
/// </ul>
/// **1** groups from fifth to eighth will be written as much as file count in that folder <br>
///    (this is argument_count-1(argc-1) for the main folder) <br>
/// **2** whenever we see a new folder we will write seventh then start writing from fourth to eighth









namespace fs = std::filesystem;


/// \brief This structure will be used to create the translation tree
struct huff_tree {
        huff_tree *left, *right; //!< left and right nodes of the tree
        unsigned char character; //!< associated character in the tree node
        long int number; //<! occurrences of the respective character
        std::string bit; //<! bit that represents
        
        huff_tree() = default;
        
        huff_tree(huff_tree *left, huff_tree *right, long int number, unsigned char character)
        {
                this->left = left;
                this->right = right;
                this->number = number;
                this->character = character;
        }
        
/// \brief comparison function by huff_tree::number for two huff_tree's structures in ascending order
///
/// \param first is first instantiation of huff_tree structure
/// \param second is second instantiation of huff_tree structure
/// \return the smaller of the two
        static bool huffTreeCompare(const huff_tree &first, const huff_tree &second)
        {
                return first.number < second.number;
        }
};

/// \brief Check if the path is to a folder or to a file
/// and return boolean
///
/// \param path - char sequence representing the path to a folder of file
/// \return true if path is to a folder or false if is to a regular file
bool isFolder(const std::string &path);

/// Count usage frequency of bytes inside the file and store the information
/// in long integer massive (bytesFreq) and parallel
///
/// \param path - char sequence representing the path to a folder of file
/// \param bytesFreq - long integer massive for bytes frequency storage
/// \param total_size - size of the content in inputted path
/// \param total_bits - count the compressed file size
void countFileBytesFreq(const std::string &path, std::map<char, int> &occurrence_symbol, long int &total_size,
                        long int &total_bits);

/// This function counts usage frequency of bytes inside a folder
///
/// \param path - char sequence representing the path to a folder of file
/// \param bytesFreq - long integer massive that counts number of times
/// that all of the unique bytes is used on the files/file names/folder names
/// \param total_size - size of the content in inputted path
/// \param total_bits - count the compressed file size
void countFolderBytesFreq(const std::string &path, std::map<char, int> &occurrence_symbol, long int &total_size,
                          long int &total_bits);

/// \brief checks if test condition is false or true
///
/// \param test - false or true. If true -> print the error, else continue
/// \param message - message that represents the error
/// \param ... - arguments for error printing
void check(bool test, const char *message, ...)
{
        if (test) {
                va_list args;
                va_start(args, message);
                vfprintf(stderr, message, args);
                va_end(args);
                fprintf(stderr, "\n");
                exit(EXIT_FAILURE);
        }
}

int main(int argc, const char *argv[])
{
        std::map<char, int> occurrence_symbol;
        
        std::string compressedFile;
        FILE *original_fp;
        
        for (int i = 1; i < argc; i++) {
                if (!isFolder(argv[i])) {
                        original_fp = fopen(argv[i], "rb");
                        if (!original_fp) {
                                std::cout << argv[i] << " file does not exist" <<
                                          std::endl << "Process has been terminated" << std::endl;
                                return 0;
                        }
                        fclose(original_fp);
                }
        }
        
        compressedFile = argv[1];
        compressedFile += ".huff";
        
        long int total_size = 0;
        long int total_bits = 16 + 9 * (argc - 1);
        for (int argvIdx = 1; argvIdx < argc; argvIdx++) {
                for (const char *c = argv[argvIdx]; *c; c++)
                        occurrence_symbol[*c]++;
                
                if (isFolder(argv[argvIdx]))
                        countFolderBytesFreq(argv[argvIdx], occurrence_symbol, total_size, total_bits);
                else
                        countFileBytesFreq(argv[argvIdx], occurrence_symbol, total_size, total_bits);
        }
        for (const auto &[key, value]: occurrence_symbol)
                std::cout << "Character: " << key << "  value: " << value << std::endl;
        
        
        unsigned long symbols = occurrence_symbol.size();
        std::cout << "Letters in file: " << symbols << std::endl;
        std::cout << "file size: " << total_size << std::endl;
        std::cout << "total bits: " << total_bits << std::endl;
        
        
        std::vector<huff_tree> array;
        array.reserve(occurrence_symbol.size());
        for (const auto &[key, value]: occurrence_symbol)
                array.emplace_back(nullptr, nullptr, value, key);
        
        
        std::sort(array.begin(), array.end(), huff_tree::huffTreeCompare);
        for (const auto &item: array) {
                std::cout << "Huff num: " << item.number << "   huff char: " << item.character << std::endl;
        }
}

bool isFolder(const std::string &path)
{
        DIR *temp = opendir(path.c_str());
        if (temp) {
                closedir(temp);
                return true;
        }
        return false;
}

void countFileBytesFreq(const std::string &path, std::map<char, int> &occurrence_symbol,
                        long int &total_size,
                        long int &total_bits)
{
        
        int fd = open(path.c_str(), O_RDONLY);
        check(fd < 0, "open %stat_buff failed: %stat_buff", path.c_str(), strerror(errno));
        
        struct stat stat_buff{ };
        int status = fstat(fd, &stat_buff);
        check(status < 0, "stat %stat_buff failed: %stat_buff", path.c_str(), strerror(errno));
        
        const char *mapped = static_cast<const char *>(mmap(nullptr, stat_buff.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
        check(mapped == MAP_FAILED, "mmap %stat_buff failed: %stat_buff", path.c_str(), strerror(errno));
        
        total_size += stat_buff.st_size;
        total_bits += 64;
        
        
        for (long int i = 0; i < stat_buff.st_size; i++)
                if (occurrence_symbol.contains(mapped[i]))
                        occurrence_symbol.at(mapped[i])++;
                else
                        occurrence_symbol.insert_or_assign(mapped[i], 1);
        
        int err = munmap((void *) mapped, stat_buff.st_size);
        if (err != 0) {
                printf("UnMapping Failed\n");
                exit(EXIT_FAILURE);
        }
}

void countFolderBytesFreq(const std::string &path, std::map<char, int> &occurrence_symbol,
                          long int &total_size, long int &total_bits)
{
        total_size += 4096;
        total_bits += 16;
        
        
        for (const auto &entry: fs::recursive_directory_iterator(path)) {
                std::string next_path = entry.path();
                std::string folder_name = &next_path.substr(next_path.find_last_of('/'))[1];
                if (folder_name[0] == '.')
                        continue;
                
                total_bits += 9;
                for (const char *c = folder_name.c_str(); *c; c++)
                        occurrence_symbol[*c]++;
                
                std::cout << next_path << std::endl;
                if (entry.is_directory()) {
                        total_size += 4096;
                        total_bits += 16;
                } else
                        countFileBytesFreq(next_path, occurrence_symbol, total_size, total_bits);
        }
}
