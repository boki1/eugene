#include <iostream>
#include <dirent.h>
#include <fcntl.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <map>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <numeric>

#define BYTE 8

namespace fs = std::filesystem;

namespace internal::compression
{
    
    template<typename T>
    std::vector<uint8_t> compress(const T &);
    
    template<typename T>
    T decompress(std::vector<uint8_t> v);
    
}

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
///     <li>first (one byte) -> symbols</li>
///     <li>second (bit groups)
///         <ul>
///             <li>(8 bits) -> current unique byte</li>
///             <li>(8 bits) -> length of the transformation</li>
///             <li>(bits) -> transformation code of that unique byte</li>
///         </ul>
///     </li>
///     <li>third (2 bytes)**1** -> file_count (inside the current folder)
///     <li>fourth (1 bit)**2** -> file or folder information -> folder(0), file(1)</li>
///     <li>fifth (8 bytes) -> size of current input_file (IF FILE)
///     <li> sixth (bit group)
///         <ul>
///             <li>(8 bits) -> length of current input_file's or folder's name</li>
///             <li>(bits) -> transformed version of current input_file's or folder's name</li>
///         </ul>
///     </li>
///     <li>seventh (a lot of bits) -> transformed version of current input_file (IF FILE)</li>
/// </ul>
/// **1** groups from fifth to eighth will be written as much as file count in that folder <br>
///    (this is argument_count-1(argc-1) for the main folder) <br>
/// **2** whenever we see a new folder we will write_from_ch seventh then start writing from fourth to eighth
class Compressor {
public:
/// \brief Constructor of the compression class with which you can compress provided files
///
/// \param argc - number of files for compress
/// \param argv - path's to files for compress
        Compressor(const int argc, const char *argv[])
        {
                files.reserve(argc - 1);
                for (int i = 1; i < argc; ++i)
                        files.emplace_back(argv[i]);
                
                if (argc == 2) {
                        compressed_name = files[0];
                        compressed_name += ".huff";
                } else
                        compressed_name = "bundle.huff";
        }

/// \brief The main function of compression class that do all the magic with provided files.
        void operator()()
        {
//                TODO: find all "open/fopen/opendir" functions and check for errors
//                for (const auto &item: files) {
//                        int fd = open(item.c_str(), O_RDONLY);
//                        check(fd, fd < 0, "open %stat_buff failed: %stat_buff ", item.c_str(), strerror(errno));
//                }
                
                
                for (const auto &file: files)
                        all_size += std::accumulate(
                                fs::recursive_directory_iterator(file.c_str()), fs::recursive_directory_iterator(), 0,
                                [ ](auto sz, auto entry) { return is_directory(entry) ? sz : sz + file_size(entry); });
                
                total_bits = 16 + 9 * files.size();
                for (const auto &item: files) {
                        for (const char *c = item.c_str(); *c; c++)
                                occurrence_symbol[*c]++;
                        
                        if (fs::is_directory(item))
                                count_folder_bytes_freq(item);
                        else
                                count_file_bytes_freq(item);
                }
                
                symbols = occurrence_symbol.size();
                
                
                tree.resize(symbols * 2 - 1);
                initialize_tree();
                
                compressed_fp = fopen(compressed_name.c_str(), "wb");
                fwrite(&symbols, 1, 1, compressed_fp);
                total_bits += BYTE;
                
                transform();
                all_file_write();
                
                
                fclose(compressed_fp);
                system("clear");
                std::cout << std::endl << "Created compressed file: " << compressed_name << std::endl;
                std::cout << "Compression is complete" << std::endl;
        }

private:
        /// \brief This structure will be used to create the translation tree
        struct huff_tree {
                huff_tree *left{nullptr}, *right{nullptr}; //!< left and right nodes of the tree
                unsigned char character; //!< associated character in the tree node
                long int number; //<! occurrences of the respective character
                std::string bit; //<! bit that represents
                
                huff_tree() = default;
                
                huff_tree(long int num, unsigned char c): character(c), number(num)
                { }

/// \brief comparison function by huff_tree::number for two huff_tree's structures in ascending order
///
/// \param first is first instance of huff_tree structure
/// \param second is second instance of huff_tree structure
/// \return the smaller of the two
                static bool huffTreeCompare(const huff_tree &first, const huff_tree &second)
                {
                        return first.number < second.number;
                }
        };
        
        std::vector<std::string> files; //!< path to the files for compress
        
        FILE *compressed_fp = nullptr; //!< file pinter to the new created compressed file
        std::map<unsigned char, int> occurrence_symbol; //!< key-value pair
//!< in which keys are symbols and values are their number of occurrences
        
        std::vector<huff_tree> tree; //!< vector of huff_tree's that represents trie
        
        std::string compressed_name; //!< new name of the compressed file
        unsigned long all_size = 0; //!< size of the original file or folder
        unsigned long total_bits = 0; //!< count the compressed file size
        unsigned long symbols = 0; //!< count of the file or folder symbols
        
        
        std::string str_arr[256]; //!< transformation string
//!< is put to str_arr array to make the compression process more time efficient
        unsigned char current_byte = '\0'; //!< unsigned char value
//!< that represents the current_byte
        int current_bit_count = 0; //!< integer value of current_bit_count
        
        
        /// \brief First creates the base of translation tree(and then sorting them by ascending frequencies).
/// Then creates pointers that traverses through leaf's.
/// At every cycle, 2 of the least weighted nodes will be chosen to
/// create a new node that has weight equal to sum of their weights combined.
/// After we are done with these nodes they will become children of created nodes
/// and they will be passed so that they wont be used in this process again.
/// Finally, we are adding the bytes from root to leaf's
/// and after this is done every leaf will have a transformation string that corresponds to it
/// It is actually a very neat process. Using 4th and 5th code blocks, we are making sure that
/// the most used character is using least number of bits.
/// Specific number of bits we re going to use for that character is determined by weight distribution
        void initialize_tree()
        {
                huff_tree *e = tree.data();
                for (const auto &[key, value]: occurrence_symbol) {
                        e->right = nullptr;
                        e->left = nullptr;
                        e->number = value;
                        e->character = key;
                        e++;
                }
                std::sort(tree.begin(), tree.end() - (long) (symbols - 1), huff_tree::huffTreeCompare);
                
                huff_tree *min1 = tree.data(), *min2 = tree.data() + 1;
                huff_tree *curr = tree.data() + symbols;
                huff_tree *not_leaf = tree.data() + symbols;
                huff_tree *is_leaf = tree.data() + 2;
                
                for (int i = 0; i < symbols - 1; i++) {
                        curr->number = min1->number + min2->number;
                        curr->left = min1;
                        curr->right = min2;
                        min1->bit = "1";
                        min2->bit = "0";
                        curr++;
                        
                        if (is_leaf >= tree.data() + symbols) {
                                min1 = not_leaf;
                                not_leaf++;
                        } else {
                                if (is_leaf->number < not_leaf->number) {
                                        min1 = is_leaf;
                                        is_leaf++;
                                } else {
                                        min1 = not_leaf;
                                        not_leaf++;
                                }
                        }
                        
                        if (is_leaf >= tree.data() + symbols) {
                                min2 = not_leaf;
                                not_leaf++;
                        } else if (not_leaf >= curr) {
                                min2 = is_leaf;
                                is_leaf++;
                        } else {
                                if (is_leaf->number < not_leaf->number) {
                                        min2 = is_leaf;
                                        is_leaf++;
                                } else {
                                        min2 = not_leaf;
                                        not_leaf++;
                                }
                        }
                }
                for (huff_tree *huff = tree.data() + symbols * 2 - 2; huff > tree.data() - 1; huff--) {
                        if (huff->left)
                                huff->left->bit = huff->bit + huff->left->bit;
                        if (huff->right)
                                huff->right->bit = huff->bit + huff->right->bit;
                }
        }

/// \brief Count usage frequency of bytes inside the file and store the information
/// in long integer massive (bytesFreq) and parallel
///
/// \param path - char sequence representing the path to a folder of file
        void count_file_bytes_freq(const std::string &path)
        {
                unsigned char x;
                FILE *original_fp = fopen(path.c_str(), "rb");
                fseek(original_fp, 0, SEEK_END);
                const long int size = ftell(original_fp);
                rewind(original_fp);
                
                total_bits += 64;
                
                fread(&x, 1, 1, original_fp);
                for (long int j = 0; j < size; j++) {
                        occurrence_symbol[x]++;
                        fread(&x, 1, 1, original_fp);
                }
                fclose(original_fp);
        }

/// \brief This function counts usage frequency of bytes inside a folder
///
/// \param path - char sequence representing the path to a folder of file
        void count_folder_bytes_freq(const std::string &path)
        {
                total_bits += 16;
                
                
                for (const auto &entry: fs::recursive_directory_iterator(path)) {
                        std::string next_path = entry.path();
                        std::string folder_name = &next_path.substr(next_path.find_last_of('/'))[1];
                        if (folder_name[0] == '.')
                                continue;
                        
                        total_bits += 9;
                        for (const char *c = folder_name.c_str(); *c; c++)
                                occurrence_symbol[*c]++;
                        
                        if (entry.is_directory())
                                total_bits += 16;
                        else
                                count_file_bytes_freq(next_path);
                }
        }

/// \brief Writes the translation script into compressed file and the str_arr array.
/// (Manages second from part 2)
        void transform()
        {
                const char *str_pointer;
                unsigned char len, current_character;
                for (huff_tree *huff = tree.data(); huff < tree.data() + symbols; huff++) {
                        str_arr[(huff->character)] = huff->bit;
                        len = huff->bit.length();
                        current_character = huff->character;
                        
                        write_from_ch(current_character);
                        write_from_ch(len);
                        total_bits += len + 16;
                        
                        str_pointer = huff->bit.c_str();
                        while (*str_pointer) {
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                switch (*str_pointer) {
                                        case '1': {
                                                current_byte <<= 1;
                                                current_byte |= 1;
                                                current_bit_count++;
                                                break;
                                        }
                                        case '0': {
                                                current_byte <<= 1;
                                                current_bit_count++;
                                                break;
                                        }
                                        default: {
                                                std::cerr << "An error has occurred" << std::endl << "Compression process aborted" << std::endl;
                                                fclose(compressed_fp);
                                                remove(compressed_name.c_str());
                                                exit(1);
                                        }
                                }
                                str_pointer++;
                        }
                        
                        total_bits += len * (huff->number);
                }
                if (total_bits % BYTE)
                        total_bits = (total_bits / BYTE + 1) * BYTE;
                
                
                std::cout << "The size of the sum of ORIGINAL files is: " << all_size << " bytes" << std::endl;
                std::cout << "The size of the COMPRESSED file will be: " << total_bits / BYTE << " bytes" << std::endl;
                std::cout << "Compressed file's size will be [%" << 100 * ((float) total_bits / BYTE / (float) all_size) << "] of the original file"
                          << std::endl;
                if (total_bits / BYTE > all_size)
                        std::cout << std::endl << "WARNING: COMPRESSED FILE'S SIZE WILL BE HIGHER THAN THE SUM OF ORIGINALS" << std::endl <<
                                  std::endl;
        }

/// \brief Writes all information in compressed order.
/// (Manages from third to seventh of part 2)
        void all_file_write()
        {
                write_file_count(files.size());
                
                FILE *original_fp;
                for (const auto &item: files) {
                        if (!fs::is_directory(item)) {
                                original_fp = fopen(item.c_str(), "rb");
                                fseek(original_fp, 0, SEEK_END);
                                long size = ftell(original_fp);
                                rewind(original_fp);
                                
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                current_byte <<= 1;
                                current_byte |= 1;
                                current_bit_count++;
                                
                                write_file_size(size);
                                write_file_name(item.c_str());
                                write_the_file_content(original_fp, size);
                                fclose(original_fp);
                        } else {
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                current_byte <<= 1;
                                current_bit_count++;
                                
                                write_file_name(item.c_str());
                                
                                write_the_folder(item);
                        }
                }
                
                
                if (current_bit_count == BYTE)
                        fwrite(&current_byte, 1, 1, compressed_fp);
                else {
                        current_byte <<= BYTE - current_bit_count;
                        fwrite(&current_byte, 1, 1, compressed_fp);
                }
        }

/// \brief Open dir path and count regular files in it.
/// Then write this count in compressed file. (Manages third of part 2)
        void write_folder_files_count(const std::string &path)
        {
                DIR *dir = opendir(path.c_str());
                struct dirent *current;
                int file_count = 0;
                while ((current = readdir(dir))) {
                        if (current->d_name[0] == '.')
                                continue;
                        file_count++;
                }
                write_file_count(file_count);
        }

/// \brief This function manages all other function to make folder compression available.
/// (Manages from third to seventh of part 2 for a folder)
///
/// \param path - folder name
        void write_the_folder(const std::string &path)
        {
                write_folder_files_count(path);
                
                for (const auto &entry: fs::recursive_directory_iterator(path)) {
                        const std::string next_path = entry.path();
                        const std::string folder_name = &next_path.substr(next_path.find_last_of('/'))[1];
                        if (folder_name[0] == '.')
                                continue;
                        
                        if (!entry.is_directory()) {
                                FILE *original_fp = fopen(next_path.c_str(), "rb");
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                current_byte <<= 1;
                                current_byte |= 1;
                                current_bit_count++;
                                
                                write_file_size(entry.file_size());
                                write_file_name(folder_name.c_str());
                                write_the_file_content(original_fp, entry.file_size());
                                fclose(original_fp);
                        } else {
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                current_byte <<= 1;
                                current_bit_count++;
                                
                                write_file_name(folder_name.c_str());
                                
                                write_folder_files_count(next_path);
                        }
                }
        }

/// \brief This function translates and writes bytes from current input file to the compressed file.
/// (Manages seventh of part 2)
///
/// \param original_fp - file pointer to original file
/// \param size - size of the original file
        void write_the_file_content(FILE *original_fp, unsigned long size)
        {
                unsigned char x;
                const char *str_pointer;
                fread(&x, 1, 1, original_fp);
                for (long int i = 0; i < size; i++) {
                        str_pointer = str_arr[x].c_str();
                        while (*str_pointer) {
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                switch (*str_pointer) {
                                        case '1':current_byte <<= 1;
                                                current_byte |= 1;
                                                current_bit_count++;
                                                break;
                                        case '0':current_byte <<= 1;
                                                current_bit_count++;
                                                break;
                                        default:std::cerr << "An error has occurred" << std::endl << "Process has been aborted";
                                                exit(2);
                                }
                                str_pointer++;
                        }
                        fread(&x, 1, 1, original_fp);
                }
        }

/// \brief This function writes bytes that are translated from current input file's name to the compressed file.
/// (Manages sixth of part 2)
///
/// \param file_name - name of the file
        void write_file_name(const char *file_name)
        {
                write_from_ch(strlen(file_name));
                const char *str_pointer;
                for (const char *c = file_name; *c; c++) {
                        str_pointer = str_arr[(unsigned char) (*c)].c_str();
                        while (*str_pointer) {
                                if (current_bit_count == BYTE) {
                                        fwrite(&current_byte, 1, 1, compressed_fp);
                                        current_bit_count = 0;
                                }
                                switch (*str_pointer) {
                                        case '1':current_byte <<= 1;
                                                current_byte |= 1;
                                                current_bit_count++;
                                                break;
                                        case '0':current_byte <<= 1;
                                                current_bit_count++;
                                                break;
                                        default:std::cerr << "An error has occurred" << std::endl << "Process has been aborted";
                                                exit(2);
                                }
                                str_pointer++;
                        }
                }
        }

/// \brief This function is writing byte count of current input file to compressed file using 8 bytes.
/// It is done like this to make sure that it can work on little, big or middle-endian systems.
/// (Manages fifth of part 2)
///
/// \param size - size of the original file
        void write_file_size(unsigned long size)
        {
                for (int i = 0; i < BYTE; i++) {
                        write_from_ch(size % 256);
                        size /= 256;
                }
        }

/// \brief This function is writing number of files we re going to translate inside current folder to compressed file's 2 bytes
/// It is done like this to make sure that it can work on little, big or middle-endian systems
/// (Manages third of part 2)
///
/// \param file_count - number of files that are provided (argc - 1)
        void write_file_count(unsigned long file_count)
        {
                unsigned char temp = file_count % 256;
                write_from_ch(temp);
                temp = file_count / 256;
                write_from_ch(temp);
        }

/// \brief This function is used for writing the uChar to compressed file.
/// It does not write it directly as one byte! Instead it mixes uChar and current byte, writes 8 bits of it
/// and puts the rest to current byte for later use.
///
/// \param ch - character
        void write_from_ch(unsigned char ch)
        {
                current_byte <<= BYTE - current_bit_count;
                current_byte |= (ch >> current_bit_count);
                fwrite(&current_byte, 1, 1, compressed_fp);
                current_byte = ch;
        }
};
