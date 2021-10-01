#include <iostream>
#include <dirent.h>
#include <fcntl.h>
#include <vector>
#include <filesystem>
#include <map>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>

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
/// **2** whenever we see a new folder we will writeFromCh seventh then start writing from fourth to eighth









namespace fs = std::filesystem;


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

struct transform_supp {
        std::string str_arr[256];
        unsigned char current_byte = '\0';
        int current_bit_count = 0;
};

/// \brief Check if the path is to a folder or to a file
/// and return boolean
///
/// \param path - char sequence representing the path to a folder of file
/// \return true if path is to a folder or false if is to a regular file
bool isFolder(const std::string &path);

/// \brief Count usage frequency of bytes inside the file and store the information
/// in long integer massive (bytesFreq) and parallel
///
/// \param path - char sequence representing the path to a folder of file
/// \param occurrence_symbol - key-value pair in which keys are symbols and values are their
/// number of occurrences
/// \param total_size - size of the content in inputted path
/// \param total_bits - count the compressed file size
void countFileBytesFreq(const std::string &path, std::map<unsigned char, int> &occurrence_symbol, long int &total_size,
                        long int &total_bits);

/// \brief This function counts usage frequency of bytes inside a folder
///
/// \param path - char sequence representing the path to a folder of file
/// \param occurrence_symbol - key-value pair in which keys are symbols and values are their
///// number of occurrences
/// \param total_size - size of the content in inputted path
/// \param total_bits - count the compressed file size
void countFolderBytesFreq(const std::string &path, std::map<unsigned char, int> &occurrence_symbol, long int &total_size,
                          long int &total_bits);

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
///
/// \param occurrence_symbol - key-value pair in which keys are symbols and values are their
/// number of occurrences
/// \param symbols - count of the file symbols
/// \return - vector of huff_tree's that represents trie
std::vector<huff_tree> createTree(const std::map<unsigned char, int> &occurrence_symbol, unsigned long symbols);

/// \brief This function is used for writing the uChar to compressed file.
/// It does not write it directly as one byte! Instead it mixes uChar and current byte, writes 8 bits of it
/// and puts the rest to current byte for later use
///
/// \param pFILE - file pointer
/// \param ch - character
/// \param byte - for file write
/// \param bit_count - integer value for current bit count
void writeFromCh(FILE *pFILE, unsigned char ch, unsigned char &byte, int bit_count);

/// \brief Writes the translation script into compressed file and the str_arr array
///
/// \param pFILE - file pointer
/// \param tree - vector of huff_tree's that represents trie
/// \param bits - in this function total bits doesn't represent total bits
//  instead, it represents (8 * number of bytes) we are going to use on our compressed file
/// \param file_size - size of the file
/// \param compressed_name - new name of the file
/// \param symbols - count of the file symbols
/// \return - transform_supp struct that contains str_arr array that make the compression process more time efficient,
/// unsigned char value of the current_byte and integer value of current_bit_count
transform_supp transformation(FILE *pFILE, std::vector<huff_tree> &tree, long bits, long file_size, const std::string &compressed_name,
                              unsigned long symbols);

/// \brief Writes all information in compressed order.
///
/// \param pFile - file pointer
/// \param argc - integer value that count provided files
/// \param argv - array that stores provided file names
/// \param trans - transform_supp struct that contains str_arr array that make the compression process more time efficient,
/// unsigned char value of the current_byte and integer value of current_bit_count
void allFileWrite(FILE *pFile, int argc, const char *argv[], transform_supp trans);

/// \brief This function is writing number of files we re going to translate inside current folder to compressed file's 2 bytes
/// It is done like this to make sure that it can work on little, big or middle-endian systems
///
/// \param pFILE - file pointer
/// \param file_count - number of files that are provided (argc - 1)
/// \param curr_byte - reference to unsigned char value of the current_byte
/// \param curr_bit_count - integer value of current_bit_count
void writeFileCount(FILE *pFILE, int file_count, unsigned char &curr_byte, int curr_bit_count);

/// \brief This function is writing byte count of current input file to compressed file using 8 bytes
/// It is done like this to make sure that it can work on little, big or middle-endian systems
///
/// \param pFILE - file pointer
/// \param size - size of the original file
/// \param curr_byte - reference to unsigned char value of the current_byte
/// \param curr_bit_count - integer value of current_bit_count
void writeFileSize(FILE *pFILE, long size, unsigned char &curr_byte, int curr_bit_count);

/// \brief This function writes bytes that are translated from current input file's name to the compressed file.
///
/// \param pFILE - file pointer
/// \param file_name - name of the file
/// \param str_arr - array that make the compression process more time efficient
/// \param curr_byte - reference to unsigned char value of the current_byte
/// \param curr_bit_count - integer value of current_bit_count
void writeFileName(FILE *pFILE, const char *file_name, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count);

/// \brief This function translates and writes bytes from current input file to the compressed file.
///
/// \param compressed_fp - file pointer to compressed file
/// \param original_fp - file pointer to original file
/// \param size - size of the original file
/// \param str_arr - array that make the compression process more time efficient
/// \param curr_byte - reference to unsigned char value of the current_byte
/// \param curr_bit_count - integer value of current_bit_count
void writeTheFileContent(FILE *compressed_fp, FILE *original_fp, long size, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count);

/// \brief This function manages all other function to make folder compression available
///
/// \param pFILE - file pointer
/// \param path - folder name
/// \param str_arr - array that make the compression process more time efficient
/// \param curr_byte - reference to unsigned char value of the current_byte
/// \param curr_bit_count - integer value of current_bit_count
void writeTheFolder(FILE *pFILE, std::string path, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count);

/// \brief checks if test condition is false or true
///
/// \param test - false or true. If true -> print the error, else continue
/// \param message - message that represents the error
/// \param fd - file descriptor for closing
/// \param ... - arguments for error printing
void check(int fd, bool test, const char *message, ...)
{
        if (!test) return;
        close(fd);
        va_list args;
        va_start(args, message);
        vfprintf(stderr, message, args);
        va_end(args);
        fprintf(stderr, "\n");
        exit(EXIT_FAILURE);
}

int main(int argc, const char *argv[])
{
        std::map<unsigned char, int> occurrence_symbol;
        
        std::string compressed_name;
        
        for (int i = 1; i < argc; ++i) {
                int fd = open(argv[i], O_RDONLY);
                check(fd, fd < 0, "open %stat_buff failed: %stat_buff ", argv[i], strerror(errno));
        }
        
        compressed_name = argv[1];
        compressed_name += ".compressed";
        
        long int file_size = 0;
        long int total_bits = 16 + 9 * (argc - 1);
        for (int argvIdx = 1; argvIdx < argc; argvIdx++) {
                for (const char *c = argv[argvIdx]; *c; c++)
                        occurrence_symbol[*c]++;
                
                if (isFolder(argv[argvIdx]))
                        countFolderBytesFreq(argv[argvIdx], occurrence_symbol, file_size, total_bits);
                else
                        countFileBytesFreq(argv[argvIdx], occurrence_symbol, file_size, total_bits);
        }
        for (const auto &[key, value]: occurrence_symbol)
                std::cout << "Character: " << key << "  value: " << value << std::endl;
        
        
        unsigned long symbols = occurrence_symbol.size();
        std::cout << "Letters in file: " << symbols << std::endl;
        std::cout << "file size: " << file_size << std::endl;
        std::cout << "total bits: " << total_bits << std::endl;
        
        
        std::vector<huff_tree> tree = createTree(occurrence_symbol, symbols);
        
        for (const auto &item: tree) {
                std::cout << "Huff num: " << item.number << "\thuff char: " << item.character
                          << "\thuff bit: " << item.bit << std::endl;
        }
        
        
        FILE *compressed_fp = fopen(compressed_name.c_str(), "wb");
        fwrite(&symbols, 1, 1, compressed_fp);
        
        allFileWrite(compressed_fp, argc, argv,
                     transformation(compressed_fp, tree, total_bits + 8, file_size, compressed_name, symbols));
        
        fclose(compressed_fp);
        system("clear");
        std::cout << std::endl << "Created compressed file: " << compressed_name << std::endl;
        std::cout << "Compression is complete" << std::endl;
}

transform_supp transformation(FILE *pFILE, std::vector<huff_tree> &tree, long bits, long file_size, const std::string &compressed_name,
                              unsigned long symbols)
{
        char *str_pointer;
        unsigned char len, current_character;
        transform_supp trans;
        for (huff_tree *huff = tree.data(); huff < tree.data() + symbols; huff++) {
                trans.str_arr[(huff->character)] = huff->bit;
                len = huff->bit.length();
                current_character = huff->character;
                
                writeFromCh(pFILE, current_character, trans.current_byte, trans.current_bit_count);
                writeFromCh(pFILE, len, trans.current_byte, trans.current_bit_count);
                bits += len + 16;
                
                str_pointer = &huff->bit[0];
                while (*str_pointer) {
                        if (trans.current_bit_count == 8) {
                                fwrite(&trans.current_byte, 1, 1, pFILE);
                                trans.current_bit_count = 0;
                        }
                        switch (*str_pointer) {
                                case '1': {
                                        trans.current_byte <<= 1;
                                        trans.current_byte |= 1;
                                        trans.current_bit_count++;
                                        break;
                                }
                                case '0': {
                                        trans.current_byte <<= 1;
                                        trans.current_bit_count++;
                                        break;
                                }
                                default: {
                                        std::cout << "An error has occurred" << std::endl << "Compression process aborted" << std::endl;
                                        fclose(pFILE);
                                        remove(compressed_name.c_str());
                                        exit(1);
                                }
                        }
                        str_pointer++;
                }
                
                bits += len * (huff->number);
        }
        if (bits % 8)
                bits = (bits / 8 + 1) * 8;
        
        
        std::cout << "The size of the sum of ORIGINAL files is: " << file_size << " bytes" << std::endl;
        std::cout << "The size of the COMPRESSED file will be: " << bits / 8 << " bytes" << std::endl;
        std::cout << "Compressed file's size will be [%" << 100 * ((float) bits / 8 / (float) file_size) << "] of the original file" << std::endl;
        if (bits / 8 > file_size)
                std::cout << std::endl << "COMPRESSED FILE'S SIZE WILL BE HIGHER THAN THE SUM OF ORIGINALS" << std::endl << std::endl;
        return trans;
}

void allFileWrite(FILE *pFile, int argc, const char *argv[], transform_supp trans)
{
        std::cout << "\n\nTrans curr byte: " << trans.current_byte << std::endl;
        std::cout << "Trans curr bit count : " << trans.current_bit_count << std::endl;
        std::cout << "Trans str_arr : " << trans.str_arr << std::endl << std::endl << std::endl;
        
        writeFileCount(pFile, argc - 1, trans.current_byte, trans.current_bit_count);
        
        FILE *original_fp;
        for (int current_file = 1; current_file < argc; current_file++) {
                
                if (!isFolder(argv[current_file])) {
                        original_fp = fopen(argv[current_file], "rb");
                        fseek(original_fp, 0, SEEK_END);
                        long size = ftell(original_fp);
                        rewind(original_fp);
                        
                        if (trans.current_bit_count == 8) {
                                fwrite(&trans.current_byte, 1, 1, pFile);
                                trans.current_bit_count = 0;
                        }
                        trans.current_byte <<= 1;
                        trans.current_byte |= 1;
                        trans.current_bit_count++;
                        
                        writeFileSize(pFile, size, trans.current_byte, trans.current_bit_count);
                        writeFileName(pFile, argv[current_file], trans.str_arr, trans.current_byte, trans.current_bit_count);
                        writeTheFileContent(pFile, original_fp, size, trans.str_arr, trans.current_byte, trans.current_bit_count);
                        fclose(original_fp);
                } else {
                        if (trans.current_bit_count == 8) {
                                fwrite(&trans.current_byte, 1, 1, pFile);
                                trans.current_bit_count = 0;
                        }
                        trans.current_byte <<= 1;
                        trans.current_bit_count++;
                        
                        writeFileName(pFile, argv[current_file], trans.str_arr, trans.current_byte, trans.current_bit_count);
                        
                        std::string folder_name = argv[current_file];
                        writeTheFolder(pFile, folder_name, trans.str_arr, trans.current_byte, trans.current_bit_count);
                }
        }
        
        
        if (trans.current_bit_count == 8)
                fwrite(&trans.current_byte, 1, 1, pFile);
        else {
                trans.current_byte <<= 8 - trans.current_bit_count;
                fwrite(&trans.current_byte, 1, 1, pFile);
        }
}

void writeTheFolder(FILE *pFILE, std::string path, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count)
{
        FILE *original_fp;
        path += '/';
        DIR *dir = opendir(&path[0]);
        std::string next_path;
        struct dirent *current;
        int file_count = 0;
        long int size;
        while ((current = readdir(dir))) {
                if (current->d_name[0] == '.') {
                        if (current->d_name[1] == 0)continue;
                        if (current->d_name[1] == '.' && current->d_name[2] == 0)continue;
                }
                file_count++;
        }
        rewinddir(dir);
        writeFileCount(pFILE, file_count, curr_byte, curr_bit_count);
        
        while ((current = readdir(dir))) {
                if (current->d_name[0] == '.') {
                        if (current->d_name[1] == 0)continue;
                        if (current->d_name[1] == '.' && current->d_name[2] == 0)continue;
                }
                
                next_path = path + current->d_name;
                if (!isFolder(&next_path[0])) {
                        
                        original_fp = fopen(&next_path[0], "rb");
                        fseek(original_fp, 0, SEEK_END);
                        size = ftell(original_fp);
                        rewind(original_fp);
                        
                        if (curr_bit_count == 8) {
                                fwrite(&curr_byte, 1, 1, pFILE);
                                curr_bit_count = 0;
                        }
                        curr_byte <<= 1;
                        curr_byte |= 1;
                        curr_bit_count++;
                        
                        writeFileSize(pFILE, size, curr_byte, curr_bit_count);
                        writeFileName(pFILE, current->d_name, str_arr, curr_byte, curr_bit_count);
                        writeTheFileContent(pFILE, original_fp, size, str_arr, curr_byte, curr_bit_count);
                        fclose(original_fp);
                } else {
                        if (curr_bit_count == 8) {
                                fwrite(&curr_byte, 1, 1, pFILE);
                                curr_bit_count = 0;
                        }
                        curr_byte <<= 1;
                        curr_bit_count++;
                        
                        writeFileName(pFILE, current->d_name, str_arr, curr_byte, curr_bit_count);
                        
                        writeTheFolder(pFILE, next_path, str_arr, curr_byte, curr_bit_count);
                }
        }
        closedir(dir);
}

void writeTheFileContent(FILE *compressed_fp, FILE *original_fp, long size, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count)
{
        unsigned char *x_p, x;
        x_p = &x;
        char *str_pointer;
        fread(x_p, 1, 1, original_fp);
        for (long int i = 0; i < size; i++) {
                str_pointer = &str_arr[x][0];
                while (*str_pointer) {
                        if (curr_bit_count == 8) {
                                fwrite(&curr_byte, 1, 1, compressed_fp);
                                curr_bit_count = 0;
                        }
                        switch (*str_pointer) {
                                case '1':curr_byte <<= 1;
                                        curr_byte |= 1;
                                        curr_bit_count++;
                                        break;
                                case '0':curr_byte <<= 1;
                                        curr_bit_count++;
                                        break;
                                default:std::cout << "An error has occurred" << std::endl << "Process has been aborted";
                                        exit(2);
                        }
                        str_pointer++;
                }
                fread(x_p, 1, 1, original_fp);
        }
}

void writeFileName(FILE *pFILE, const char *file_name, std::string *str_arr, unsigned char &curr_byte, int &curr_bit_count)
{
        writeFromCh(pFILE, strlen(file_name), curr_byte, curr_bit_count);
        char *str_pointer;
        for (const char *c = file_name; *c; c++) {
                str_pointer = &str_arr[(unsigned char) (*c)][0];
                while (*str_pointer) {
                        if (curr_bit_count == 8) {
                                fwrite(&curr_byte, 1, 1, pFILE);
                                curr_bit_count = 0;
                        }
                        switch (*str_pointer) {
                                case '1':curr_byte <<= 1;
                                        curr_byte |= 1;
                                        curr_bit_count++;
                                        break;
                                case '0':curr_byte <<= 1;
                                        curr_bit_count++;
                                        break;
                                default:std::cout << "An error has occurred" << std::endl << "Process has been aborted";
                                        exit(2);
                        }
                        str_pointer++;
                }
        }
}

void writeFileSize(FILE *pFILE, long size, unsigned char &curr_byte, int curr_bit_count)
{
        for (int i = 0; i < 8; i++) {
                writeFromCh(pFILE, size % 256, curr_byte, curr_bit_count);
                size /= 256;
        }
}

void writeFileCount(FILE *pFILE, int file_count, unsigned char &curr_byte, int curr_bit_count)
{
        unsigned char temp = file_count % 256;
        writeFromCh(pFILE, temp, curr_byte, curr_bit_count);
        temp = file_count / 256;
        writeFromCh(pFILE, temp, curr_byte, curr_bit_count);
}

void writeFromCh(FILE *pFILE, unsigned char ch, unsigned char &byte, int bit_count)
{
        byte <<= 8 - bit_count;
        byte |= (ch >> bit_count);
        fwrite(&byte, 1, 1, pFILE);
        byte = ch;
}

std::vector<huff_tree> createTree(const std::map<unsigned char, int> &occurrence_symbol, unsigned long symbols)
{
        std::vector<huff_tree> tree{symbols * 2 - 1};
        
        huff_tree *e = tree.data();
        for (const auto &[key, value]: occurrence_symbol) {
                e->right = nullptr;
                e->left = nullptr;
                e->number = value;
                e->character = key;
                e++;
        }
        std::sort(tree.begin(), tree.end() - (symbols - 1), huff_tree::huffTreeCompare);
        
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
        return tree;
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

void countFileBytesFreq(const std::string &path, std::map<unsigned char, int> &occurrence_symbol,
                        long int &total_size,
                        long int &total_bits)
{
        unsigned char x;
        FILE *original_fp = fopen(path.c_str(), "rb");
        fseek(original_fp, 0, SEEK_END);
        long int size = ftell(original_fp);
        total_size += size;
        rewind(original_fp);
        
        total_bits += 64;
        
        fread(&x, 1, 1, original_fp);
        for (long int j = 0; j < size; j++) {
                occurrence_symbol[x]++;
                fread(&x, 1, 1, original_fp);
        }
        fclose(original_fp);
}

void countFolderBytesFreq(const std::string &path, std::map<unsigned char, int> &occurrence_symbol,
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
