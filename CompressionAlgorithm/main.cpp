#include <iostream>
#include <dirent.h>



/// Compression algorithm is based on <a href="https://en.wikipedia.org/wiki/Huffman_coding#Basic_technique">huffman coding</a>
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











/// \brief Check if the path is to a folder or to a file
/// and return boolean
///
/// \param path char sequence representing the path to a folder of file
/// \return true if path is to a folder or false if is to a regular file
bool isFolder(const char *);

/// Get file size in long integer representation
///
/// \param path char sequence representing the path to a folder of file
/// \return file size
long int fileSize(const char *);

/// Count usage frequency of bytes inside the file and store the information
/// in long integer massive (bytesFreq) and parallel
///
/// \param path char sequence representing the path to a folder of file
/// \param bytesFreq long integer massive for bytes frequency storage
/// \param total_size size of the content in inputted path
/// \param total_bits count the compressed file size
void countFileBytesFreq(std::string path, long int *bytesFreq, long int &total_size, long int &total_bits);

/// This function counts usage frequency of bytes inside a folder
///
/// \param path char sequence representing the path to a folder of file
/// \param bytesFreq long integer massive that counts number of times
/// that all of the unique bytes is used on the files/file names/folder names
/// \param total_size size of the content in inputted path
/// \param total_bits count the compressed file size
void countFolderBytesFreq(std::string path, long int *bytesFreq, long int &total_size, long int &total_bits);


int main(int argc, const char *argv[]) {
    long int bytesFreq[256]; //!< bytes frequency storage
    std::fill(bytesFreq, bytesFreq + 256, 0);

    std::string compressedFile;
    FILE *original_fp;

///    Check the input
    for (int i = 1; i < argc; i++) {
        if (isFolder(argv[i])) {
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

    long int total_size = 0; //!< size of the content in inputted path
    long int total_bits = 16 + 9 * (argc - 1); //!< count the compressed file size
    for (int argvIdx = 1; argvIdx < argc; argvIdx++) {
///        Count usage frequency of unique bytes on the file name (or folder name)
        for (const char *c = argv[argvIdx]; *c; c++)
            bytesFreq[(unsigned char) (*c)]++;

        if (isFolder(argv[argvIdx]))
            countFolderBytesFreq(argv[argvIdx], bytesFreq, total_size, total_bits);
        else
            countFileBytesFreq(argv[argvIdx], bytesFreq, total_size, total_bits);
    }
    for (int i = 0; i < 256; ++i)
        if (bytesFreq[i] != 0)
            std::cout << i << ": " << bytesFreq[i] << std::endl;

///    Count symbols inside 
    int symbols = 0;
    for (long int *i = bytesFreq; i < bytesFreq + 256; i++)
        if (*i)
            symbols++;
    std::cout << "Letters in file: " << symbols << std::endl;
}

bool isFolder(const char *path) {
    DIR *temp = opendir(path);
    if (temp) {
        closedir(temp);
        return true;
    }
    return false;
}

long int fileSize(const char *path) {
    long int size;
    FILE *fp = fopen(path, "rb");
    fseek(fp, 0, SEEK_END);
    size = ftell(fp);
    fclose(fp);
    return size;
}

void countFileBytesFreq(std::string path, long int *bytesFreq,
                        long int &total_size,
                        long int &total_bits) {
    long int size = fileSize(&path[0]);
    FILE *original_fp;
    total_size += size;
    total_bits += 64;

    original_fp = fopen(&path[0], "rb");

    unsigned char freq;
    fread(&freq, 1, 1, original_fp);
    for (long int i = 0; i < size; i++) {
        bytesFreq[freq]++;
        fread(&freq, 1, 1, original_fp);
    }
    fclose(original_fp);
}

void countFolderBytesFreq(std::string path, long int *bytesFreq,
                          long int &total_size, long int &total_bits) {
    path += '/';
    DIR *dir = opendir(&path[0]), *next_dir;
    std::string next_path;
    total_size += 4096;
    total_bits += 16; // for file_count
    struct dirent *current;
    while ((current = readdir(dir))) {
        if (current->d_name[0] == '.') {
            if (current->d_name[1] == 0)continue;
            if (current->d_name[1] == '.' && current->d_name[2] == 0)continue;
        }
        total_bits += 9;

        for (char *c = current->d_name; *c; c++)
            bytesFreq[(unsigned char) (*c)]++;

        next_path = path + current->d_name;

        next_dir = opendir(&next_path[0]);
        if (next_dir) {
            closedir(next_dir);
            countFolderBytesFreq(next_path, bytesFreq, total_size, total_bits);
        } else
            countFileBytesFreq(next_path, bytesFreq, total_size, total_bits);
    }
    closedir(dir);
}