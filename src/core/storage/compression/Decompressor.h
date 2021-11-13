#ifndef STORAGE_DECOMPRESSOR_INCLUDED
#define STORAGE_DECOMPRESSOR_INCLUDED

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>

static constexpr unsigned char Check = 0b10000000;
static constexpr int Symbols = 256;
static constexpr int MkdirPermission = 0755;

/// Decompression algorithm is based on
/// <a href="https://en.wikipedia.org/wiki/Huffman_coding#Basic_technique">huffman coding</a>
/// <br> <br>
/// <ul>
///     <li>first (one byte) -> m_symbols</li>
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
///             <li>(bits) -> transform and write version of current input_file's or folder's name</li>
///         </ul>
///     </li>
///     <li>seventh (a lot of bits) -> write transformed version of current input_file (IF FILE)</li>
/// </ul>
/// **1** groups from fourth to seventh will be written as much as file count in that folder <br>
/// **2** whenever we see a new folder we will write sixth then
///    start writing the files(and folders) inside the current folder from third to seventh
namespace decompression {
namespace storage::detail {
class DecompressorImpl {
public:
	DecompressorImpl() = default;

	explicit DecompressorImpl(FILE *compressed) : m_compressed(compressed) {}

	/// \brief The main function of decompression class that do all the magic with provided m_files.
	void operator()() {
		fread(&m_symbols, 1, 1, m_compressed);
		if (m_symbols == 0)
			m_symbols = Symbols;

		m_trie_root = new huff_trie;
		for (unsigned long i = 0; i < m_symbols; i++)
			process_n_bits_to_string(m_trie_root);

		translation("", false);

		fclose(m_compressed);
		deallocate_trie(m_trie_root);
		std::cout << "Decompression is complete" << std::endl;
	}

	/// \brief This structure will be used to represent the trie
	struct huff_trie {
		huff_trie *zero{nullptr};
		huff_trie *one{nullptr};      //!< zero and one bit representation nodes of the m_trie_root
		unsigned char character{'\0'};//!< associated character in the trie node
	};

	huff_trie *m_trie_root = nullptr;
	FILE *m_compressed = nullptr;//!< file pinter to the compressed file
	unsigned long m_symbols = 0; //!< count of the file or folder m_symbols

	unsigned char m_current_byte = '\0';//!< unsigned char value
	                                    //!< that represents the current byte
	int m_current_bit_count = 0;        //!< integer value of current bits count

	/// \brief Reads how many folders/files the program is going to create inside
	/// the main folder. File count was written to the compressed file from least significant byte
	/// to most significant byte to make sure system's endianness does not affect the process and that is
	/// why we are processing size information like this
	int get_file_count() {
		int file_count;
		file_count = process_byte_number();
		file_count += Symbols * process_byte_number();
		return file_count;
	}

	/// \brief checks if next input is either a file or a folder
	///
	/// \return 1 for file and 0 for folder
	bool is_file() {
		bool val;
		if (m_current_bit_count == 0) {
			fread(&m_current_byte, 1, 1, m_compressed);
			m_current_bit_count = CHAR_BIT;
		}
		val = m_current_byte & Check;
		m_current_byte <<= 1;
		m_current_bit_count--;
		return val;
	}

	/// \brief process_byte_number reads 8 successive bits from compressed file
	/// (does not have to be in the same byte)
	///
	/// \return the latter 8 successive bits in unsigned char form
	unsigned char process_byte_number() {
		unsigned char val, temp_byte;
		fread(&temp_byte, 1, 1, m_compressed);
		val = m_current_byte | (temp_byte >> m_current_bit_count);
		m_current_byte = temp_byte << (CHAR_BIT - m_current_bit_count);
		return val;
	}

	/// \brief process_n_bits_to_string function reads n successive bits from the compressed file
	/// and stores it in a leaf of the translation trie,
	/// after creating that leaf and sometimes after creating nodes that are binding that leaf to the trie.
	///
	/// \param node - pointer to the trie node that is going to be created
	void process_n_bits_to_string(huff_trie *node) {
		unsigned char curr_char = process_byte_number();
		int len = process_byte_number();
		if (len == 0)
			len = Symbols;

		for (int i = 0; i < len; i++) {
			if (m_current_bit_count == 0) {
				fread(&m_current_byte, 1, 1, m_compressed);
				m_current_bit_count = CHAR_BIT;
			}

			if (m_current_byte & Check) {
				if (!(node->one))
					node->one = new huff_trie;

				node = node->one;
			} else {
				if (!(node->zero))
					node->zero = new huff_trie;

				node = node->zero;
			}
			m_current_byte <<= 1;
			m_current_bit_count--;
		}
		node->character = curr_char;
	}

	/// \brief Size was written to the compressed file from least significant byte
	/// to the most significant byte to make sure system's endianness
	/// does not affect the process and that is why we are processing size information like this
	///
	/// \return file size
	long int read_file_size() {
		long int size = 0;
		long int multiplier = 1;
		for (size_t i = 0; i < CHAR_BIT; i++) {
			size += process_byte_number() * multiplier;
			multiplier *= Symbols;
		}
		return size;
	}

	std::string create_new_file() {
		const int file_name_length = process_byte_number();
		auto *new_file = new unsigned char[file_name_length + 4];
		write_file_name(new_file, file_name_length);
		return reinterpret_cast<const char *>(new_file);
	}

	/// \brief Decodes current file's name and writes file name to new_file char array
	///
	/// \param new_file - char array to write file name to
	/// \param file_name_length - length of file name
	void write_file_name(unsigned char *new_file, int file_name_length) {
		huff_trie *node;
		new_file[file_name_length] = 0;
		for (int i = 0; i < file_name_length; i++) {
			node = m_trie_root;
			iterate_over_nodes(&node);
			new_file[i] = node->character;
		}
	}

	/// \brief This function translates compressed file from info that is now stored in the translation trie
	/// then writes it to a newly created file
	///
	/// \param path - path to the file that is being decoded
	/// \param size - size of the file that is being decoded
	void translate_file(const std::string &path, long int size) {
		huff_trie *node;
		FILE *fp_new = fopen(path.c_str(), "wb");
		for (long int i = 0; i < size; i++) {
			node = m_trie_root;
			iterate_over_nodes(&node);
			fwrite(&(node->character), 1, 1, fp_new);
		}
		fclose(fp_new);
	}

	/// \brief This function iterates over the translation trie and writes the file
	/// to a newly created file
	///
	/// \param node - pointer to the current node in the trie
	void iterate_over_nodes(huff_trie **node) {
		while ((*node)->zero || (*node)->one) {
			if (m_current_bit_count == 0) {
				fread(&m_current_byte, 1, 1, m_compressed);
				m_current_bit_count = CHAR_BIT;
			}
			if (m_current_byte & Check) {
				(*node) = (*node)->one;
			} else {
				(*node) = (*node)->zero;
			}
			m_current_byte <<= 1;
			m_current_bit_count--;
		}
	}

	/// \brief translation function is used for creating files and folders inside given path
	/// by using information from the compressed file.
	/// whenever it creates another file it will recursively call itself with path of the newly created file
	/// and in this way translates the compressed file.
	///
	/// \param path - the file will be created here
	/// \param change_path - if there are compressed folders - this flag allows recursion
	void translation(const std::string &path, bool change_path) {
		int file_count = get_file_count();
		for (int current_file = 0; current_file < file_count; current_file++) {
			if (is_file()) {
				long int size = read_file_size();

				std::string new_path = create_new_file();

				if (change_path)
					new_path.insert(0, path + "/");
				translate_file(new_path, size);
			} else {
				std::string new_path = create_new_file();

				if (change_path)
					new_path.insert(0, path + "/");

				mkdir(new_path.c_str(), MkdirPermission);
				translation(new_path, true);
			}
		}
	}

private:
	/// \brief deallocate_trie function is used for deallocating trie
	///
	/// \param node - pointer to the current node in the trie
	void deallocate_trie(huff_trie *node) {
		if (node->zero)
			deallocate_trie(node->zero);
		if (node->one)
			deallocate_trie(node->one);
		delete node;
	}
};
}// namespace storage::detail

class Decompressor {
private:
	using pimpl = storage::detail::DecompressorImpl;
	std::unique_ptr<pimpl> decompressor_impl;

public:
	/// \brief Constructor of the decompression class with which you can detail provided file
	///
	/// \param path - path to the file for detail
	explicit Decompressor(const std::string &path) {
		FILE *path_to_compressed;
		path_to_compressed = fopen(path.c_str(), "rb");
		if (!path_to_compressed) {
			//                        TODO: task for the logger
			std::cout << "Please provide valid file name" << std::endl;
			return;
		}
		decompressor_impl = std::make_unique<pimpl>(path_to_compressed);
	}

	/// \brief The main function of decompression class that do all the magic with provided m_files.
	void operator()() {
		(*decompressor_impl)();
	}
};
}// namespace decompression

#endif
