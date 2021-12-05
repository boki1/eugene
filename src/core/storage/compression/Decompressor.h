#ifndef STORAGE_DECOMPRESSOR_INCLUDED
#define STORAGE_DECOMPRESSOR_INCLUDED

#include <core/storage/Logger.h>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <fstream>
#include <filesystem>

static constexpr uint8_t Check = 0b10000000;
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
	///
	/// \param folder_name name of the folder to decompress (by default it is set to decompress all the files)
	void operator()(std::string_view folder_name = "") {
		fread(&m_symbols, 1, 1, m_compressed);
		if (m_symbols == 0)
			m_symbols = Symbols;

		m_trie_root = new huff_trie;
		for (unsigned long i = 0; i < m_symbols; i++)
			process_n_bits_to_string(m_trie_root);

		if (folder_name.empty()) {
			Logger::the().log(spdlog::level::info, "Decompressor: Decompressing all files...");
			translation("", false);
		} else {
			Logger::the().log(spdlog::level::info,
			                  R"(Decompressor: Decompressing files in file/folder: "{}")",
			                  folder_name);
			translation_search("", folder_name, false);
		}

		fclose(m_compressed);
		deallocate_trie(m_trie_root);
		Logger::the().log(spdlog::level::info, "Decompressor: Decompression is completed");
	}

	/// \brief This structure will be used to represent the trie
	struct huff_trie {
		huff_trie *zero{nullptr};
		huff_trie *one{nullptr};      //!< zero and one bit representation nodes of the m_trie_root
		char character{'\0'};//!< associated character in the trie node
	};

	huff_trie *m_trie_root = nullptr;
	FILE *m_compressed = nullptr;//!< file pinter to the compressed file
	unsigned long m_symbols = 0; //!< count of the file or folder m_symbols

	uint8_t m_current_byte = '\0';//!< uint8_t value
	//!< that represents the current byte
	int m_current_bit_count = 0;        //!< integer value of current bits count

	/// \brief Reads how many folders/files the program is going to create inside
	/// the main folder. File count was written to the compressed file from least significant byte
	/// to most significant byte to make sure system's endianness does not affect the process and that is
	/// why we are processing size information like this
	///
	/// \return integer value of the file count
	unsigned long get_file_count() {
		unsigned long file_count;
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
	/// \return the latter 8 successive bits in uint8_t form
	uint8_t process_byte_number() {
		uint8_t val;
		uint8_t temp_byte;
		fread(&temp_byte, 1, 1, m_compressed);
		val = (m_current_byte | (temp_byte >> m_current_bit_count));
		m_current_byte = temp_byte << (CHAR_BIT - m_current_bit_count);
		return val;
	}

	/// \brief process_n_bits_to_string function reads n successive bits from the compressed file
	/// and stores it in a leaf of the translation trie,
	/// after creating that leaf and sometimes after creating nodes that are binding that leaf to the trie.
	///
	/// \param node - pointer to the trie node that is going to be created
	void process_n_bits_to_string(huff_trie *node) {
		char curr_char = (char) process_byte_number();
		long len = process_byte_number();
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

	/// \brief Decodes current file's name and writes file name to new_file string
	///
	/// \param file_length - length of file name
	std::string get_name() {
		huff_trie *node;
		std::string new_file;
		int file_length = process_byte_number();

		for (int i = 0; i < file_length; i++) {
			node = m_trie_root;
			iterate_over_nodes(&node);
			new_file.push_back(node->character);
		}
		return new_file;
	}

	/// \brief This function translates compressed file from info that is now stored in the translation trie
	/// then writes it to a newly created file
	///
	/// \param path - path to the file that is being decoded
	/// \param size - size of the file that is being decoded
	void translate_file(const std::string &path, long int size) {
		huff_trie *node;

		std::filesystem::create_directories(path.substr(0, path.find_last_of('/')));
		std::ofstream new_file(path, std::ios::binary);
		for (long int i = 0; i < size; i++) {
			node = m_trie_root;
			iterate_over_nodes(&node);
			new_file << node->character;
		}
		new_file.close();
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
			if (m_current_byte & Check)
				(*node) = (*node)->one;
			else
				(*node) = (*node)->zero;
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
		unsigned long file_count = get_file_count();
		for (unsigned long current_file = 0; current_file < file_count; current_file++) {
			long int size = 0;
			bool file = is_file();
			if (file)
				size = read_file_size();

			std::string new_path = get_name();
			if (change_path)
				new_path.insert(0, path + "/");

			if (file) {
				if (size == 0) {
					Logger::the().log(spdlog::level::err, "Size cannot be "
														  "fetched from compressed file");
					return;
				}
				translate_file(new_path, size);
			} else {
				mkdir(new_path.c_str(), MkdirPermission);
				translation(new_path, true);
			}
		}
	}

	/// \brief translation function is used for creating files and folders inside given path
	/// by using information from the compressed file.
	/// whenever it creates another file it will recursively call itself with path of the newly created file
	/// and in this way translates the compressed file.
	///
	/// \param for_decompress - folder to decompress
	/// \param change_path - if there are compressed folders - this flag allows recursion
	void translation_search(const std::string &path, std::string_view for_decompress,
	                        bool change_path) {
		unsigned long file_count = get_file_count();
		for (unsigned long current_file = 0; current_file < file_count; current_file++) {
			long int size = 0;
			bool file = is_file();
			if (file)
				size = read_file_size();

			std::string new_path = get_name();
			const std::string curr_file = new_path;
			if (change_path)
				new_path.insert(0, path + "/");

			if (file) {
				if (curr_file == for_decompress) {
					if (size == 0) {
						Logger::the().log(spdlog::level::err, "Size cannot be "
						                                      "fetched from compressed file");
						return;
					}
					translate_file(new_path, size);
					break;
				}
				huff_trie *node;
				for (long int i = 0; i < size; i++) {
					node = m_trie_root;
					iterate_over_nodes(&node);
				}
			} else {
				if (curr_file == for_decompress) {
					translation(new_path, true);
					break;
				}
				translation_search(new_path, for_decompress, true);
			}
		}
		Logger::the().log(spdlog::level::debug, R"(Decompressor: File "{}" skipped)", path);
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
	explicit Decompressor(std::string_view path) {
		FILE *path_to_compressed;
		path_to_compressed = fopen(path.begin(), "rb");
		if (!path_to_compressed) {
			Logger::the().log(spdlog::level::err, R"(Decompressor: File not found: "{}")", path);
			return;
		}
		decompressor_impl = std::make_unique<pimpl>(path_to_compressed);
	}

	/// \brief The main function of decompression class that do all the magic with provided m_files.
	void operator()(std::string_view folder_name = "") {
		(*decompressor_impl)(folder_name);
	}
};
}// namespace decompression

#endif
