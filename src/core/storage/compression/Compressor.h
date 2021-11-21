#ifndef STORAGE_COMPRESSOR_INCLUDED
#define STORAGE_COMPRESSOR_INCLUDED

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <utility>
#include <vector>

static constexpr size_t Folder = 16;
static constexpr size_t OsBites = 64;

using dynamic_bitset = std::vector<bool>;

namespace fs = std::filesystem;

namespace internal::compression {

template<typename T>
std::vector<uint8_t> compress(const T &);

template<typename T>
T decompress(std::vector<uint8_t> v);

}// namespace internal::compression

/// Compression algorithm is based on
/// <a href="https://en.wikipedia.org/wiki/Huffman_coding#Basic_technique">huffman coding</a>
/// and is separated in 2 parts:
/// <br> <br>
/// <h2>Part 1</h2>
/// <ol type = "1">
///     <li>Size information</li>
///     <li>Counting usage frequency of unique bytes and unique byte count</li>
///     <li>Creating the base of the translation map</li>
///     <li>Creating the translation m_trie_root inside the translation map by weight distribution</li>
///     <li>Adding strings from top to bottom to create translated versions of unique bytes</li>
/// </ol>
/// <br>
/// <h2>Part 2</h2>
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
///             <li>(bits) -> transformed version of current input_file's or folder's name</li>
///         </ul>
///     </li>
///     <li>seventh (a lot of bits) -> transformed version of current input_file (IF FILE)</li>
/// </ul>
/// **1** groups from fifth to eighth will be written as much as file count in that folder <br>
///    (this is argument_count-1(argc-1) for the main folder) <br>
/// **2** whenever we see a new folder we will write_from_ch seventh then start writing from fourth to eighth
namespace compression {
namespace storage::detail {
class CompressorInternal {
public:
	static std::string return_file_info(const std::string &path) {
		std::ifstream in(path, std::ifstream::binary);
		std::string buff(fs::file_size(path), 0);

		in.read(buff.data(), buff.size());
		return buff;
	}

	CompressorInternal() = default;

	CompressorInternal(std::vector<std::string> files, std::string new_compressed_name)
	    : m_files(std::move(files)), m_compressed_name(std::move(new_compressed_name)) {}

	void operator()() {
		for (const auto &file : m_files) {
			if (fs::is_directory(file))
				m_all_size += std::accumulate(
				        fs::recursive_directory_iterator(file.c_str()), fs::recursive_directory_iterator(), 0,
				        [](auto sz, auto entry) { return is_directory(entry) ? sz : sz + file_size(entry); });
			else
				m_all_size += fs::file_size(file);
		}
		m_total_bits = Folder + 9 * m_files.size();
		for (const auto &item : m_files) {
			for (const char *c = item.c_str(); *c; c++)
				m_occurrence_symbol[*c]++;

			if (fs::is_directory(item))
				count_folder_bytes_freq(item);
			else
				count_file_bytes_freq(item);
		}

		m_symbols = m_occurrence_symbol.size();

		m_trie.resize(m_symbols * 2 - 1);
		initialize_trie();

		m_compressed_fp = fopen(m_compressed_name.c_str(), "wb");
		fwrite(&m_symbols, 1, 1, m_compressed_fp);
		m_total_bits += CHAR_BIT;

		process();
		all_file_write();

		fclose(m_compressed_fp);
		std::cout << std::endl
		          << "Created compressed file: " << m_compressed_name << std::endl;
		std::cout << "Compression is complete" << std::endl;
	}

	/// \brief This structure will be used to create the trie
	struct huff_trie {
		huff_trie *left{nullptr}, *right{nullptr};//!< left and right nodes of the m_trie_root
		unsigned char character;                  //!< associated character in the m_trie_root node
		long int number;                          //<! occurrences of the respective character
		dynamic_bitset bit;                       //<! bit that represents Huffman code of current character

		huff_trie() = default;

		huff_trie(long int num, unsigned char c) : character(c), number(num) {}

		bool operator<(const huff_trie &second) const {
			return this->number < second.number;
		}
	};

	std::vector<std::string> m_files;//!< path to the m_files for compress

	FILE *m_compressed_fp = nullptr;                 //!< file pinter to the new created compressed file
	std::map<unsigned char, int> m_occurrence_symbol;//!< key-value pair
	                                                 //!< in which keys are m_symbols and values are their number of occurrences

	std::vector<huff_trie> m_trie;//!< vector of detail's that represents trie

	std::string m_compressed_name; //!< new name of the compressed file
	unsigned long m_all_size = 0;  //!< size of the original file or folder
	unsigned long m_total_bits = 0;//!< count the compressed file size
	unsigned long m_symbols = 0;   //!< count of the file or folder m_symbols

	std::map<unsigned char, std::string> m_char_huffbit;//!< transformation string
	                                                    //!< is put to m_str_arr array to make the compression process more time efficient
	unsigned char m_current_byte = '\0';                //!< unsigned char value
	                                                    //!< that represents the m_current_byte
	int m_current_bit_count = 0;                        //!< integer value of m_current_bit_count

	/// \brief First creates the base of trie(and then sorting them by ascending frequencies).
	/// Then creates pointers that traverses through leaf's.
	/// At every cycle, 2 of the least weighted nodes will be chosen to
	/// create a new node that has weight equal to sum of their weights combined.
	/// After we are done with these nodes they will become children of created nodes
	/// and they will be passed so that they wont be used in this process again.
	/// Finally, we are adding the bytes from m_trie_root to leaf's
	/// and after this is done every leaf will have a transformation string that corresponds to it
	/// It is actually a very neat process. Using 4th and 5th code blocks, we are making sure that
	/// the most used character is using least number of bits.
	/// Specific number of bits we re going to use for that character is determined by weight distribution
	void initialize_trie() {
		huff_trie *e = m_trie.data();
		for (const auto &[key, value] : m_occurrence_symbol) {
			e->right = nullptr;
			e->left = nullptr;
			e->number = value;
			e->character = key;
			e++;
		}
		std::sort(m_trie.begin(), m_trie.end() - (long) (m_symbols - 1));

		huff_trie *min1 = m_trie.data();                //!< min1 and min2 represents nodes that has minimum weights
		huff_trie *min2 = m_trie.data() + 1;            //!< min1 and min2 represents nodes that has minimum weights
		huff_trie *not_leaf = m_trie.data() + m_symbols;//!< not_leaf is the pointer that traverses through nodes that are not leaves
		huff_trie *is_leaf = m_trie.data() + 2;         //!< is_leaf is the pointer that traverses through leaves and
		huff_trie *curr = m_trie.data() + m_symbols;

		for (unsigned long i = 0; i < m_symbols - 1; i++) {
			curr->number = min1->number + min2->number;
			curr->left = min1;
			curr->right = min2;
			min1->bit.push_back(true);
			min2->bit.push_back(false);
			curr++;

			if (is_leaf >= m_trie.data() + m_symbols) {
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

			if (is_leaf >= m_trie.data() + m_symbols) {
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
		for (huff_trie *huff = m_trie.data() + m_symbols * 2 - 2; huff > m_trie.data() - 1; huff--) {
			if (huff->left)
				huff->left->bit.insert(huff->left->bit.begin(), huff->bit.begin(), huff->bit.end());
			if (huff->right)
				huff->right->bit.insert(huff->right->bit.begin(), huff->bit.begin(), huff->bit.end());
		}
	}

	/// \brief Count usage frequency of bytes inside the file and store the information
	/// in long integer massive (bytesFreq) and parallel
	///
	/// \param path - char sequence representing the path to a folder of file
	void count_file_bytes_freq(const std::string &path) {
		m_total_bits += OsBites;
		const std::string buff = return_file_info(path);

		for (const auto &item : buff)
			m_occurrence_symbol[item]++;
	}

	/// \brief This function counts usage frequency of bytes inside a folder
	///
	/// \param path - char sequence representing the path to a folder of file
	void count_folder_bytes_freq(std::string_view path) {
		m_total_bits += Folder;

		for (const auto &entry : fs::recursive_directory_iterator(path)) {
			std::string next_path = entry.path();
			std::string curr_fdir_name = &next_path.substr(next_path.find_last_of('/'))[1];
			if (curr_fdir_name[0] == '.')
				continue;

			m_total_bits += 9;
			for (const auto &item : curr_fdir_name)
				m_occurrence_symbol[item]++;

			if (entry.is_directory())
				m_total_bits += Folder;
			else
				count_file_bytes_freq(next_path);
		}
	}

	/// \brief Process the compression and write the compressed file size
	/// (Manages second from part 2)
	void process() {
		for (auto it = m_trie.begin(); it < m_trie.begin() + m_symbols; ++it) {
			std::vector<char> v_char;
			std::transform(it->bit.begin(), it->bit.end(), std::back_inserter(v_char),
			               [](const auto &x) { return x ? '1' : '0'; });
			std::string huffbit(v_char.begin(), v_char.end());
			m_char_huffbit[it->character] = huffbit;

			write_from_ch(it->character);
			write_from_ch(it->bit.size());
			m_total_bits += it->bit.size() + 16;

			write_bytes(huffbit);
			m_total_bits += it->bit.size() * (it->number);
		}
		if (m_total_bits % CHAR_BIT)
			m_total_bits = (m_total_bits / CHAR_BIT + 1) * CHAR_BIT;

		std::cout << "The size of the sum of ORIGINAL m_files is: " << m_all_size << " bytes" << std::endl;
		std::cout << "The size of the COMPRESSED file will be: " << m_total_bits / CHAR_BIT << " bytes" << std::endl;
		std::cout << "Compressed file's size will be [%" << 100 * ((float) m_total_bits / CHAR_BIT / (float) m_all_size)
		          << "] of the original file"
		          << std::endl;
		if (m_total_bits / CHAR_BIT > m_all_size)
			std::cout << std::endl
			          << "WARNING: COMPRESSED FILE'S SIZE WILL BE HIGHER THAN THE SUM OF ORIGINALS" << std::endl
			          << std::endl;
	}

	/// \brief Writes all information in compressed order.
	/// (Manages from third to seventh of part 2)
	void all_file_write() {
		write_file_count(m_files.size());

		FILE *original_fp;
		for (const auto &item : m_files) {
			if (!fs::is_directory(item)) {
				original_fp = fopen(item.c_str(), "rb");
				fseek(original_fp, 0, SEEK_END);
				long size = ftell(original_fp);
				rewind(original_fp);

				if (m_current_bit_count == CHAR_BIT) {
					fwrite(&m_current_byte, 1, 1, m_compressed_fp);
					m_current_bit_count = 0;
				}
				m_current_byte <<= 1;
				m_current_byte |= 1;
				m_current_bit_count++;

				write_file_size(size);
				write_file_name(item);
				write_file_content(item);
				fclose(original_fp);
			} else {
				if (m_current_bit_count == CHAR_BIT) {
					fwrite(&m_current_byte, 1, 1, m_compressed_fp);
					m_current_bit_count = 0;
				}
				m_current_byte <<= 1;
				m_current_bit_count++;

				write_file_name(item);

				write_folder(item);
			}
		}

		if (m_current_bit_count == CHAR_BIT)
			fwrite(&m_current_byte, 1, 1, m_compressed_fp);
		else {
			m_current_byte <<= CHAR_BIT - m_current_bit_count;
			fwrite(&m_current_byte, 1, 1, m_compressed_fp);
		}
	}

	/// \brief Open dir path and count regular m_files in it.
	/// Then write this count in compressed file. (Manages third of part 2)
	void write_folder_files_count(std::string_view path) {
		int file_count = 0;
		for (const auto &entry : fs::directory_iterator(path)) {
			const std::string next_path = entry.path();
			const std::string curr_fdir_name = &next_path.substr(next_path.find_last_of('/'))[1];
			if (curr_fdir_name[0] == '.')
				continue;
			file_count++;
		}
		write_file_count(file_count);
	}

	/// \brief This function manages all other function to make folder compression available.
	/// (Manages from third to seventh of part 2 for a folder)
	///
	/// \param path - folder name
	void write_folder(std::string_view path) {
		write_folder_files_count(path);

		for (const auto &entry : fs::recursive_directory_iterator(path)) {
			const std::string next_path = entry.path();
			const std::string curr_fdir_name = &next_path.substr(next_path.find_last_of('/'))[1];
			if (curr_fdir_name[0] == '.')
				continue;

			if (!entry.is_directory()) {
				FILE *original_fp = fopen(next_path.c_str(), "rb");
				if (m_current_bit_count == CHAR_BIT) {
					fwrite(&m_current_byte, 1, 1, m_compressed_fp);
					m_current_bit_count = 0;
				}
				m_current_byte <<= 1;
				m_current_byte |= 1;
				m_current_bit_count++;

				write_file_size(entry.file_size());
				write_file_name(curr_fdir_name);
				write_file_content(next_path);
				fclose(original_fp);
			} else {
				if (m_current_bit_count == CHAR_BIT) {
					fwrite(&m_current_byte, 1, 1, m_compressed_fp);
					m_current_bit_count = 0;
				}
				m_current_byte <<= 1;
				m_current_bit_count++;

				write_file_name(curr_fdir_name);

				write_folder_files_count(next_path);
			}
		}
	}

	/// \brief This function translates and writes bytes from current input file to the compressed file.
	/// (Manages seventh of part 2)
	///
	/// \param original_fp - file pointer to original file
	/// \param size - size of the original file
	void write_file_content(const std::string &path) {
		const std::string buff = return_file_info(path);

		for (const auto &item : buff)
			write_bytes(m_char_huffbit[item]);
	}

	/// \brief Writes provided string bytes to the new compressed file
	///
	/// \param for_write - string that will be written
	void write_bytes(std::string_view for_write) {
		for (const auto &item : for_write) {
			if (m_current_bit_count == CHAR_BIT) {
				fwrite(&m_current_byte, 1, 1, m_compressed_fp);
				m_current_bit_count = 0;
			}
			switch (item) {
			case '1':
				m_current_byte <<= 1;
				m_current_byte |= 1;
				m_current_bit_count++;
				break;
			case '0':
				m_current_byte <<= 1;
				m_current_bit_count++;
				break;

				//                                logger case
			default: continue;
			}
		}
	}

	/// \brief This function writes bytes that are translated from current input file's name to the compressed file.
	/// (Manages sixth of part 2)
	///
	/// \param file_name - name of the file
	void write_file_name(std::string_view file_name) {
		write_from_ch(file_name.size());
		for (const auto &item : file_name)
			write_bytes(m_char_huffbit[item]);
	}

	/// \brief This function is writing byte count of current input file to compressed file using 8 bytes.
	/// It is done like this to make sure that it can work on little, big or middle-endian systems.
	/// (Manages fifth of part 2)
	///
	/// \param size - size of the original file
	void write_file_size(unsigned long size) {
		for (size_t i = 0; i < CHAR_BIT; i++) {
			write_from_ch(size % 256);
			size /= 256;
		}
	}

	/// \brief This function is writing number of m_files we re going to translate inside current folder to compressed file's 2 bytes
	/// It is done like this to make sure that it can work on little, big or middle-endian systems
	/// (Manages third of part 2)
	///
	/// \param file_count - number of m_files that are provided (argc - 1)
	void write_file_count(unsigned long file_count) {
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
	void write_from_ch(unsigned char ch) {
		m_current_byte <<= CHAR_BIT - m_current_bit_count;
		m_current_byte |= (ch >> m_current_bit_count);
		fwrite(&m_current_byte, 1, 1, m_compressed_fp);
		m_current_byte = ch;
	}
};
}// namespace storage::detail
class Compressor {
private:
	using pimpl = storage::detail::CompressorInternal;
	std::unique_ptr<pimpl> compressor_internal;

public:
	/// \brief Constructor of the compression class with which you can compress provided m_files
	///
	/// \param argc - number of m_files for compress
	/// \param argv - path's to m_files for compress
	explicit Compressor(const std::vector<std::string> &args, std::string_view compressed_name = "") {
		std::string new_compressed_name;

		//                if (args.empty())
		//                        TODO: Logger task
		if (!compressed_name.empty())
			new_compressed_name = compressed_name;
		else if (args.size() == 1)
			new_compressed_name = args[0] + ".huff";
		else
			new_compressed_name = "bundle.huff";

		compressor_internal = std::make_unique<pimpl>(args, new_compressed_name);
	}

	/// \brief The main function of compression class that do all the magic with provided m_files.
	void operator()() {
		(*compressor_internal)();
	}
};
}// namespace compression

#endif