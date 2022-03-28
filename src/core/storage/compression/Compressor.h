#pragma once

#include <climits>
#include <filesystem>
#include <fstream>
#include <numeric>

#include <core/Logger.h>

static constexpr size_t FileBits = 9;
static constexpr size_t FileSizeBits = 64;
static constexpr size_t FileCountBitsInsideCurrFolder = 16;
static constexpr size_t BitGroups_Second = 16;

namespace fs = std::filesystem;

namespace internal::compression {

template<typename T>
std::vector<uint8_t> compress(const T &);

template<typename T>
T decompress(std::vector<uint8_t> v);

}// namespace internal::compression

/// Compression algorithm is based on
/// <a href="http://www.huffmancoding.com/my-uncle/scientific-american">huffman coding</a>
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
/// **1** groups from third to seventh will be written as much as file count in that folder <br>
///    (this is argument_count-1(argc-1) for the main folder) <br>
/// **2** whenever we see a new folder we will write_from_ch (sixth) then start writing from third to seventh
namespace compression {
namespace storage::detail {
class CompressorInternal {
public:
	static std::string return_file_info(const std::string &path) {
		std::ifstream in(path, std::ifstream::binary);
		std::string buff(fs::file_size(path), 0);

		in.read(buff.data(), (long) buff.size());
		return buff;
	}

	static unsigned long get_file_folder_size(const std::string &path) {
		unsigned long size = 0;
		if (fs::is_directory(path))
			size += std::accumulate(
				fs::recursive_directory_iterator(path.c_str()),
				fs::recursive_directory_iterator(),
				0,
				[](auto sz, auto entry) {
				  return is_directory(entry) ? sz : sz + file_size(entry);
				});
		else
			size += fs::file_size(path);
		return size;
	}

	CompressorInternal() = default;

	CompressorInternal(std::vector<std::string> files, std::string new_compressed_name)
		: m_files(std::move(files)), m_compressed_name(std::move(new_compressed_name)) {
		std::fill(m_occurrence_symbol.begin(), m_occurrence_symbol.end(), 0);
	}

	void operator()() {
		for (const auto &file : m_files) {
			m_all_size += get_file_folder_size(file);
		}

		Logger::the().log(spdlog::level::info,
		                  R"(Compressor: started for "{0}" file/files/folder/folders with size: "{1}" bytes)",
		                  m_files.begin()->c_str(), m_all_size);

		m_total_bits = FileCountBitsInsideCurrFolder + FileBits * m_files.size();
		for (const auto &item : m_files) {
			for (const char *c = item.c_str(); *c; c++)
				m_occurrence_symbol[(uint8_t) *c]++;

			if (fs::is_directory(item))
				count_folder_bytes_freq(item);
			else
				count_file_bytes_freq(item);
		}

		for (const auto &item : m_occurrence_symbol) {
			if (item)
				m_symbols++;
		}

		m_trie.resize(m_symbols * 2 - 1);
		initialize_trie();

		Logger::the().log(spdlog::level::info,
		                  R"(Compressor: initialized the trie with "{0}" symbols and "{1}" nodes)",
		                  m_symbols, m_trie.size());

		m_compressed_fp = fopen(m_compressed_name.c_str(), "wb");
		fwrite(&m_symbols, 1, 1, m_compressed_fp);
		m_total_bits += CHAR_BIT;

		process();

		all_file_write();
		fclose(m_compressed_fp);

		Logger::the().log(spdlog::level::info,
		                  R"(Compressor: created compressed file: "{0}")",
		                  m_compressed_name);
		Logger::the().log(spdlog::level::info,
		                  "Compressor: compression is completed\n");
	}

	/// \brief This structure will be used to create the trie
	struct huff_trie {
		huff_trie *left{nullptr}, *right{nullptr}; //!< left and right nodes of the m_trie_root
		uint8_t character; //!< associated character in the m_trie_root node
		long int char_occurrence; //<! occurrences of the respective character
		std::string bit; //<! bit that represents Huffman code of current character

		huff_trie() = default;

		huff_trie(long int num, uint8_t c) : character(c), char_occurrence(num) {}

		bool operator<(const huff_trie &second) const {
			return this->char_occurrence < second.char_occurrence;
		}
	};

	std::vector<std::string> m_files;//!< path to the m_files for compress
	FILE *m_compressed_fp = nullptr; //!< file pointer to the new created compressed file

	std::array<long int, 256> m_occurrence_symbol;//!< long integer array that will contain
	//!< the number of occurrences of each symbol in the m_files

	std::vector<huff_trie> m_trie;//!< vector of detail's that represents trie

	std::string m_compressed_name; //!< new name of the compressed file
	unsigned long m_all_size = 0;  //!< size of the original file or folder
	unsigned long m_total_bits = 0;//!< count the compressed file size
	unsigned long m_symbols = 0;   //!< count of the file or folder m_symbols

	std::array<std::string, 256> m_char_huffbits;//!< transformation string
	//!< is put to m_str_huffbits array to make the compression process more time efficient

	uint8_t m_current_byte = '\0';//!< uint8_t value that represents the m_current_byte
	int m_current_bit_count = 0;        //!< integer value of m_current_bit_count

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
		for (unsigned long i = 0; i < m_occurrence_symbol.size(); ++i) {
			if (m_occurrence_symbol[i]) {
				e->right = nullptr;
				e->left = nullptr;
				e->char_occurrence = m_occurrence_symbol[i];
				e->character = i;
				e++;
			}
		}
		std::sort(m_trie.begin(), m_trie.end() - (long) (m_symbols - 1));

		huff_trie *min1 = m_trie.data();    //!< min1 and min2 represents nodes that has minimum weights
		huff_trie *min2 = m_trie.data() + 1;//!< min1 and min2 represents nodes that has minimum weights

		huff_trie *not_leaf = m_trie.data() + m_symbols;//!< not_leaf is the pointer that
		//!< traverses through nodes that are not leaves

		huff_trie *is_leaf = m_trie.data() + 2;//!< is_leaf is the pointer that traverses through leaves
		huff_trie *curr = m_trie.data() + m_symbols;

		for (unsigned long i = 0; i < m_symbols - 1; i++) {
			curr->char_occurrence = min1->char_occurrence + min2->char_occurrence;
			curr->left = min1;
			curr->right = min2;
			min1->bit = "1";
			min2->bit = "0";
			curr++;

			if (is_leaf >= m_trie.data() + m_symbols) {
				min1 = not_leaf;
				not_leaf++;
			} else {
				if (is_leaf->char_occurrence < not_leaf->char_occurrence) {
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
				if (is_leaf->char_occurrence < not_leaf->char_occurrence) {
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
	/// in long integer massive
	///
	/// \param path - string that represents the path to the file
	void count_file_bytes_freq(const std::string &path) {
		m_total_bits += FileSizeBits;
		const std::string buff = return_file_info(path);

		for (const auto &item : buff)
			m_occurrence_symbol[(uint8_t) item]++;
	}

	/// \brief This function counts usage frequency of bytes inside a folder
	///
	/// \param path - string that represents the path to the folder
	void count_folder_bytes_freq(std::string_view path) {
		m_total_bits += FileCountBitsInsideCurrFolder;

		for (const auto &entry : fs::recursive_directory_iterator(path)) {
			std::string next_path = entry.path();
			std::string curr_fdir_name = &next_path.substr(next_path.find_last_of('/'))[1];
			if (curr_fdir_name[0] == '.')
				continue;

			m_total_bits += FileBits;
			for (const auto &item : curr_fdir_name)
				m_occurrence_symbol[(uint8_t) item]++;

			if (entry.is_directory())
				m_total_bits += FileCountBitsInsideCurrFolder;
			else
				count_file_bytes_freq(next_path);
		}
	}

	/// \brief Process the compression and write the compressed file size
	/// (Manages second from part 2)
	void process() {
		for (auto it = m_trie.begin(); it < m_trie.begin() + (long) m_symbols; ++it) {
			m_char_huffbits[it->character] = it->bit;

			write_from_ch(it->character);
			write_from_ch(it->bit.size());
			m_total_bits += it->bit.size() + BitGroups_Second;

			write_bytes(it->bit);
			m_total_bits += it->bit.size() * (it->char_occurrence);
		}
		if (m_total_bits % CHAR_BIT)
			m_total_bits = m_total_bits / CHAR_BIT + 1;

		Logger::the().log(spdlog::level::info,
		                  R"(Compressor: The size of the sum of ORIGINAL m_files is: "{}" bytes)",
		                  m_all_size);
		Logger::the().log(spdlog::level::info,
		                  R"(Compressor: The size of the COMPRESSED file will be: "{}" bytes)",
		                  m_total_bits);
		Logger::the().
			log(spdlog::level::info, "Compressor: Compressed file's size will be [%{}] of the original file",
			    100 * ((float) m_total_bits / (float) m_all_size));

		if (m_total_bits / CHAR_BIT > m_all_size)
			Logger::the().
				log(spdlog::level::warn,
				    "Compressor: COMPRESSED FILES SIZE WILL BE HIGHER THAN THE SUM OF ORIGINALS");
	}

	/// \brief Writes all information in compressed order.
	/// (Manages from third to seventh of part 2)
	void all_file_write() {
		write_file_count(m_files.size());

		for (const auto &item : m_files) {
			if (!fs::is_directory(item)) {
				unsigned long size = fs::file_size(item);

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
	///
	/// \param path - string that represents the path to the file/folder
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
	/// \param path - string that represents the path to the file/folder
	void write_folder(std::string_view path) {
		write_folder_files_count(path);

		for (const auto &entry : fs::recursive_directory_iterator(path)) {
			const std::string next_path = entry.path();
			const std::string curr_fdir_name = &next_path.substr(next_path.find_last_of('/'))[1];
			if (curr_fdir_name[0] == '.')
				continue;

			if (!entry.is_directory()) {
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
	/// \param path - string that represents the path to the file/folder
	void write_file_content(const std::string &path) {
		const std::string buff = return_file_info(path);

		for (const auto &item : buff)
			write_bytes(m_char_huffbits[(uint8_t) item]);
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
			case '1': m_current_byte <<= 1;
				m_current_byte |= 1;
				m_current_bit_count++;
				break;
			case '0': m_current_byte <<= 1;
				m_current_bit_count++;
				break;

			default:
				Logger::the().log(spdlog::level::err,
				                  "Compressor: Function write_bytes incorrect configuration!");
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
			write_bytes(m_char_huffbits[(uint8_t) item]);
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

	/// \brief This function is writing number of m_files we re going to translate inside current
	/// folder to compressed file's 2 bytes.
	/// It is done like this to make sure that it can work on little, big or middle-endian systems
	/// (Manages third of part 2)
	///
	/// \param file_count - number of m_files that are provided (argc - 1)
	void write_file_count(unsigned long file_count) {
		uint8_t temp = file_count % 256;
		write_from_ch(temp);
		temp = file_count / 256;
		write_from_ch(temp);
	}

	/// \brief This function is used for writing the uChar to compressed file.
	/// It does not write it directly as one byte! Instead it mixes uChar and current byte, writes 8 bits of it
	/// and puts the rest to current byte for later use.
	///
	/// \param ch - character
	void write_from_ch(uint8_t ch) {
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
	/// \brief Constructor of the compression class with which you
	/// can compress provided m_files
	///
	/// \param args - arguments that are provided to the program
	/// \param compressed_name - name of the future compressed file
	explicit Compressor(const std::vector<std::string> &args,
	                    std::string_view compressed_name = "") {
		std::string new_compressed_name;

		if (args.empty())
			Logger::the().log(spdlog::level::err, "Compressor: No files provided!");
		if (!compressed_name.empty())
			new_compressed_name = compressed_name;
		else if (args.size() == 1)
			new_compressed_name = args[0] + ".huff";
		else
			new_compressed_name = "bundle.huff";

		compressor_internal = std::make_unique<pimpl>(args, new_compressed_name);
	}

	/// \brief The main function of compression class that do all
	/// the magic with provided m_files.
	void operator()() {
		(*compressor_internal)();
	}
};
}// namespace compression
