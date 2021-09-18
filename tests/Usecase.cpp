namespace example_usecase {

struct s {
  uint32_t num;
  std::string num_as_str;
};

class example_dev {
private:
	static constexpr int Invalid_fd = -1;
public:
	explicit example_dev(std::string fname)
			:m_fname{std::move(fname)} { }

	void open() { }
	void close() { m_fd = Invalid_fd; }
	void read() { }
	void write() { }
	void seek() { }

private:
	int m_fd{Invalid_fd};
	std::string m_fname;
};

struct index_config {
  using Key = std::string;
  using Val = struct s;
  using StorageDev = example_dev;

  static constexpr bool ApplyCompression{false};
  static constexpr uint64_t BranchingFactor{8};
};

static_assert(indexing::IndexConfig<index_config>);

}

