#include <cstdint>
#include <string>
#include <iostream>
#include <variant>
#include <filesystem>
namespace fs = std::filesystem;

#include <fmt/core.h>

#include <core/Config.h>
#include <core/storage/btree/Btree.h>

#include <nop/structure.h>

using namespace internal;
using namespace internal::storage;
using namespace internal::storage::btree;

struct Person {
	std::uint64_t phonenumber;
    std::uint32_t age;
    std::string name;

	friend std::istream &operator>>(std::istream &is, Person &person) {
		fmt::print("Phonenumber: ");
		is >> person.phonenumber;
		fmt::print("Age: ");
		is >> person.age;
		std::cout << "Name: ";
		is >> person.name;
		return is;
	}

	friend std::ostream &operator<<(std::ostream &os, const Person &person) {
		os << "Phonenumber: " << person.phonenumber << '\n';
		os << "Age: " << person.age << '\n';
		os << "Name: " << person.name << '\n';
		return os;
	}

	using Id = std::uint64_t;
	NOP_STRUCTURE(Person, phonenumber, age, name);
};

template<>
struct fmt::formatter<Person> {
	template<typename ParseContext>
	constexpr auto parse(ParseContext &ctx) {
		return ctx.begin();
	}

	template<typename FormatContext>
	auto format(const Person &pp, FormatContext &ctx) {
		std::stringstream sstr;
		sstr << pp;
		return fmt::format_to(ctx.out(), "{}", sstr.str());
	}
};

EU_CONFIG_DYN(P, Person::Id, Person);
using T = Btree<PConfig>;

struct Db {
	bool db_open;
	std::string db_name;
	T *db_tree;
};

static void db_help([[maybe_unused]] Db &) {
	fmt::print("=== Help for Id-to-Person KV store ===\n");
	fmt::print(" Operations: \n");
	fmt::print(" open _filename_  open db\n");
	fmt::print(" close             close db\n");
	fmt::print(" insert _key_ _val_ insert entry into db\n");
	fmt::print(" update _key_ _val_ update entry into db\n");
	fmt::print(" remove _key_       remove entry into db\n");
	fmt::print(" get _key_       get entry val from db\n");
	fmt::print(" present _key_       check whether key is present in db\n");
	fmt::print(" quit             quit\n");
	fmt::print(" help             show help menu\n");
}

static Person g_p;
static Person::Id g_id;

static Person::Id g_id_counter = 0;

static void db_open(Db &db) {
	using enum btree::ActionOnConstruction;
	std::cout << "Db name: ";
	std::string infname;
	std::cin >> infname;
	auto action_flag = Bare;
	if (fs::exists(fs::current_path() / infname)) {
		std::cout << " - '" << infname << "' exists. Load it (l) or create a new one (b)? ";
		action_flag = Load;
		char b;
		std::cin >> b;
		if (b == 'b')
			action_flag = Bare;
	}

	if (db.db_open)
		delete db.db_tree;
	db.db_tree = new T{infname, action_flag};
	db.db_open = true;
}

static void db_close(Db &db) {
	db.db_tree->save();
	db.db_open = false;
	db.db_name.clear();
	delete db.db_tree;
	db.db_tree = nullptr;
}

static void db_insert(Db &db) {
	std::cin >> g_p;
	auto id = g_id_counter + 1;
	bool flag = std::holds_alternative<T::InsertedEntry>(db.db_tree->insert(id, g_p));
	fmt::print(" -- insert: {}; Access with Id = {}\n", (flag ? "succeeded" : "failed"), id);
	if (flag)
		g_id_counter = id;
}

static void db_update(Db &db) {
	std::cout << "Id: ";
	std::cin >> g_id;
	std::cin >> g_p;
	bool flag = std::holds_alternative<T::InsertedEntry>(db.db_tree->update(g_id, g_p));
	fmt::print(" -- update: {}\n", (flag ? "succeeded" : "failed"));
}

static void db_remove(Db &db) {
	std::cin >> g_id;
	bool flag = std::holds_alternative<T::RemovedVal>(db.db_tree->remove(g_id));
	fmt::print(" -- remove {}\n", (flag ? "succeeded" : "failed"));
}

static void db_get(Db &db) {
	std::cin >> g_id;
	if (auto rv = db.db_tree->get(g_id); rv)
		std::cout << " -- get:\n" << *rv << '\n';
	else
		fmt::print(" -- get: no such entry\n");
}

static void db_present(Db &db) {
	std::cin >> g_id;
	auto flag = db.db_tree->contains(g_id);
	fmt::print(" -- present: {}\n", (flag ? "present" : "not present"));
}

int main(int, char**) { fmt::print("=== Id-to-Person KV store ===\n");
	std::string cmd;
	using cmd_type = void(*)(Db &);
	const std::unordered_map<std::string, cmd_type> cmds {
		{ "open", db_open },
		{ "close", db_close },
		{ "insert", db_insert },
		{ "update", db_update },
		{ "remove", db_remove },
		{ "get", db_get },
		{ "present", db_present },
		{ "help", db_help }
	};

	for (Db db {
			.db_open = false,
			.db_name = "",
			.db_tree = nullptr
			};;) {
		fmt::print("> ");
		std::cin >> cmd;
		if (!cmds.contains(cmd)) {
			fmt::print(" - Unknown command\n");
			continue;
		}
		if (cmd == "quit") {
			break;
		}
		(*cmds.at(cmd))(db);
		fmt::print("\n");
	}
	return 0;
}
