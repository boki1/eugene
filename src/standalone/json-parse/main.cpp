#include <nlohmann/json.hpp>

#include <iostream>
#include <string>

using json = nlohmann::json;

enum task_type {
	GET,
	POST,
	DELETE
};

struct task {
	std::string m_data;
	task_type m_type;
	std::string author;
	std::string destination;
};

void from_json(const json &json_obj, task &task) {
	json_obj.at("m_data").get_to(task.m_data);
	json_obj.at("task_type").get_to(task.m_type);
	json_obj.at("author").get_to(task.author);
	json_obj.at("destination").get_to(task.destination);
}

int main() {
	std::istringstream file(R"json({
  "tasks": [
    {
      "task": {
        "m_data": "0123456789",
		"task_type": 0,
        "author": "Ivan",
        "destination": "unknown"
      }
    }
  ]})json");

	json json_obj;
	file >> json_obj;

	json &arr = json_obj["tasks"];
	std::vector<task> tasks;

	std::for_each(arr.begin(), arr.end(), [&tasks](const json &o) {
	  if (auto it = o.find("task"); it != o.end()) {
		  tasks.push_back(it->get<task>());
	  }
	});

	for (task &task : tasks) {
		std::cout << task.m_data << '\n';
		std::cout << task.m_type << '\n';
		std::cout << task.author << '\n';
		std::cout << task.destination << '\n';
	}
}