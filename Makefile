EU_BUILD_TYPE ?= Debug
EU_C_COMPILER ?= gcc-11
EU_CXX_COMPILER ?= g++-11
EU_BUILD_DIR ?= build
EU_THREADS ?= 4
EU_BUILD_TESTS := no
EU_BUILD_BENCHMARKS := no
EU_CONAN_PROFILE ?= ${HOME}/.conan/profiles/default

all: config-test config-bench config build

exec-all: all exec-test exec-bench

format:
	find src \( -name '*.cpp' -o -name '*.h' \) -exec clang-format -i '{}' \;
	find . -name "CMakeLists.txt" -exec cmake-format -i '{}' \;

lint:
	run-clang-tidy.py -header-filter='.*' -checks='*'

clean: mrproper

mrproper:
	rm -rf ${EU_BUILD_DIR}
	rm -f build.log

dep-setup:
	mkdir -p build
	CXX=${EU_CXX_COMPILER} CC=${EU_C_COMPILER}\
		conan install . --profile ${EU_CONAN_PROFILE} -if ${EU_BUILD_DIR} --build=missing

config: dep-setup
	cmake -S. -B${EU_BUILD_DIR}/ -GNinja\
		-DCMAKE_C_COMPILER=${EU_C_COMPILER}\
		-DCMAKE_CXX_COMPILER=${EU_CXX_COMPILER}\
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON\
		-DCMAKE_BUILD_TYPE=${EU_BUILD_TYPE}\
		-DEU_BUILD_TESTS=${EU_BUILD_TESTS}\
		-DEU_BUILD_BENCHMARKS=${EU_BUILD_BENCHMARKS}
	ln -sf ${EU_BUILD_DIR}/compile_commands.json compile_commands.json

config-test:
	$(eval EU_BUILD_TESTS=yes)

config-bench:
	$(eval EU_BUILD_BENCHMARKS=yes)

build: config
	ninja -C${EU_BUILD_DIR} -j${EU_THREADS}

test: config-test config build exec-test

exec-test:
	ctest --no-tests=error --rerun-failed --output-on-failure --test-dir ${EU_BUILD_DIR}
	$(eval EU_BUILD_TESTS=no)

exec-bench:
	for bench in `ls ${EU_BUILD_DIR}/bin/Bench*`; do $$bench; done
	$(eval EU_BUILD_BENCHMARKS=no)

bench: config-bench config build exec-bench

clean-test: clean test

clean-bench: clean bench

clean-build: clean build

clean-all: clean all

clean-exec-all: clean-all exec-all

.PHONY: all clean clean-test clean-bench clean-build clean-all clean-exec-all test bench
