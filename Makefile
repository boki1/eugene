EU_BUILD_TYPE ?= Debug
EU_C_COMPILER ?= gcc-11
EU_CXX_COMPILER ?= g++-11
EU_BUILD_DIR ?= build
EU_THREADS ?= 4
EU_BUILD_TESTS := no
EU_BUILD_BENCHMARKS := no

all: config-test config-bench config build

clean: mrproper

mrproper:
	rm -rf ${EU_BUILD_DIR}
	rm -f build.log

config:
	mkdir -p build
	conan install . -if ${EU_BUILD_DIR} --build=missing
	cmake -S. -B${EU_BUILD_DIR}/ -GNinja\
		-DCMAKE_C_COMPILER=${EU_C_COMPILER}\
		-DCMAKE_CXX_COMPILER=${EU_CXX_COMPILER}\
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

test: config-test config build
	test --no-tests=error --rerun-failed --output-on-failure --test-dir ${EU_BUILD_DIR}
	$(eval EU_BUILD_TESTS=no)

bench: config-bench config build
	for bench in `ls ${EU_BUILD_DIR}/bin/Bench*`; do $$bench; done
	$(eval EU_BUILD_BENCHMARKS=no)

clean-test: clean test

clean-bench: clean bench

clean-build: clean build

clean-all: clean all

.PHONY: all clean clean-test clean-bench clean-build clean-all
