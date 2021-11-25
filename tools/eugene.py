import sys
import os
from random import choice
import subprocess as sp
from collections import namedtuple as nt


def setup(pip):
    reqs = ["cmake-format", "colorama", "pylev"]
    for req in reqs:
        cmd = f"{pip} install {req}"
        print(cmd)
        sp.run(cmd.split())


try:
    from colorama import Fore as color
    from colorama import Style
    from pylev import classic_levenshtein as lev
except ImportError:
    if input("Install missing modules? Y/n ").lower() != 'y':
        sys.exit(0)

    pip_cand = input("How is `pip` spelled - pip2, pip3, etc.? [default: `pip`] ") if not None else "pip"
    print(f"Entered '{pip_cand}'")
    setup(pip_cand if pip_cand else "pip")
    from collections import namedtuple as nt
    from colorama import Fore as color
    from colorama import Style
    from pylev import classic_levenshtein as lev

previous_color_ = color.WHITE


def get_color():
    global previous_color_
    colors = [color.GREEN, color.YELLOW, color.BLUE, color.MAGENTA, color.CYAN]
    while True:
        new_color = choice(colors)
        if new_color != previous_color_:
            previous_color_ = new_color
            return new_color


def print_style(*args, **kwargs):
    print(f"\n{get_color()} > ", end='')
    print(*args, **kwargs)
    print(Style.RESET_ALL)


def print_scream(string):
    print(f"{color.RED}{string}{Style.RESET_ALL}")


def do_if(cmd, msg, qprompt=False):
    if qprompt:
        out = input(msg + ' Ok? ').lower()
        if out != '' and out != 'y':
            return
    sp.run(cmd.split())


Param = nt('Param', 'field desc subcmds')
AvailableParams = list([
    Param('build', 'build project only', ['test: builds tests', 'doc: builds doc']),
    Param('test', 'build tests', ['runtest: runs tests']),
    Param('lint', 'run local linters', ['style: clang-format', 'tidy: ctidy']),
    Param('doc', 'build local documentation', ['updoc: uploads to gh-pages']),
    Param('clean', 'clean metadata', ['ignored: removes all files and dirs from the .gitignore']),
])

retry_avail_ = True


def autocomplete(arg):
    global retry_avail_
    if not retry_avail_:
        return False
    retry_avail_ = not retry_avail_

    required = 0.5 * len(arg)
    avail_args = [param.field for param in AvailableParams]
    for avail_arg in avail_args:
        lev_ = lev(arg, avail_arg)
        if lev_ <= required:
            print_style(f"Proceed with '{avail_arg}'? Y/n: ", end='')
            if input().lower() == 'y':
                print()
                return avail_arg
            break
    print()
    return None


def do_config_cmd(compiler_matrix) -> bool:
    os.environ["CC"] = compiler_matrix[0]
    os.environ["CXX"] = compiler_matrix[1]
    cmake_command = f"cmake -S. -Bbuild -GNinja \
            -DCMAKE_C_COMPILER={compiler_matrix[0]} \
            -DCMAKE_CXX_COMPILER={compiler_matrix[1]}".split()
    conan_install_command = f"conan install . -if build --build=missing".split()
    print_style("Trying to configure project ...")
    try:
        sp.check_call(conan_install_command)
        sp.run(conan_install_command)
        sp.check_call(cmake_command)
        sp.run(cmake_command)
        print_style("Configuration successful!")
    except:
        print_style("Configuration failed :(")
        return False
    return True


def do_config():
    try:
        os.mkdir("build/")
        print("INFO: Created 'build/' directory")
    except OSError:
        print_style("INFO: 'build/' directory already exists")

    if not do_config_cmd(('gcc-11', 'g++-11')):
        if not do_config_cmd(('gcc', 'g++')):
            sys.exit(-1)

def do_build(test=False, doc=False):
    do_config()
    ninja_command="ninja -Cbuild -j4"
    if test:
        os.environ["EUGENE_BUILD_TESTS"] = "1"
    print_style(f"Building project{' and tests' if test else ''}")
    sp.run(ninja_command.split())

    if doc:
        assert False, "Building doc is not yet supported"


def do_run():
    print("Running project")


def do_run_test():
    print_style("Running tests")
    runtest_cmd = "ctest --rerun-failed --output-on-failure --test-dir build/".split()
    sp.run(runtest_cmd)


def do_updoc():
    print("Uploading docs")


def do_lint():
    print("Linting")
    clangtidy_cmd = "clang-tidy -file --check=modernize,readability,performance"
    cmake_format_cmd = 'find . -name "CMakeLists.txt" | xargs cmake-format -i'
    clang_format_all_cmd = "clang-format-all src/core src/eugene-api".split()
    sp.run(clang_format_all_cmd)
    sp.run(cmake_format_cmd, shell=True, stdout=sp.PIPE)


def do_clean(is_kind=False):
    do_if("rm -rf build", "Removing build directory", is_kind)
    do_if("rm -f build.log", "Removing build logs", is_kind)

    print_style("Clean without scratching")


def usage():
    global AvailableParams
    usage_str = "\n\tEugene multifunctional script\n\t=============================\n\n"
    for f in AvailableParams:
        usage_str += f"\t• {get_color()}{f.field}{Style.RESET_ALL} is used to {f.desc}\n"
        for sc in f.subcmds:
            usage_str += f'\t\t - {sc}\n'
    print(usage_str)


def parse(cmd_line, cmd, cmd_args, cmd_argc):
    assert cmd_argc <= 1

    if cmd == "build":
        if cmd_argc == 1:
            test, doc = 'test' in cmd_args[0], 'doc' in cmd_args[0]
        else:
            test, doc = False, False
        do_build(test, doc)

    elif cmd == 'config':
        assert cmd_argc == 0
        do_config()
    
    elif cmd == 'test':
        do_build(test=True, doc=False)
        if cmd_argc == 1 and 'run' in cmd_args[0]:
            do_run_test()

    elif cmd == 'clean' or cmd == 'mrproper':
        forced = (cmd_argc == 1 and 'force' in cmd_args[0]) or cmd_argc == 0
        kind = cmd_argc == 1 and 'kind' in cmd_args[0]
        do_clean(kind and not forced)

    elif cmd == 'lint':
        assert cmd_argc == 0
        do_lint()

    elif cmd == 'doc':
        do_build(test=False, doc=True)
        if cmd_argc == 1 and 'up' in cmd_args[0]:
            do_updoc()

    elif cmd == 'version':
        print("Eugene 0.01")

    else:
        usage(), print('\n')
        actual_cmd = autocomplete(cmd)
        if actual_cmd:
            parse(cmd_line, actual_cmd, cmd_args, cmd_argc)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        sys.exit(0)

    args = [arg.lower() for arg in sys.argv]
    parse(args[1:], args[1], args[2:], len(args) - 2)
