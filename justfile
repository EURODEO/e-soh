# list recipes
default:
  @just --list

set positional-arguments

all: lint build local test

# ---------------------------------------------------------------------------- #
#                                    test                                      #
# ---------------------------------------------------------------------------- #
# run linters
lint flag="":
	./ci/scripts/lint.sh ${1}

# run tests
test flag="-i":
	./ci/scripts/test.sh ${1}

# ---------------------------------------------------------------------------- #
#                                    build                                     #
# ---------------------------------------------------------------------------- #
build:
	./ci/scripts/build.sh


# # ---------------------------------------------------------------------------- #
# #                                     local                                    #
# # ---------------------------------------------------------------------------- #
# run docker-compose
local:
	./ci/scripts/local.sh

# run pip-compile for all the requirement files
pip-compile:
	./ci/scripts/pip-compile.sh

# Already included in the build step, but can also be used seperate.
copy-proto:
    ./ci/scripts/copy-protobuf.sh
