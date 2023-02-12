#### PROJECT SETTINGS ####
# Compiler used
CXX = clang++
# Extension of source files used in the project
SRC_EXT = cpp
# Path to the source directory, relative to the makefile
SRC_PATH = ./src
# General compiler flags
COMPILE_FLAGS = -std=c++17 -march=haswell -D__FAAS_SRC \
	-Wall -Wextra -Werror -Wno-unused-parameter \
	-fdata-sections -ffunction-sections
# Additional release-specific flags
RCOMPILE_FLAGS = -DNDEBUG -O3
# Additional debug-specific flags
DCOMPILE_FLAGS = -DDEBUG -g
# Add additional include paths
INCLUDES = -I$(SRC_PATH) -I./include -I./deps/out/include \
	-I./deps/fmt/include \
	-I./deps/GSL/include \
	-I./deps/json/single_include \
	-I./deps/xxHash \
	# -I./deps/ordered-map/include
# Protobuf compiler
PROTOC = ./deps/out/bin/protoc
# General linker settings
ABSL_LIBRARIES = $(shell find deps/out/lib/libabsl_*.a -printf '%f\n' \
		| sed -e 's/libabsl_\([a-z0-9_]\+\)\.a/-labsl_\1/g')
LINK_FLAGS = -Ldeps/out/lib \
	-Wl,-Bstatic -luv_a -lhttp_parser -lnghttp2 \
	-luring -lprotobuf-lite -lrocksdb -ltkrzw -lzookeeper_st \
	-Wl,--start-group $(ABSL_LIBRARIES) -Wl,--end-group \
	-lzstd -ljemalloc \
	-Wl,-Bdynamic -lpthread -ldl \
	-Wl,--gc-sections
# Additional release-specific linker settings
RLINK_FLAGS =
# Additional debug-specific linker settings
DLINK_FLAGS =
#### END PROJECT SETTINGS ####

# These options can be overridden in config.mk
DISABLE_STAT = 1
DEBUG_BUILD = 0
BUILD_BENCH = 0
FORCE_DCHECK = 0

ifneq ("$(wildcard config.mk)","")
include config.mk
endif

ifneq (,$(findstring clang,$(CXX)))
COMPILE_FLAGS += -Wthread-safety -Wno-unused-private-field
endif

ifeq ($(DISABLE_STAT),1)
COMPILE_FLAGS += -D__FAAS_DISABLE_STAT
endif

ifeq ($(FORCE_DCHECK),1)
COMPILE_FLAGS += -DDCHECK_ALWAYS_ON
endif

# Function used to check variables. Use on the command line:
# make print-VARNAME
# Useful for debugging and adding features
print-%: ; @echo $*=$($*)

# Shell used in this makefile
# bash is used for 'echo -en'
SHELL = /bin/bash
# Clear built-in rules
.SUFFIXES:

# Verbose option, to output compile and link commands
export V := 0
export CMD_PREFIX := @
ifeq ($(V),1)
CMD_PREFIX :=
endif

COMPILE_FLAGS += $(INCLUDES)

ifeq ($(DEBUG_BUILD),1)
BUILD_NAME     = debug
COMPILE_FLAGS += $(DCOMPILE_FLAGS)
LINK_FLAGS    += $(DLINK_FLAGS)
else
BUILD_NAME     = release
COMPILE_FLAGS += $(RCOMPILE_FLAGS)
LINK_FLAGS    += $(RLINK_FLAGS)
endif

BUILD_PATH := build/$(BUILD_NAME)
BIN_PATH := bin/$(BUILD_NAME)

# Find all source files in the source directory, sorted by most
# recently modified
SOURCES = $(shell find $(SRC_PATH) -name '*.$(SRC_EXT)')
BIN_SOURCES = $(shell find $(SRC_PATH)/bin -name '*.$(SRC_EXT)')
BENCH_BIN_SOURCES = $(shell find $(SRC_PATH)/bin -name 'bench_*.$(SRC_EXT)')

# Protobuf related
PROTO_SOURCES = $(shell find $(SRC_PATH)/proto -name '*.proto')
PROTO_HEADERS = $(PROTO_SOURCES:$(SRC_PATH)/proto/%.proto=$(SRC_PATH)/proto/%.pb.h)
PROTO_OBJECTS = $(PROTO_SOURCES:$(SRC_PATH)/proto/%.proto=$(BUILD_PATH)/proto/%.pb.o)

# Set the object file names, with the source directory stripped
# from the path, and the build path prepended in its place
OBJECTS = $(SOURCES:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
OBJECTS += $(PROTO_OBJECTS)
# Set the dependency files that will be used to add header dependencies
DEPS = $(OBJECTS:.o=.d)

BIN_OBJECTS = $(BIN_SOURCES:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
NON_BIN_OBJECTS = $(filter-out $(BIN_OBJECTS),$(OBJECTS))

BENCH_BIN_OUTPUTS = $(BENCH_BIN_SOURCES:$(SRC_PATH)/bin/%.$(SRC_EXT)=$(BIN_PATH)/%)
BIN_OUTPUTS = $(BIN_OBJECTS:$(BUILD_PATH)/bin/%.o=$(BIN_PATH)/%)

ifeq ($(BUILD_BENCH),1)
TARGET_BINS = $(BIN_OUTPUTS)
else
TARGET_BINS = $(filter-out $(BENCH_BIN_OUTPUTS),$(BIN_OUTPUTS))
endif

TIME_FILE = $(dir $@).$(notdir $@)_time
START_TIME = date '+%s' > $(TIME_FILE)
END_TIME = read st < $(TIME_FILE) ; \
	$(RM) $(TIME_FILE) ; \
	st=$$((`date '+%s'` - $$st - 86400)) ; \
	echo `date -u -d @$$st '+%H:%M:%S'`

.PHONY: all
all: dirs
	@echo "Beginning $(BUILD_NAME) build"
	@$(START_TIME)
	@$(MAKE) binary --no-print-directory
	@echo -n "Total build time: "
	@$(END_TIME)

# Create the directories used in the build
.PHONY: dirs
dirs:
	@mkdir -p $(dir $(OBJECTS))
	@mkdir -p $(BIN_PATH)

# Removes all build files
.PHONY: clean
clean:
	@echo "Deleting directories"
	@$(RM) -r build bin
	@echo "Deleting protobuf generated code"
	@$(RM) -f src/proto/*.pb.h src/proto/*.pb.cc src/proto/*.pb.cpp

binary: $(TARGET_BINS)

# Proto files must be compiled before all objects
$(OBJECTS): $(PROTO_HEADERS)

# Link the executable
$(BIN_PATH)/%: $(BUILD_PATH)/bin/%.o $(NON_BIN_OBJECTS)
	@echo "Linking: $@"
	$(CMD_PREFIX)$(CXX) $^ $(LDFLAGS) $(LINK_FLAGS) -o $@

.SECONDARY: $(OBJECTS)

# Add dependency files, if they exist
-include $(DEPS)

# Source file rules
# After the first compilation they will be joined with the rules from the
# dependency files to provide header dependencies
$(BUILD_PATH)/%.o: $(SRC_PATH)/%.$(SRC_EXT)
	@echo "Compiling: $< -> $@"
	$(CMD_PREFIX)$(CXX) $(CXXFLAGS) $(COMPILE_FLAGS) -MP -MMD -c $< -o $@

# Protobuf-related rules
$(SRC_PATH)/proto/%.pb.cpp $(SRC_PATH)/proto/%.pb.h: $(SRC_PATH)/proto/%.proto
	@echo "Compiling proto file $<"
	$(CMD_PREFIX)$(PROTOC) --proto_path=$(SRC_PATH)/proto --cpp_out=$(SRC_PATH)/proto $<
	@mv $(patsubst %.proto,%.pb.cc,$<) $(patsubst %.proto,%.pb.cpp,$<)
$(SRC_PATH)/proto/%.pb.h: $(SRC_PATH)/proto/%.pb.cpp
