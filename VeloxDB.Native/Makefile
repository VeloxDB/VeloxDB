# Makefile for libvlxdb_native supporting multi-platform C builds
# Designed to be used from within container provided in the same directory
# Usage: make all-platforms
# Individual Usage Example: make OS=linux ARCH=x86_64

OS ?= linux
ARCH ?= x86_64

# --- Constants and Derivations ---
CC = clang
SOURCES = native_file.c
OBJECTS = $(SOURCES:.c=.o)

ifeq ($(ARCH), x86_64)
	ARCH_CANONICAL = x86_64
	ARCH_FLAG = -D_X64
	OUTPUT_ARCH_NAME = x64
else ifeq ($(ARCH), aarch64)
	# NOTE: For Clang/LLVM target triples, the canonical name is sometimes 'arm64' on macOS
	ARCH_CANONICAL = aarch64
	ARCH_FLAG = -D_ARM64
	OUTPUT_ARCH_NAME = arm64
else
	$(error Unsupported ARCH. Set ARCH={x86_64, aarch64})
endif

# 2. OPERATING SYSTEM LOGIC (Defines TARGET_NAME, LINK_PATH, and OS_FLAG)
ifeq ($(OS), linux)
	OS_TRIPLE_SUFFIX = -unknown-linux-gnu
	OS_FLAG = -D_LINUX
	LINK_PATH ?= /lib
	TARGET_NAME = libvlxdb_native.so
	CROSS_OS = linux-gnu

	TARGET_TRIPLE = $(ARCH_CANONICAL)$(OS_TRIPLE_SUFFIX)
	
	CROSS_ROOT = /usr/$(ARCH)-$(CROSS_OS)
	CFLAGS = -O2 -Wall -Wextra -std=c99 -fPIC -I$(INCLUDE_PATH) $(OS_FLAG) $(ARCH_FLAG) -target $(TARGET_TRIPLE)
	LDFLAGS = -shared -L$(LINK_PATH) -B$(CROSS_ROOT)/bin -target $(TARGET_TRIPLE) 

else ifeq ($(OS), macos)
	MACOSX_DEPLOYMENT_TARGET := 12.0
	ifeq ($(ARCH), aarch64)
		ARCH_CANONICAL = arm64
	endif
	OS_TRIPLE_SUFFIX = -apple-darwin
	OS_FLAG = -D_MACOS
	LINK_PATH ?=
	TARGET_NAME = libvlxdb_native.dylib
	INCLUDE_PATH=/osxcross/include
	LIB_PATH=/osxcross/lib

	ifeq ($(ARCH), x86_64)
		CC = o64-clang
	else ifeq ($(ARCH), aarch64)
		CC = oa64-clang
	else
		$(error Unsupported ARCH. Set ARCH={x86_64, aarch64})
	endif

	CFLAGS = -O2 -Wall -Wextra -std=c99 -fPIC $(OS_FLAG) $(ARCH_FLAG) -framework CoreFoundation -framework IOKit -mmacosx-version-min=$(MACOSX_DEPLOYMENT_TARGET)
	LDFLAGS = -shared 
else
	$(error Unsupported OS. Set OS={linux, macos})
endif

LINK_PATH= $(CROSS_ROOT)/lib
INCLUDE_PATH = $(CROSS_ROOT)/include

# --- Output Directory Configuration ---
# Example: bin/linux-x64
OUTPUT_DIR = bin/$(OS)-$(OUTPUT_ARCH_NAME)
FINAL_TARGET_PATH = $(OUTPUT_DIR)/$(TARGET_NAME)

all: $(FINAL_TARGET_PATH)
	@echo "--- Build successful for OS: $(OS) / ARCH: $(ARCH) -> Output: $(FINAL_TARGET_PATH)"

$(FINAL_TARGET_PATH): $(SOURCES)
	@mkdir -p $(OUTPUT_DIR)
	@echo "Linking $(TARGET_NAME) as $(TARGET_TRIPLE) to $(OUTPUT_DIR)..."
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

all-platforms:
	$(MAKE) OS=linux ARCH=x86_64 all
	$(MAKE) OS=linux ARCH=aarch64 all
	$(MAKE) OS=macos ARCH=x86_64 all
	$(MAKE) OS=macos ARCH=aarch64 all
# Rule to clean up generated files
clean:
	# Remove object files from the root directory
	rm -f $(OBJECTS)
	# Remove the specific output file for the current OS/ARCH combination
	rm -f $(FINAL_TARGET_PATH)
	# Attempt to remove the entire 'bin' directory if it only contains the compiled files (optional, but convenient)
	@echo "Cleaning up $(OUTPUT_DIR)..."
	@find bin -type d -empty -delete 2>/dev/null || true

.PHONY: all clean
