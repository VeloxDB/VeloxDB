# syntax=docker/dockerfile:1

FROM crazymax/osxcross:latest-ubuntu AS osxcross

FROM ubuntu:24.04

# Install basic development tools for the interactive environment
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang lld libc6-dev bash make libc6-dev-arm64-cross \
        binutils-aarch64-linux-gnu libgcc-*-dev-arm64-cross \
        gcc-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

COPY --from=osxcross /osxcross /osxcross

# Set environment variables so the shell can find the cross-compiler tools
ENV PATH="/osxcross/bin:$PATH"
ENV LD_LIBRARY_PATH="/osxcross/lib:$LD_LIBRARY_PATH"

# Set the entry point to bash
ENTRYPOINT ["/bin/bash"]