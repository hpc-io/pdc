# clang format command
find . -regex '.*\.\(c\|h\|cpp\|hpp\)' -exec clang-format -i --style=file {} +
