CC = g++ # define the C/C++ compiler to use
CFLAGS = -O0 -g -Wall -Wextra -pedantic -fPIC
# define any directories containing header files other than /usr/include
INCLUDES = -I/. -Iopenai
# define the C++ source files
SRCS = chat.cpp
# define the C/C++ object files 
# This uses Suffix Replacement within a macro:
#   $(name:string1=string2)
#         For each word in 'name' replace 'string1' with 'string2'
OBJS = $(SRCS:.c=.o)
# define the shared library name
TARGET = libchat.so

.PHONY: clean
    
all:   $(SUBDIRS) $(TARGET)
	@echo  Successfully compiled a .so library.

$(SUBDIRS):
	$(MAKE) -C $@

# compile object file to .so shared library
$(TARGET): $(OBJS) 
	$(CC) $(CFLAGS) $(INCLUDES) -shared -o $(TARGET) $(OBJS)

# compile source files to object files

# this is a suffix replacement rule for building .o's from .c's
# it uses automatic variables $<: the name of the prerequisite of
# the rule(a .c file) and $@: the name of the target of the rule (a .o file) 
# (see the gnu make manual section about automatic variables)
.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -cpp $<  -o $@
clean:
	$(RM) *.o *.so
