#include <iostream>
#include <ext/stdio_filebuf.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>

# define ZERO_BYTE_TRANSFER_ERRNO 0

#ifdef EINTR
# define IS_EINTR(x) ((x) == EINTR)
#else
# define IS_EINTR(x) 0
#endif

enum { SYS_BUFSIZE_MAX = INT_MAX >> 20 << 20 };

size_t safe_write (int fd, void const *buf, size_t count)
{
  for (;;)
    {
      ssize_t result = write (fd, buf, count);

      if (0 <= result)
        return result;
      else if (IS_EINTR (errno))
        continue;
      else if (errno == EINVAL && SYS_BUFSIZE_MAX < count)
        count = SYS_BUFSIZE_MAX;
      else
        return result;
    }
}

size_t full_rw (int fd, const void *buf, size_t count)
{
  size_t total = 0;
  const char *ptr = (const char *) buf;

  while (count > 0)
    {
      size_t n_rw = safe_write (fd, ptr, count);
      if (n_rw == (size_t) -1)
        break;
      if (n_rw == 0)
        {
          errno = ZERO_BYTE_TRANSFER_ERRNO;
          break;
        }
      total += n_rw;
      ptr += n_rw;
      count -= n_rw;
    }

  return total;
}


int main(int argc, char const *argv[])
{
    if (argc < 2) {
        std::cerr
            << "You must pass only one argument to the programm contained path to the file with files to read."
            << std::endl
            << "Example of usage: ccat file.txt"
            << std::endl;

        return EXIT_FAILURE;
    }

    //open file and get descriptor number
    FILE * file = fopen(argv[1], "r");

    if (!file) {
        std::cerr << "failed to open " << argv[1] << std::endl;
    }

    int fileDescriptor = fileno(file);

    //Notify the system that we will sequentially read the file on that file descriptor
    posix_fadvise(fileDescriptor, 0, 0, POSIX_FADV_SEQUENTIAL);

    //Make c++ istream from file descriptor
    __gnu_cxx::stdio_filebuf<char> filebuf(fileDescriptor, std::ios::in);

    std::istream is(&filebuf);

    std::string fileName;

    char *inbuf;

    size_t pageSize = getpagesize();
    size_t insize = 128*1024;
    size_t n_read;

    /*
        Main loop with fast buffered read of files though syscals and output to stdout.
    */
    while(std::getline(is, fileName)) {

        int fileWithDataDescriptor = open(fileName.c_str(), O_RDONLY);

        if (!fileWithDataDescriptor) {
            std::cerr << "failed to open " << fileName << std::endl;
            return EXIT_FAILURE;
        }

        //Notify the system that we will sequentially read the file on that file descriptor
        posix_fadvise(fileWithDataDescriptor, 0, 0, POSIX_FADV_SEQUENTIAL);

        inbuf = (char*) aligned_alloc(pageSize, insize + pageSize - 1);

        while(n_read = read(fileWithDataDescriptor, inbuf, insize))
        {
            full_rw(1, inbuf, n_read);
        }

        free(inbuf);
        close(fileWithDataDescriptor);
    }

    filebuf.close();
    close(fileDescriptor);

    return EXIT_SUCCESS;
}
