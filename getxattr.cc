#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>

int main(int argc, char** argv)
{
  char* buf = new char[4096];
  ssize_t cnt = getxattr(argv[1], argv[2], buf, 4096);
  if (cnt > 0)
  {
     write(1, buf, cnt);
     write(1, "\n", 1);
  }
  return 0;
}