#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <mntent.h>
#include <stdlib.h>
#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

/*
 * this tool means to make quota querying (and eventually management easier)
 * first it allows for querying what quota applies to a certain path
 * cephfs-quota <path> - print current quota limitations on <path>
 */

bool is_ceph_mount(const char *filename)
{
  struct stat s;
  FILE *      fp;
  struct mntent *mnt;
  dev_t dev;

  if (stat(filename, &s) != 0) {
    return false;
  }
  dev = s.st_dev;

  if ((fp = setmntent("/proc/mounts", "r")) == NULL) {
    return false;
  }
  while (1) {
    mnt = getmntent(fp);
    if (mnt == NULL) {
      return false;
    }

    if (stat(mnt->mnt_dir, &s) != 0) {
        return false;
    }

    if (strncmp(mnt->mnt_fsname, "ceph", 4) && s.st_dev == dev) {
      endmntent(fp);
      return true;
    }
  }

}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
      CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  if (is_ceph_mount(".")) {
    std::err << ". is ceph mount" << std::endl;
  } else {
    std::err << ". is not ceph mount" << std::endl;
  }
}
