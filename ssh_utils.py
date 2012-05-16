import os.path
from stat import S_ISDIR
from libcloud.compute.ssh import SSHClient
from paramiko.sftp import SFTPError

class CloudSSHClient(SSHClient):
    
    
    @staticmethod
    def normalize_dirpath(dirpath):
        while dirpath.endswith("/"):
            dirpath = dirpath[:-1]
        return dirpath
    
    
    def mkdir(self, sftp, remotepath, mode=0777, intermediate=False):
        remotepath = self.normalize_dirpath(remotepath)
        if intermediate:
            try:
                sftp.mkdir(remotepath, mode=mode)
            except IOError, e:
                self.mkdir(sftp, remotepath.rsplit("/", 1)[0], mode=mode,
                           intermediate=True)
                return sftp.mkdir(remotepath, mode=mode)
        else:
            sftp.mkdir(remotepath, mode=mode)


    def put_dir_recursively(self,  localpath, remotepath, preserve_perm=True):
        "upload local directory to remote recursively"
        
        assert remotepath.startswith("/"), "%s must be absolute path" % remotepath
        
        # normalize
        localpath = self.normalize_dirpath(localpath)
        remotepath = self.normalize_dirpath(remotepath)

        sftp = self.client.open_sftp()
        
        try:
            sftp.chdir(remotepath)
            localsuffix = localpath.rsplit("/", 1)[1]
            remotesuffix = remotepath.rsplit("/", 1)[1]
            if localsuffix != remotesuffix:
                remotepath = os.path.join(remotepath, localsuffix)
        except IOError, e:
            pass
                        
        for root, dirs, fls in os.walk(localpath):
            prefix = os.path.commonprefix([localpath, root])
            suffix = root.split(prefix, 1)[1]
            if suffix.startswith("/"):
                suffix = suffix[1:]

            remroot = os.path.join(remotepath, suffix)

            try:
                sftp.chdir(remroot)
            except IOError, e:
                if preserve_perm:
                    mode = os.stat(root).st_mode & 0777
                else:
                    mode = 0777
                self.mkdir(sftp, remroot, mode=mode, intermediate=True)
                sftp.chdir(remroot)
                
            for f in fls:
                remfile = os.path.join(remroot, f)
                localfile = os.path.join(root, f)
                sftp.put(localfile, remfile)
                if preserve_perm:
                    sftp.chmod(remfile, os.stat(localfile).st_mode & 0777)


    def get_dir_recursively(self,  remotepath, localpath, preserve_perm=True):

        def get_one_layer(sftp, remotepath, localpath, preserve_perm=preserve_perm,
                          dirmode=0755):
    
            file_list = sftp.listdir(path=remotepath)
                        
            if not os.path.exists(localpath):
                os.mkdir(localpath)
                if preserve_perm:
                    os.chmod(localpath, dirmode)
                    # TODO: perm ?

            for item in file_list:
                remfile = os.path.join(remotepath, item)
                localfile = os.path.join(localpath, item)
                st_mode = sftp.stat(remfile).st_mode
                if S_ISDIR(st_mode):
                    get_one_layer(sftp, remfile, localfile, preserve_perm=preserve_perm,
                                  dirmode=st_mode & 0777)
                else:
                    sftp.get(remfile, localfile)
                    if preserve_perm:
                        os.chmod(localfile, st_mode & 0777)

        sftp = self.client.open_sftp()
        get_one_layer(sftp, remotepath, localpath, preserve_perm=preserve_perm)


def test():
    from config import *

    ssh_client = CloudSSHClient(hostname='ec2-*.compute-1.amazonaws.com',
                                   port=SSH_PORT, username=SSH_USERNAME,
                                   key=SSH_KEYPATH)
    ssh_client.connect()
    #ssh_client.put_dir_recursively('../model', '/home/ubuntu/model-tmp')
    ssh_client.put_dir_recursively('../model', 'model-tmp2')
    #ssh_client.get_dir_recursively('model-tmp', '../model-tmp')
    ssh_client.close()


if __name__ == "__main__":
    test()
    