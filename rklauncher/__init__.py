from rk import rk
from paramiko import SSHClient, SSHException
from scp import SCPClient
from collections import defaultdict
from subprocess import run
import re
import json
import os
import logging


logger = logging.getLogger(__name__)


class RemoteKernel(object):
    def __init__(self, uri, rk_id, venv=None, python_cmd='python', display_name=None):
        """
        :param uri: remote machine hostname
        :param rk_id: kernel id. must be unique on remote machine venv and local rk file
        :param venv: name of venv to create or use. Defaults to the rk_id value
        :param python_cmd: points to any currently existing python installation. Valid values include 'python',
            'python2.7', 'python3', 'python3.6' etc.
        :param display_name: name to display in kernel list
        """
        self.rk_id = rk_id
        self.uri = uri
        self.python_cmd = python_cmd
        self.venv_name = venv
        self.display_name = display_name or '{uri} :: {rk_id}'.format_map(self.__dict__)

        if not uri or not rk_id or not python_cmd:
            raise ValueError('Cannot initialize RemoteKernel with params {}'.format(self.__dict__))

    def __str__(self):
        return "RemoteKernel {rk_id} :: uri='{uri}' {venv_name} python_cmd={python_cmd}".format_map(
            defaultdict(str, **{x:y for x, y in self.__dict__.items() if y}))

    def __eq__(self, other):
        if not isinstance(other, RemoteKernel):
            return False

        return self.uri == other.uri \
               and self.venv_name == other.venv_name \
               and self.python_cmd == other.python_cmd


class RKManager(object):
    """
    Supports 'create', 'remove', and 'list_installed' operations
    """

    PYTHON_VERSION_RE = re.compile('python\s+(\w)\..*', flags=re.IGNORECASE)

    def __init__(self):
        rk.create_dictionaries()

    @staticmethod
    def __run_remote(func, sshclient, remotekernel: RemoteKernel, ssh_key=None, username='ubuntu'):
        sshclient.load_system_host_keys()
        sshclient.connect(remotekernel.uri, username=username, key_filename=ssh_key)

        func()

    @staticmethod
    def __execute_ssh(sshclient, cmd):
        logger.debug("Executing '{}'".format(cmd))
        _, stdout, stderr = sshclient.exec_command(cmd)

        stderr = stderr.read().decode('utf-8')
        logger.debug('STDERR - {}'.format(stderr))
        stdout = stdout.read().decode('utf-8')
        logger.debug("STDOUT - {}".format(stdout))

        return stdout, stderr

    def create(self, remotekernel: RemoteKernel, ssh_key=None, requirements_file=None, **kwargs):
        """

        :param remotekernel:
        :param ssh_key: use the specified local key file for ssl auth. defaults to ~/.ssh/id_rsa
        :param requirements_file: install all packages defined by a requirements.txt file on remote kernel
        :param **overwrite: if set to true will overwrite the current kernel definition for the given kernel name
        :param **remote_venv_root_dir: default '~/'.
        :param **remote_username: default 'ubuntu'.

        :return boolean: indicates success
        """
        sshclient = SSHClient()

        remote_venv_dir = None
        if remotekernel.venv_name:
            remote_venv_root_dir = kwargs.get('remote_venv_root_dir') or '~/'
            remote_venv_dir = os.path.join(remote_venv_root_dir, remotekernel.venv_name)

        remote_bin_dir = os.path.join(remote_venv_dir or '')

        remote_python_cmd = os.path.join(remote_bin_dir, 'python')
        remote_pip_cmd = os.path.join(remote_bin_dir, 'pip')
        remote_jupyter_cmd = os.path.join(remote_bin_dir, 'jupyter')
        remote_username = kwargs.get('remote_username') or 'ubuntu'

        def __create():
            rks = self.get_installed()

            # Check if kernel already exists on local
            duplicates = [x for x in rks if x == remotekernel]
            overwrite = False
            if len(duplicates) > 0:
                for dupe in duplicates:
                    if dupe.rk_id == remotekernel.rk_id:
                        if kwargs.get('overwrite'):
                            overwrite = True
                            logger.info('Overwriting RemoteKernel {}'.format(remotekernel.rk_id))
                        else:
                            raise Exception(
                                "RemoteKernel {} already exists and 'overwrite' not set".format(dupe.rk_id))
                    else:
                        logger.warning('Duplicate RemoteKernel found. '
                                       'Existing kernel {} has the same parameters as new kernel {}'.format(
                                            dupe.rk_id,
                                            remotekernel.rk_id))

            # Find/create venv on remote
            if remotekernel.venv_name:
                cmd_does_venv_exist = '[ -d {} ] && echo "True"'.format(remote_venv_dir)
                stdout, _ = self.__execute_ssh(sshclient, cmd_does_venv_exist)
                if not stdout:
                    cmd_create_venv = 'virtualenv -p={py_cmd} {venv_dir}'.format(
                        py_cmd=remotekernel.python_cmd,
                        venv_dir=remote_venv_dir
                    )
                    # TODO validate call success
                    self.__execute_ssh(sshclient, cmd_create_venv)

            # TODO check permissions instead of python install path
            sudo = ''
            if not remotekernel.venv_name:
                cmd_python_location = 'which {python}'.format(python=remote_python_cmd)
                py_location_stdout, _ = self.__execute_ssh(sshclient, cmd_python_location)
                if py_location_stdout.startswith('/usr'):
                    sudo = 'sudo'

            cmd_install_ipython = '{sudo} {pip} install jupyter'.format(sudo=sudo, pip=remote_pip_cmd)
            # TODO validate call success
            self.__execute_ssh(sshclient, cmd_install_ipython)

            # Create ipython kernel on remote
            cmd_does_remote_kernel_exist = '{jupyter} kernelspec list'.format(jupyter=remote_jupyter_cmd)
            stdout = self.__execute_ssh(sshclient, cmd_does_remote_kernel_exist)
            if remotekernel.rk_id in [x.split('\s')[0] for x in stdout[1:]]:
                if not kwargs.get('overwrite'):
                    raise Exception("Kernel {} exists on remote and 'overwrite' not set".format(remotekernel.rk_id))

            cmd_create_ipy_kernel = '{sudo} {python} -m ipykernel install --name={kernel_id}'.format(
                sudo=sudo,
                python=remote_python_cmd,
                kernel_id=remotekernel.rk_id
            )
            # TODO validate call success
            self.__execute_ssh(sshclient, cmd_create_ipy_kernel)

            # Install requirements file
            if requirements_file:
                remote_requirements_file = os.path.join(remote_venv_dir, 'requirements.txt')

                scp = SCPClient(sshclient.get_transport())
                scp.put(requirements_file, remote_requirements_file)
                scp.close()

                cmd_install_reqs = '{sudo} {pip} install -r {file}'.format(
                    sudo=sudo,
                    pip=remote_pip_cmd,
                    file=remote_requirements_file
                )
                # TODO validate call success
                self.__execute_ssh(sshclient, cmd_install_reqs)

            # Add remote ipython kernel spec to local rk config
            kernel_dict = self.__get_local_kernelspec_dict()
            kernel_dict[remotekernel.rk_id] = {
                'display_name': remotekernel.display_name,
                'interpreter': remotekernel.python_cmd,
                'language': 'python',
                'remote_host': '{user}@{uri}'.format(user=remote_username, uri=remotekernel.uri),
                'venv': remotekernel.venv_name
            }
            with open(self.__get_local_kernelspec_path(), 'w') as ks:
                json.dump(kernel_dict, ks)

            # init remote ipython kernel
            # TODO refactor rk to abstract cli from lib then run from this process
            if overwrite:
                run(['sudo', 'rk', 'uninstall', remotekernel.rk_id])
            run(['sudo', 'rk', 'install', remotekernel.rk_id])

        self.__run_remote(__create, sshclient, remotekernel, ssh_key=ssh_key, username=remote_username)

    def remove(self, remotekernel: RemoteKernel, ssh_key=None, **kwargs):
        """

        :param remotekernel:
        :param ssh_key: use the specified local key file for ssl auth. defaults to ~/.ssh/id_rsa
        :param kwargs:
        :return:
        """

        sshclient = SSHClient()

        def __remove():
            # TODO: remove ipython kernel on remote

            kernel_dict = self.__get_local_kernelspec_dict()

            if remotekernel.rk_id not in kernel_dict:
                raise Exception('RemoteKernel {} not in local config'.format(remotekernel))

            # TODO refactor rk to abstract cli from lib then run from this process
            run(['sudo', 'rk', 'uninstall', remotekernel.rk_id])

            new_kd = kernel_dict.pop(remotekernel.rk_id)
            with open(self.__get_local_kernelspec_path(), 'w') as new_ks:
                json.dump(new_kd, new_ks)

        self.__run_remote(__remove, sshclient, remotekernel, ssh_key=ssh_key)

    @staticmethod
    def __get_local_kernelspec_path():
        config_kernels_rel_path = rk.config["config_kernels_rel_path"]
        return os.path.join(rk.module_location, config_kernels_rel_path)

    @staticmethod
    def __get_local_kernelspec_dict():
        with open(RKManager.__get_local_kernelspec_path(), 'r') as f:
            kernels_dict = json.load(f)

        return kernels_dict

    def get_installed(self, uri=None):
        """

        Lists all installed remote kernels

        :param uri: filter by uri
        :return list[RemoteKernel]: returns the list of installed remote kernels
        """
        # Load kernels.json file
        kernels_dict = self.__get_local_kernelspec_dict()

        remote_kernels = [RemoteKernel(y['remote_host'], x, venv=y.get('venv'), python_cmd=y.get('interpreter'))
                          for x, y in kernels_dict.items()if y['remote_host'] != uri]
        return remote_kernels

