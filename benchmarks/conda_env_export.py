import re
import subprocess

import yaml


def export_env(history_only=False, include_builds=False):
    """
    COPIED from https://gist.github.com/gwerbin/
    dab3cf5f8db07611c6e0aeec177916d8#file-conda_env_export-py
    """
    """ Capture `conda env export` output """
    cmd = ['conda', 'env', 'export']
    if history_only:
        cmd.append('--from-history')
        if include_builds:
            raise ValueError('Cannot include build versions with "from history" mode')
    if not include_builds:
        cmd.append('--no-builds')
    cp = subprocess.run(cmd, stdout=subprocess.PIPE)
    try:
        cp.check_returncode()
    except Exception as e:
        raise e
    else:
        return yaml.safe_load(cp.stdout)


def remove_version(env_list):
    new_list = []
    for d in env_list:
        d_prefix = re.sub(r'[>=].*', '', d)
        new_list.append(d_prefix)
    return new_list


def _is_history_dep(d, history_deps):
    if not isinstance(d, str):
        return False
    d_prefix = re.sub(r'=.*', '', d)
    return d_prefix in history_deps


def _get_pip_deps(full_deps):
    for dep in full_deps:
        if isinstance(dep, dict) and 'pip' in dep:
            return dep


def _combine_env_data(env_data_full, env_data_hist):
    deps_full = env_data_full['dependencies']
    deps_hist = env_data_hist['dependencies']
    deps_hist = remove_version(deps_hist)
    deps = [dep for dep in deps_full if _is_history_dep(dep, deps_hist)]
    # pip_deps = _get_pip_deps(deps_full)

    env_data = {}
    env_data['channels'] = env_data_full['channels']
    env_data['dependencies'] = deps
    # env_data['dependencies'].append(pip_deps)

    return env_data


def env_dump(env_yml, env_dump_file):
    env_data_full = export_env()
    # env_data_hist = export_env(history_only=True)
    with open(env_yml) as f:
        env_data_hist = yaml.safe_load(f)
    env_data = _combine_env_data(env_data_full, env_data_hist)
    with open(env_dump_file, 'w') as fout:
        yaml.dump(env_data, fout)
