#!/usr/bin/python3

import os
import subprocess
import shutil


def find_submodules(base_dir):
    submodule_dirs = []
    for root, dirs, _ in os.walk(base_dir):
        for d in dirs:
            dir_path = os.path.join(root, d)
            git_dir = os.path.join(dir_path, "src")
            if os.path.exists(git_dir) and os.path.isdir(git_dir):
                submodule_dirs.append(dir_path)
    return submodule_dirs


def find_all_submodules(base_dir):
    all_submodules = []
    submodules = find_submodules(base_dir)
    all_submodules.extend(submodules)

    for submodule in submodules:
        sub_submodules = find_all_submodules(submodule)
        all_submodules.extend(sub_submodules)

    return all_submodules


parent_project_dir = os.path.dirname(os.path.abspath(__file__))

# 设置目标目录
target_dir = os.path.join(parent_project_dir, "target")
if os.path.exists(target_dir):
    if os.path.isdir(target_dir):
        shutil.rmtree(target_dir)
    elif os.path.isfile(target_dir):
        os.remove(target_dir)

os.makedirs(target_dir, exist_ok=True)

# 获取所有子模块的目录
submodule_dirs = find_all_submodules(base_dir=parent_project_dir)

# compile all modules
subprocess.run(["mvn", "clean", "install"], cwd=parent_project_dir, check=True)

# 复制JAR文件到目标目录
target_lib_dir = target_dir + "/lib"
os.makedirs(target_lib_dir, exist_ok=True)

for submodule_dir in submodule_dirs:
    module_target_dir = os.path.join(submodule_dir, "target")
    if os.path.exists(module_target_dir):
        for jar_file in os.listdir(module_target_dir):
            if jar_file.endswith(".jar"):
                shutil.copy(os.path.join(module_target_dir, jar_file), target_lib_dir)

# 复制依赖项到lib目录
for submodule_dir in submodule_dirs:
    lib_dir = os.path.join(submodule_dir, "lib")
    if os.path.isdir(lib_dir):
        shutil.rmtree(lib_dir)
    elif os.path.isfile(lib_dir):
        os.remove(lib_dir)
    print(f"Copying dependencies for {submodule_dir}...")
    subprocess.run(["mvn", "dependency:copy-dependencies", "-DoutputDirectory=lib"], cwd=submodule_dir, check=True)
    if os.path.exists(lib_dir):
        for dep_file in os.listdir(lib_dir):
            if dep_file.endswith(".jar"):
                shutil.copy(os.path.join(lib_dir, dep_file), target_lib_dir)
        shutil.rmtree(lib_dir)

print("JAR files and dependencies have been copied to the 'target/lib' directory.")

target_conf_dir = target_dir + "/conf"
os.makedirs(target_conf_dir, exist_ok=True)

conf_dir = parent_project_dir + "/bootstrap/src/main/resources/conf"
for conf_file in os.listdir(conf_dir):
    if conf_file.endswith(".yaml"):
        shutil.copy(os.path.join(conf_dir, conf_file), target_conf_dir)


# 将启动文件
target_bin_dir = target_dir + "/bin"
os.makedirs(target_bin_dir, exist_ok=True)

for conf_file in os.listdir(parent_project_dir):
    if conf_file.endswith(".sh"):
        shutil.copy(os.path.join(parent_project_dir, conf_file), target_bin_dir)
        subprocess.run(["chmod", "u+x", os.path.join(target_bin_dir, conf_file)])
