# MiniSQL


本框架参考 CMU-15445 BusTub 框架进行改写，在保留了缓冲池、索引、记录模块的一些核心设计理念的基础上，做了一些修改和扩展，使之兼容于原 MiniSQL 实验指导的要求。
以下列出了改动/扩展较大的几个地方：

- 对 Disk Manager 模块进行改动，扩展了位图页、磁盘文件元数据页用于支持持久化数据页分配回收状态；
- 在 Record Manager, Index Manager, Catalog Manager 等模块中，通过对内存对象的序列化和反序列化来持久化数据；
- 对 Record Manager 层的一些数据结构（ `Row`、`Field`、`Schema`、`Column` 等）和实现进行重构；
- 对 Catalog Manager 层进行重构，支持持久化并为 Executor 层提供接口调用;
- 扩展了 Parser 层，Parser 层支持输出语法树供 Executor 层调用；

此外还涉及到很多零碎的改动，包括源代码中部分模块代码的调整，测试代码的修改，性能上的优化等，在此不做赘述。


课程目前已经结束，代码发布至公共托管平台。

### 重要！！！

确保你的环境装有 git，你将会用到它来管理这个项目的版本，使用 `git --version` 查看 git 版本

#### 配置 git 全局代理

如果你所在的网络环境需要代理才能访问 GitHub，并且你正在使用一个本地代理工具（例如 Clash, V2RayN, Shadowsocks 等的客户端通常会监听类似 127.0.0.1:7890 的端口），那么请执行以下命令配置 Git 代理。否则，请跳过此步骤或根据你的实际代理情况进行配置。在任意命令行（如 `Windows Powershell` ）中执行

```
git config --global http.proxy 127.0.0.1:7890
git config --global https.proxy 127.0.0.1:7890
```

如果需要取消代理，请执行

```
git config --global --unset http.proxy
git config --global --unset https.proxy
```

#### 使用 CLion 开始开发

如果使用 CLion 开发，可以直接在 Welcome 界面选择 `Projects -> Clone Repository -> GitHub`，在登录你的 GitHub 账户后，应该能看到一个叫做 MiniSQL 的仓库。选中该仓库后，按需修改本地存放的位置，点击 `Clone` 按钮后等待全部内容加载完成。

CLion 界面的最顶端有横着排列的三个按钮，分别用于展开主菜单，显示当前的项目名称，以及显示当前所在的 Git Branch。如果一切正常，Git Branch 按钮会显示 master。点击 Git Branch 按钮展开菜单，找到 `Remote -> origin -> master`，点击这一行内容后，会在右侧出现一个二级菜单。在二级菜单中选择 `New Branch from origin/master`，在 Branch Name 处填入名称，并仅勾选 Checkout branch（一般是默认勾选）后，点击 Create。注意，名称必须以 `feature/` 开头，后面紧跟你所负责部分的名称，全部小写，空格用短横代替，例如 `feature/disk-and-buffer-pool-manager`。

随后你将会切换到新的 Branch，可以开始开发。在正式开始写代码前，还需要在 CLion 中配置 WSL，请参考“构建”部分。

#### 向本仓库提交代码

请不要在 master 分支提交修改（根据 CLion 界面最上方 Git Branch 按钮显示的内容就可以判断当前所在的分支）。

如果 Git Branch 按钮显示 `feature/` 开头的分支，则修改都会提交到这个分支，不必担心会提交到 master。

当你需要提交代码时，请找到界面最顶端的主菜单按钮（图标为四条横线），点击后，依次找到 `Git -> Commit...`，在下方 Changes 处会立即出现已修改但尚未提交的内容。此时，通过复选框选择要提交哪些，在 Commit Message 处写上此次提交更新的内容，点击 `Commit and Push...`，在弹出的对话框中再点击 Push，即可将修改推送到 GitHub 服务器。

#### 申请将提交的代码合并到 master 分支

一旦 Push 成功后，CLion 就会提示可以进行 `Pull request`，此时根据提示操作即可。当然也可以直接访问[本仓库的主页](https://github.com/Jerry-Gren/MiniSQL/)，此时应该也会有 `Pull requests` 的提示。点击按钮后，根据提示提交申请。

审批和合并的操作由仓库管理员进行，在正式合并前，必须通过这个分支所对应的在线测试（在线测试会运行与本地相同的测试）。测试是由 GitHub 服务器自动进行的，具体信息可以访问[本仓库的Action](https://github.com/Jerry-Gren/MiniSQL/actions)进行查看。

对于同一个分支，一旦开启了 `Pull request`，在管理员还没有来得及将代码合并之前，还可以继续向这个分支 Push 代码。此时所提交的代码将会和之前已经在 `Pull request` 里面的部分一起，重新进入自动测试。自动测试通过后，管理员才能够进行合并。如果管理员合并了代码，这一次 `Pull request` 就关闭了。

#### 后续处理

当一个 `feature/` 分支的 PR 被合并到 master 后，这个 `feature/` 分支的使命就完成了。GitHub 在合并 PR 后通常会提供一个删除该远程分支的按钮，你可以选择删除这个分支。对于后续新的功能开发或修改，建议从最新的 master 分支（不是本地的 master 分支）重新创建一个新的 `feature/` 分支（参照“使用 CLion 开始开发”部分），并为其提交新的 `Pull Request`。

如果 master 分支发生更新，建议尽快在本地进行同步。先点击 Git Branch 按钮，点击为 Local 下的 master 分支，在二级菜单中点击 Checkout 进行切换。随后点击界面最顶端的主菜单按钮（图标为四条横线），找到 `Git -> Update Project...`，选择 `Merge incoming changes into the current branch`（一般为默认选择）后确认，等待同步完成。

### 编译&开发环境

- Apple clang version: 11.0+ (MacOS)，使用 `gcc --version` 和 `g++ --version` 查看
- gcc & g++ : 8.0+ (Linux)，使用 `gcc --version` 和 `g++ --version` 查看
- cmake: 3.20+ (Both)，使用 `cmake --version` 查看
- gdb: 7.0+ (Optional)，使用 `gdb --version` 查看
- flex & bison (暂时不需要安装，但如果需要对 SQL 编译器的语法进行修改，需要安装）
- llvm-symbolizer (暂时不需要安装)
  - in mac os `brew install llvm`, then set path and env variables.
  - in centos `yum install devtoolset-8-libasan-devel libasan`
  - https://www.jetbrains.com/help/clion/google-sanitizers.html#AsanChapter
  - https://www.jianshu.com/p/e4cbcd764783

### 构建

#### Windows

目前该代码暂不支持在 Windows 平台上的编译。但在 Win10 及以上的系统中，可以通过安装 WSL（Windows 的 Linux 子系统）来进行
开发和构建。WSL 请选择 Ubuntu 子系统（推荐 Ubuntu20 及以上）。如果你使用 CLion 作为 IDE，可以在 CLion 中配置 WSL 从而进行调试，具体请参考
[CLion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/)

#### MacOS & Linux & WSL

基本构建命令

```bash
mkdir build
cd build
cmake ..
make -j
```

若不涉及到 `CMakeLists` 相关文件的变动且没有新增或删除 `.cpp` 代码（通俗来说，就是只是对现有代码做了修改）
则无需重新执行 `cmake..` 命令，直接执行 `make -j` 编译即可。

默认以 `debug` 模式进行编译，如果你需要使用 `release` 模式进行编译：

```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 测试

在构建后，默认会在 `build/test` 目录下生成 `minisql_test` 的可执行文件，通过 `./minisql_test` 即可运行所有测试。

如果需要运行单个测试，例如，想要运行 `lru_replacer_test.cpp`对应的测试文件，可以通过 `make lru_replacer_test`
命令进行构建。
