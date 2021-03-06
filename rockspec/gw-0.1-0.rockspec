package = "gw"
version = "0.1-0"
supported_platforms = {"linux"}

source = {
    url = "git://github.com/jixindatech/yulv-gw",
    tag = "v0.10",
    branch="master",
}

description = {
    summary = "yulv gateway.",
    homepage = "https://github.com/jixindatech/yulv-gw",
    maintainer = "Fangang Cheng <chengfangang@qq.com>"
}

dependencies = {
    "lua-resty-template = 1.9",
    "lua-tinyyaml = 1.0",
    "luafilesystem = 1.7.0-2",
    "jsonschema = 0.9.5",
    "luautf8 = 0.1.3-1",
    "lua-resty-iputils = 0.3.0-1",
    "lua-resty-kafka = 0.09",
    "lua-resty-jit-uuid = 0.0.7-2",
}

build = {
    type = "make",
    build_variables = {
        CFLAGS="$(CFLAGS)",
        LIBFLAG="$(LIBFLAG)",
        LUA_LIBDIR="$(LUA_LIBDIR)",
        LUA_BINDIR="$(LUA_BINDIR)",
        LUA_INCDIR="$(LUA_INCDIR)",
        LUA="$(LUA)",
    },
    install_variables = {
        INST_PREFIX="$(PREFIX)",
        INST_BINDIR="$(BINDIR)",
        INST_LIBDIR="$(LIBDIR)",
        INST_LUADIR="$(LUADIR)",
        INST_CONFDIR="$(CONFDIR)",
    },
}
