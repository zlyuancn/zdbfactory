/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/4
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "fmt"
    "strings"
    "sync"

    "github.com/pelletier/go-toml"
    "github.com/spf13/viper"
    "github.com/zlyuancn/zerrors"
    "github.com/zlyuancn/zsignal"
)

type DBType string

// 支持的db类型
const (
    Mongo         DBType = "mongo"
    Redis                = "redis"
    ESv6                 = "esv6"
    ESv7                 = "esv7"
    Mysql                = "mysql"
    SSDB                 = "ssdb"
    ETCD                 = "etcd"
    KafkaProducer        = "kafka_producer"
)

// 解析配置树中以DBPrefix开头的分片
const DBPrefix = "zdb_"

// 这个字段表示db类型, 它必须在toml分片中存在
const DBTypeField = "dbtype"

// db实例
type DBInstance struct {
    dbtype   DBType
    instance interface{}
}

// 获取db类型
func (m *DBInstance) Type() DBType {
    return m.dbtype
}

// 获取db实例
func (m *DBInstance) Instance() interface{} {
    return m.instance
}

type dbConfig struct {
    dbtype DBType
    config interface{}
}

type IDBFactory interface {
    // 构建一个空的配置结构, 返回值必须是一个指针
    MakeEmptyConfig() interface{}
    Connect(config interface{}) (c interface{}, err error)
    Close(dbinstance interface{}) error
}

var factoryStorage = map[DBType]IDBFactory{
    Mongo:         new(mongoFactory),
    Redis:         new(redisFactory),
    ESv6:          new(esv6Factory),
    ESv7:          new(esv7Factory),
    Mysql:         new(mysqlFactory),
    SSDB:          new(ssdbFactory),
    ETCD:          new(etcdFactory),
    KafkaProducer: new(kafkaProducerFactory),
}

type DBFactory struct {
    storage   map[string]*DBInstance
    confs     map[string]*dbConfig
    autoClose bool
    mx        sync.RWMutex
}

// 创建一个db工厂
func New(opts ...Options) *DBFactory {
    factory := &DBFactory{
        storage: make(map[string]*DBInstance),
        confs:   make(map[string]*dbConfig),
    }

    for _, o := range opts {
        o(factory)
    }

    if factory.autoClose {
        zsignal.RegisterOnShutdown(factory.CloseAllDb)
    }

    return factory
}

// 添加viper文件
func (m *DBFactory) AddViperFile(file, filetype string) error {
    v := viper.New()
    v.SetConfigFile(file)
    if filetype != "" {
        v.SetConfigType(filetype)
    }
    if err := v.ReadInConfig(); err != nil {
        return err
    }
    return m.AddViperTree(v)
}

// 添加viper树
func (m *DBFactory) AddViperTree(tree *viper.Viper) error {
    for key, shard := range tree.AllSettings() {
        if !strings.HasPrefix(key, DBPrefix) {
            continue
        }

        switch mm := shard.(type) {
        case map[string]interface{}:
            dbname := key[len(DBPrefix):]

            switch dbtype := mm[DBTypeField].(type) {
            case string:
                if dbtype == "" {
                    return zerrors.NewSimplef("<%s>错误, %s为空", dbname, DBTypeField)
                }

                dbtype = strings.ToLower(dbtype)
                config := m.mustGetFactory(DBType(dbtype)).MakeEmptyConfig()
                if err := tree.UnmarshalKey(key, config); err != nil {
                    return zerrors.WrapSimple(err, "配置结构解析失败")
                }

                m.AddDBConfig(dbname, DBType(dbtype), config)
            default:
                return zerrors.NewSimplef("<%s>错误, %s必须存在且为string类型", dbname, DBTypeField)
            }
        }
    }
    return nil
}

// 添加toml文件, 重复的db名会被替换掉
func (m *DBFactory) AddTomlFile(file string) error {
    tree, err := toml.LoadFile(file)
    if err != nil {
        return zerrors.WrapSimple(err, "toml文件加载失败")
    }
    return m.AddTomlTree(tree)
}

// 添加toml树, 重复的db名会被替换掉
func (m *DBFactory) AddTomlTree(tree *toml.Tree) error {
    for _, key := range tree.Keys() {
        if !strings.HasPrefix(key, DBPrefix) {
            continue
        }
        switch shard := tree.Get(key).(type) {
        case *toml.Tree:
            dbname := key[len(DBPrefix):]
            if err := m.AddTomlShard(dbname, shard); err != nil {
                return err
            }
        }
    }
    return nil
}

// 添加toml分片, 重复的db名会被替换掉
func (m *DBFactory) AddTomlShard(dbname string, shard *toml.Tree) error {
    if dbname == "" {
        return zerrors.NewSimple("dbname为空")
    }

    dbname = strings.ToLower(dbname)

    switch dbtype := shard.Get(DBTypeField).(type) {
    case string:
        if dbtype == "" {
            return zerrors.NewSimplef("<%s>错误, %s为空", dbname, DBTypeField)
        }

        dbtype = strings.ToLower(dbtype)
        config := m.mustGetFactory(DBType(dbtype)).MakeEmptyConfig()
        if err := shard.Unmarshal(config); err != nil {
            return err
        }

        m.AddDBConfig(dbname, DBType(dbtype), config)
    default:
        return zerrors.NewSimplef("<%s>错误, %s必须存在且为string类型", dbname, DBTypeField)
    }
    return nil
}

// 添加db配置, 重复的db名会被替换掉
func (m *DBFactory) AddDBConfig(dbname string, dbtype DBType, config interface{}) {
    dbname = strings.ToLower(dbname)

    m.mx.Lock()

    // 关闭之前的连接
    if instance, ok := m.storage[dbname]; ok {
        _ = m.closeDB(instance)
        delete(m.storage, dbname)
    }

    // 设置新的配置
    m.confs[dbname] = &dbConfig{
        dbtype: dbtype,
        config: config,
    }

    m.mx.Unlock()
}

// 移除db, 移除之前会关闭连接
func (m *DBFactory) RemoveDB(dbname string) {
    dbname = strings.ToLower(dbname)

    m.mx.Lock()

    if instance, ok := m.storage[dbname]; ok {
        _ = m.closeDB(instance)
        delete(m.storage, dbname)
    }

    delete(m.confs, dbname)

    m.mx.Unlock()
}

// 连接所有db
func (m *DBFactory) ConnectAllDB() error {
    m.mx.Lock()
    for dbname, conf := range m.confs {
        if _, ok := m.storage[dbname]; ok {
            continue
        }

        instance, err := m.connectDB(conf)
        if err != nil {
            m.mx.Unlock()
            return fmt.Errorf("%s, %s", dbname, err)
        }

        m.storage[dbname] = &DBInstance{dbtype: conf.dbtype, instance: instance}
    }
    m.mx.Unlock()
    return nil
}

// 关闭所有db连接
func (m *DBFactory) CloseAllDb() {
    m.mx.Lock()
    for _, instance := range m.storage {
        _ = m.closeDB(instance)
    }
    m.storage = make(map[string]*DBInstance)
    m.mx.Unlock()
}

// 获取db实例, 不存在会返回nil
func (m *DBFactory) GetDBInstance(dbname string) *DBInstance {
    dbname = strings.ToLower(dbname)

    m.mx.RLock()
    out := m.storage[dbname]
    m.mx.RUnlock()
    return out
}

func (m *DBFactory) mustGetFactory(dbtype DBType) IDBFactory {
    if factory, ok := factoryStorage[dbtype]; ok {
        return factory
    }
    panic(zerrors.NewSimplef("不支持的db类型<%v>", dbtype))
}

func (m *DBFactory) connectDB(conf *dbConfig) (interface{}, error) {
    return m.mustGetFactory(conf.dbtype).Connect(conf.config)
}
func (m *DBFactory) closeDB(instance *DBInstance) error {
    return m.mustGetFactory(instance.dbtype).Close(instance.instance)
}

// 注册自定义factory
func RegistryDBFactory(dbtype DBType, factory IDBFactory) {
    factoryStorage[dbtype] = factory
}
