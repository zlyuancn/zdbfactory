/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/7
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "context"
    "time"

    "github.com/zlyuancn/zerrors"
    "go.etcd.io/etcd/clientv3"
)

type etcdFactory int

var _ IDBFactory = (*etcdFactory)(nil)

type EtcdConfig struct {
    Address     []string
    UserName    string // 用户名
    Password    string // 密码
    DialTimeout int64  // 连接超时(毫秒
    Ping        bool   // 开始连接时是否ping确认连接情况
}

func (etcdFactory) MakeEmptyConfig() interface{} {
    return new(EtcdConfig)
}

func (etcdFactory) Connect(config interface{}) (interface{}, error) {
    var conf *EtcdConfig
    switch c := config.(type) {
    case *EtcdConfig:
        conf = c
    case EtcdConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*EtcdConfig结构")
    }

    c, err := clientv3.New(clientv3.Config{
        Endpoints:   conf.Address,
        Username:    conf.UserName,
        Password:    conf.Password,
        DialTimeout: time.Duration(conf.DialTimeout * 1e6),
    })
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    if conf.Ping {
        if _, err = c.Get(context.Background(), "/"); err != nil {
            return nil, zerrors.WrapSimple(err, "ping失败")
        }
    }

    return c, nil
}
func (etcdFactory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(*clientv3.Client)
    if !ok {
        return zerrors.NewSimple("非*clientv3.Client结构")
    }

    return c.Close()
}

// 添加etcd配置
func AddEtcdConfig(dbname string, conf *EtcdConfig) {
    AddDBConfig(dbname, ETCD, conf)
}

// 获取etcd实例
func GetEtcd(dbname string) (*clientv3.Client, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != ETCD {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(*clientv3.Client), nil
}

// 获取etcd实例, 该实例如果不是etcd类型会panic
func MustGetEtcd(dbname string) *clientv3.Client {
    c, err := GetEtcd(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
