/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/7
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "time"

    "github.com/go-redis/redis"
    "github.com/zlyuancn/zerrors"
)

type redisFactory int

var _ IDBFactory = (*redisFactory)(nil)

type RedisConfig struct {
    Address      []string // [host1:port1, host2:port2]
    Password     string
    DB           int
    IsCluster    bool
    PoolSize     int
    ReadTimeout  int64 // 超时(毫秒
    WriteTimeout int64 // 超时(毫秒
    DialTimeout  int64 // 超时(毫秒
    Ping         bool  // 开始连接时是否ping确认连接情况
}

func (redisFactory) MakeEmptyConfig() interface{} {
    return new(RedisConfig)
}

func (redisFactory) Connect(config interface{}) (interface{}, error) {
    var conf *RedisConfig
    switch c := config.(type) {
    case *RedisConfig:
        conf = c
    case RedisConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*RedisConfig结构")
    }

    var c redis.UniversalClient
    if conf.IsCluster {
        c = redis.NewClusterClient(&redis.ClusterOptions{
            Addrs:        conf.Address,
            Password:     conf.Password,
            PoolSize:     conf.PoolSize,
            ReadTimeout:  time.Duration(conf.ReadTimeout * 1e6),
            WriteTimeout: time.Duration(conf.WriteTimeout * 1e6),
            DialTimeout:  time.Duration(conf.DialTimeout * 1e6),
        })
    } else {
        if len(conf.Address) < 0 {
            return nil, zerrors.NewSimple("请检查redis配置的address")
        }
        c = redis.NewClient(&redis.Options{
            Addr:         conf.Address[0],
            Password:     conf.Password,
            DB:           conf.DB,
            PoolSize:     conf.PoolSize,
            ReadTimeout:  time.Duration(conf.ReadTimeout * 1e6),
            WriteTimeout: time.Duration(conf.WriteTimeout * 1e6),
            DialTimeout:  time.Duration(conf.DialTimeout * 1e6),
        })
    }

    if conf.Ping {
        if _, err := c.Ping().Result(); err != nil {
            return nil, zerrors.WrapSimple(err, "ping失败")
        }
    }
    return c, nil
}
func (redisFactory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(redis.UniversalClient)
    if !ok {
        return zerrors.NewSimple("非redis.UniversalClient结构")
    }

    return c.Close()
}

// 添加redis配置
func AddRedisConfig(dbname string, conf *RedisConfig) {
    AddDBConfig(dbname, Redis, conf)
}

// 获取redisdb实例
func GetRedis(dbname string) (redis.UniversalClient, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != Redis {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(redis.UniversalClient), nil
}

// 获取redisdb实例, 该实例如果不是redis类型会panic
func MustGetRedis(dbname string) redis.UniversalClient {
    c, err := GetRedis(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
