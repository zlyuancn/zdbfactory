/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/4/23
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "github.com/Shopify/sarama"
    "github.com/zlyuancn/zerrors"
)

type kafkaProducerFactory int

var _ IDBFactory = (*kafkaProducerFactory)(nil)

type KafkaProducerConfig struct {
    Address []string
    Async   bool // 是否异步
}

func (m *kafkaProducerFactory) MakeEmptyConfig() interface{} {
    return new(KafkaProducerConfig)
}
func (m *kafkaProducerFactory) Connect(config interface{}) (c interface{}, err error) {
    var conf *KafkaProducerConfig
    switch c := config.(type) {
    case *KafkaProducerConfig:
        conf = c
    case KafkaProducerConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*KafkaProducerConfig结构")
    }

    kconf := sarama.NewConfig()
    kconf.Producer.Return.Successes = true // producer把消息发给kafka之后不会等待结果返回
    kconf.Producer.Return.Errors = true    // 如果启用了该选项，未交付的消息将在Errors通道上返回，包括error(默认启用)。

    if conf.Async {
        producer, err := sarama.NewAsyncProducer(conf.Address, kconf)
        if err != nil {
            return nil, zerrors.WrapSimple(err, "连接失败")
        }
        return producer, nil
    }

    producer, err := sarama.NewSyncProducer(conf.Address, kconf)
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    return producer, nil
}
func (m *kafkaProducerFactory) Close(dbinstance interface{}) error {
    if c, ok := dbinstance.(sarama.SyncProducer); ok {
        return c.Close()
    }
    if c, ok := dbinstance.(sarama.AsyncProducer); ok {
        return c.Close()
    }
    return zerrors.NewSimple("非sarama.SyncProducer或sarama.AsyncProducer结构")
}

// 添加kafka生产者配置
func AddKafkaProducerConfig(dbname string, conf *KafkaProducerConfig) {
    AddDBConfig(dbname, KafkaProducer, conf)
}

// 获取kafka生产者实例
func GetKafkaProducer(dbname string) (sarama.SyncProducer, error) {
    a := GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != KafkaProducer {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.Type())
    }
    if c, ok := a.Instance().(sarama.SyncProducer); ok {
        return c, nil
    }

    return nil, zerrors.NewSimplef("非sarama.SyncProducer结构: %T", a.Instance())
}

func MustKafkaProducer(dbname string) sarama.SyncProducer {
    c, err := GetKafkaProducer(dbname)
    if err != nil {
        panic(err)
    }
    return c
}

// 获取kafka异步生产者实例
func GetKafkaAsyncProducer(dbname string) (sarama.AsyncProducer, error) {
    a := GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != KafkaProducer {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.Type())
    }
    if c, ok := a.Instance().(sarama.AsyncProducer); ok {
        return c, nil
    }

    return nil, zerrors.NewSimplef("非sarama.AsyncProducer结构: %T", a.Instance())
}

func MustKafkaAsyncProducer(dbname string) sarama.AsyncProducer {
    c, err := GetKafkaAsyncProducer(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
