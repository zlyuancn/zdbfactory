/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/7
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "fmt"

    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/mysql"
    "github.com/zlyuancn/zerrors"
)

type mysqlFactory int

var _ IDBFactory = (*mysqlFactory)(nil)

type MysqlConfig struct {
    Host        string // 主机地址
    DBName      string // 库名
    UserName    string // 用户名
    Password    string // 密码
    MinPoolSize int    // 最小连接池数
    MaxPoolSize int    // 最大连接池个数
    Ping        bool   // 开始连接时是否ping确认连接情况
}

func (mysqlFactory) MakeEmptyConfig() interface{} {
    return new(MysqlConfig)
}

func (mysqlFactory) Connect(config interface{}) (interface{}, error) {
    var conf *MysqlConfig
    switch c := config.(type) {
    case *MysqlConfig:
        conf = c
    case MysqlConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*MysqlConfig结构")
    }

    dbsource := fmt.Sprintf("%s:%s@tcp(%s)/%s",
        conf.UserName,
        conf.Password,
        conf.Host,
        conf.DBName,
    )
    c, err := gorm.Open("mysql", dbsource)
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    db := c.DB()
    if conf.Ping {
        if err = db.Ping(); err != nil {
            return nil, zerrors.WrapSimple(err, "ping失败")
        }
    }

    db.SetMaxIdleConns(conf.MinPoolSize)
    db.SetMaxOpenConns(conf.MaxPoolSize)
    return c, nil
}
func (mysqlFactory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(*gorm.DB)
    if !ok {
        return zerrors.NewSimple("非*gorm.DB结构")
    }

    return c.Close()
}

// 添加mysql配置
func AddMysqlConfig(dbname string, conf *MysqlConfig) {
    AddDBConfig(dbname, Mysql, conf)
}

// 获取mysql实例
func GetMysql(dbname string) (*gorm.DB, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != Mysql {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(*gorm.DB), nil
}

// 获取mysql实例, 该实例如果不是mysql类型会panic
func MustGetMysql(dbname string) *gorm.DB {
    c, err := GetMysql(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
