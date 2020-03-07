/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/4
   Description :
-------------------------------------------------
*/

package zdbfactory

type Options func(factory *DBFactory)

// 收到进程退出信号自动关闭所有db
func WithAutoCloseDB() Options {
    return func(factory *DBFactory) {
        factory.autoClose = true
    }
}
