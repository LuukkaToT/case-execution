package usecase

import "context"

// TxRunner 让 usecase 在不依赖 gorm.DB 的前提下定义 ACID 边界。基础设施
// 适配器把事务句柄放入子 ctx，使 fn 内调用的多个仓储自动加入同一事务。
type TxRunner interface {
	// Do 在事务内执行 fn；返回非 nil error 时回滚，返回 nil 时提交。子 ctx
	// 携带事务句柄，并跟随父 ctx 取消。
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}
