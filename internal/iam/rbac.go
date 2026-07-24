// rbac.go — 角色与权限表（首切片静态表，后续接策略引擎时替换 HasPermission 实现即可）。
package iam

// 角色取值（与 db/ddl/002_iam.sql 注释一致）。
const (
	RoleAdmin    = "admin"    // 全部权限
	RoleOperator = "operator" // 指令下发/取消/查询 + 资产查看
	RoleAuditor  = "auditor"  // 只读：结果查询 + 资产查看
)

// 权限标识
const (
	PermUserManage    = "user.manage"
	PermCommandSubmit = "command.submit"
	PermCommandCancel = "command.cancel"
	PermCommandResult = "command.result"
	PermAssetView     = "asset.view"
)

// rolePerms 角色 → 权限集合；admin 用 "*" 通配全部（含未来新增权限，避免漏配）。
var rolePerms = map[string]map[string]bool{
	RoleAdmin: {"*": true},
	RoleOperator: {
		PermCommandSubmit: true,
		PermCommandCancel: true,
		PermCommandResult: true,
		PermAssetView:     true,
	},
	RoleAuditor: {
		PermCommandResult: true,
		PermAssetView:     true,
	},
}

// ValidRole 角色是否合法。
func ValidRole(role string) bool {
	_, ok := rolePerms[role]
	return ok
}

// HasPermission 判定角色是否持有权限（admin 经 "*" 通配恒真）。
func HasPermission(role, perm string) bool {
	perms, ok := rolePerms[role]
	if !ok {
		return false
	}
	return perms["*"] || perms[perm]
}

// PermissionsOf 返回角色权限列表（whoami 展示用；admin 返回 ["*"]）。
func PermissionsOf(role string) []string {
	perms, ok := rolePerms[role]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(perms))
	for p := range perms {
		out = append(out, p)
	}
	return out
}
