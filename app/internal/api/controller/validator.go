package controller

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/locales/en"
	"github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslations "github.com/go-playground/validator/v10/translations/en"
	zhTranslations "github.com/go-playground/validator/v10/translations/zh"
)

// 定义一个全局翻译器T
var trans ut.Translator

// InitTrans 初始化翻译器
func InitTrans(locale string) (err error) {
	// 修改gin框架中的Validator引擎属性，实现自定制
	v, ok := binding.Validator.Engine().(*validator.Validate)
	if !ok {
		// gin 的 Engine 恒为 *validator.Validate；走到这说明 gin 版本异常，
		// 静默吞掉会让上层以为翻译器就绪了
		return fmt.Errorf("binding.Validator.Engine() is not *validator.Validate")
	}

	// 注册一个获取json tag的自定义方法：错误提示里用 json 字段名而非 Go 字段名
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})

	zhT := zh.New() // 中文翻译器
	enT := en.New() // 英文翻译器

	// 第一个参数是备用（fallback）的语言环境
	// 后面的参数是应该支持的语言环境（支持多个）
	uni := ut.New(enT, zhT, enT)

	// locale 通常取决于 http 请求头的 'Accept-Language'
	trans, ok = uni.GetTranslator(locale)
	if !ok {
		return fmt.Errorf("uni.GetTranslator(%s) failed", locale)
	}

	// 注册翻译器
	switch locale {
	case "en":
		err = enTranslations.RegisterDefaultTranslations(v, trans)
	case "zh":
		err = zhTranslations.RegisterDefaultTranslations(v, trans)
	default:
		err = enTranslations.RegisterDefaultTranslations(v, trans)
	}
	return
}

// 去除提示信息中的结构体名称
func removeTopStruct(fields map[string]string) map[string]string {
	res := map[string]string{}
	for field, err := range fields {
		res[field[strings.Index(field, ".")+1:]] = err
	}
	return res
}

// 结构体级校验（RegisterStructValidation）只用于跨字段规则——
// tag 表达不了的组合约束（如确认密码 == 密码、开始时间 < 结束时间）才按需注册，
// 单字段规则全部用 binding tag 表达。目前没有跨字段规则，无注册项
