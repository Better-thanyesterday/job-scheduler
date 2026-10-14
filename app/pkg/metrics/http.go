package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler /metrics 的标准出口。各进程（scheduler/executor）用独立 http.Server
// 挂载它，不要走业务路由的 auth/cors/ratelimit 中间件链——抓取是机器对机器的
// 高频访问，会被限流误伤，也会把凭据泄露进抓取端
func Handler() http.Handler {
	return promhttp.Handler()
}
