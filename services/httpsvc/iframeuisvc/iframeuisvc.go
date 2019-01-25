package iframeuisvc

import (
	"net/http"

	"github.com/cernbox/reva/services/httpsvc"
	"github.com/mitchellh/mapstructure"
)

type config struct {
	Prefix string `mapstructure:"prefix"`
}

type svc struct {
	prefix  string
	handler http.Handler
}

// New returns a new webuisvc
func New(m map[string]interface{}) (httpsvc.Service, error) {
	conf := &config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}

	return &svc{prefix: conf.Prefix, handler: getHandler()}, nil
}

func (s *svc) Prefix() string {
	return s.prefix
}

func (s *svc) Handler() http.Handler {
	return s.handler
}

func getHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var head string
		head, r.URL.Path = httpsvc.ShiftPath(r.URL.Path)
		if head == "open" {
			doOpen(w, r)
			return
		}
	})
}

func doOpen(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<body>

<h1>Markdown Editor</h1>

</body>
<script type="text/javascript">
alert("hello!");
</script>
</html>
	`
	w.Write([]byte(html))
}