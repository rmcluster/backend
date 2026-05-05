package webdavservice

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/webdav"
)

func NewWebDavService(fs webdav.FileSystem) *WebDavService {
	return &WebDavService{
		fs: fs,
	}
}

type WebDavService struct {
	fs webdav.FileSystem
}

func (s *WebDavService) RegisterGinHandlers(router *gin.Engine) {
	h := &webdav.Handler{
		Prefix:     "/dav",
		FileSystem: s.fs,
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("webdav: %s %s: %v", r.Method, r.URL.Path, err)
			} else {
				log.Printf("webdav: %s %s", r.Method, r.URL.Path)
			}
		},
	}

	router.Any("/dav/*filepath", gin.WrapH(h))

	// add routing for WebDAV specific methods
	methods := []string{"COPY", "LOCK", "MKCOL", "MOVE", "PROPFIND", "PROPPATCH", "UNLOCK"}
	for _, method := range methods {
		router.Handle(method, "/dav/*filepath", gin.WrapH(h))
	}
}
