package openapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	generated "github.com/wk-y/rama-swap/cmd/linux-client/openapi/generated/go"
	"github.com/wk-y/rama-swap/server/gcas"
)

func NewRouter(cas gcas.CAS) *gin.Engine {
	return generated.NewRouter(generated.ApiHandleFunctions{
		DefaultAPI: OpenAPIRoutes{
			Cas: cas,
		},
	})
}

type OpenAPIRoutes struct {
	Cas gcas.CAS
}

// ChunkChunkIdDelete implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) ChunkChunkIdDelete(c *gin.Context) {
	chunkId, chunkErr := extractHashFromContext(c)
	if chunkErr != nil {
		chunkErr.Respond(c)
		return
	}

	err := o.Cas.Delete(c.Request.Context(), chunkId)
	if err != nil {
		if errors.Is(err, gcas.HashNotFoundError{}) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "not_found",
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		}
		return
	}

	c.Status(http.StatusOK)
}

// ChunkChunkIdGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) ChunkChunkIdGet(c *gin.Context) {
	hash, chunkErr := extractHashFromContext(c)
	if chunkErr != nil {
		chunkErr.Respond(c)
		return
	}

	data, err := o.Cas.Get(c.Request.Context(), hash)
	if err != nil {
		if errors.Is(err, gcas.HashNotFoundError{}) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "not_found",
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		}
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data)
}

// ChunkChunkIdPut implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) ChunkChunkIdPut(c *gin.Context) {
	hash, chunkErr := extractHashFromContext(c)
	if chunkErr != nil {
		chunkErr.Respond(c)
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "bad_request",
		})
		return
	}

	// validate data hash
	hash_got := sha256.Sum256(data)
	if hash_got != hash {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "checksum_incorrect",
		})
		return
	}

	err = o.Cas.Put(c.Request.Context(), hash, data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.Status(http.StatusOK)
}

// ChunksHealthcheckGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) ChunksHealthcheckGet(c *gin.Context) {
	chunks, err := o.Cas.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	badChunks := make([]string, 0)
	for chunk := range chunks {
		_, err := o.Cas.Get(c.Request.Context(), chunk)
		if err != nil {
			if !errors.Is(err, gcas.HashNotFoundError{}) {
				badChunks = append(badChunks, hex.EncodeToString(chunk[:]))
			}
		}
	}
	status := "healthy"
	if len(badChunks) > 0 {
		status = "degraded"
	}

	c.JSON(http.StatusOK, generated.ChunksHealthcheckGet200Response{
		Status:    status,
		BadChunks: badChunks,
	})
}

// ChunksListGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) ChunksListGet(c *gin.Context) {
	ch, err := o.Cas.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	hashes := make([]string, 0)
	for hash := range ch {
		hashes = append(hashes, hex.EncodeToString(hash[:]))
	}

	c.JSON(http.StatusOK, hashes)
}

// StorageInfoGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) StorageInfoGet(c *gin.Context) {
	free, err := o.Cas.FreeSpace(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_space":     0,
		"used_space":      0,
		"available_space": free,
	})
}

var _ generated.DefaultAPI = OpenAPIRoutes{}

func extractHashFromContext(c *gin.Context) (gcas.Hash, *errWithResponse) {
	chunkId := c.Param("chunk_id")

	if chunkId == "" {
		return gcas.Hash{}, &errWithResponse{errors.New("chunk_id is required"), http.StatusBadRequest}
	}

	if len(chunkId) != 64 {
		return gcas.Hash{}, &errWithResponse{errors.New("chunk_id is not a valid hash"), http.StatusBadRequest}
	}

	hash := [32]byte{}
	_, err := hex.Decode(hash[:], []byte(chunkId))
	if err != nil {
		return gcas.Hash{}, &errWithResponse{errors.New("chunk_id is not a valid hash"), http.StatusBadRequest}
	}

	return gcas.Hash(hash), nil
}

type errWithResponse struct {
	err        error
	statusCode int
}

func (e *errWithResponse) Error() string {
	return e.err.Error()
}

func (e *errWithResponse) Respond(c *gin.Context) {
	c.JSON(e.statusCode, gin.H{
		"error": e.err.Error(),
	})
}

var _ error = &errWithResponse{}
