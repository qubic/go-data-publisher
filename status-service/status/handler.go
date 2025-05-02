package status

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
	sp ProcessedTickProvider
}

type ProcessedTickProvider interface {
	GetLastProcessedTick() uint32
}

type Response struct {
	LatestProcessedTick uint32 `json:"latestProcessedTick"`
}

func NewHandler(sp ProcessedTickProvider) *Handler {
	return &Handler{sp: sp}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(Response{
		LatestProcessedTick: h.sp.GetLastProcessedTick(),
	})
	if err != nil {
		log.Printf("Error writing status response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
